package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// scriptedChunkServer answers the i-th request with replies[i] (the last one
// repeats) and records every request body.
type scriptedReply struct {
	status int
	body   string
}

func scriptedChunkServer(t *testing.T, replies ...scriptedReply) (*httptest.Server, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		mu.Lock()
		i := len(got)
		got = append(got, m)
		mu.Unlock()
		if r.Method != http.MethodPost {
			http.NotFound(w, r) // progress reads (GET /status/durable) are not part of the script
			return
		}
		rep := replies[len(replies)-1]
		if i < len(replies) {
			rep = replies[i]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rep.status)
		_, _ = io.WriteString(w, rep.body)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		var posts []map[string]any
		for _, m := range got {
			if m != nil {
				posts = append(posts, m)
			}
		}
		return posts
	}
}

// shortBound makes the per-request bound (the server's OpDeadline, 5 min in production) small for a test.
func shortBound(t *testing.T, d time.Duration) {
	t.Helper()
	old := chunkRequestBound
	chunkRequestBound = d
	t.Cleanup(func() { chunkRequestBound = old })
}

const (
	timedOut  = `{"code":"remote_unavailable","message":"storage: capture upload: storage: chunk remote unavailable","retryable":true}`
	refused   = `{"code":"snapshot_exists","message":"snapshot s1 exists with another request","retryable":false}`
	published = `{"volumeID":"v1","name":"s1","headGen":4,"headEpoch":1,"cutLSN":65536}`
)

// B2 (CUT-BUDGET-r1, r2.439): a snapshot whose work outlasts one request finishes across requests, every request
// carrying the same name and request id, so a commit that landed during a timed-out request is found, not repeated.
func TestChunkSnapshotRepeatsTimedOutRequestsWithTheSameNameAndRequestID(t *testing.T) {
	shortBound(t, 200*time.Millisecond)
	srv, posts := scriptedChunkServer(t, scriptedReply{503, timedOut}, scriptedReply{503, timedOut}, scriptedReply{200, published})
	code, out, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "5s")
	if code != 0 {
		t.Fatalf("exit %d, want 0 (the third request publishes): stdout %q stderr %q", code, out, errs)
	}
	if out != published {
		t.Fatalf("stdout %q: want only the final answer %q", out, published)
	}
	p := posts()
	if len(p) != 3 {
		t.Fatalf("%d requests, want 3: %+v", len(p), p)
	}
	for i, b := range p {
		if b["name"] != "s1" || b["requestID"] != "rid-1" {
			t.Fatalf("request %d: name %v request id %v, want s1 / rid-1 in every request", i+1, b["name"], b["requestID"])
		}
		if ms, _ := b["timeoutMs"].(float64); ms <= 0 || ms > 200 {
			t.Fatalf("request %d: timeoutMs %v, want 0 < t <= the per-request bound (200)", i+1, b["timeoutMs"])
		}
	}
	if !strings.Contains(errs, "request 1 ended: remote_unavailable") || !strings.Contains(errs, "request 2 ended: remote_unavailable") {
		t.Fatalf("stderr %q: want a progress line per timed-out request", errs)
	}
}

// A generated request id is printed once and reused by every request of the loop.
func TestChunkSnapshotLoopReusesTheGeneratedRequestID(t *testing.T) {
	shortBound(t, 200*time.Millisecond)
	srv, posts := scriptedChunkServer(t, scriptedReply{503, timedOut}, scriptedReply{200, published})
	code, _, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--timeout", "5s")
	if code != 0 {
		t.Fatalf("exit %d: %q", code, errs)
	}
	p := posts()
	if len(p) != 2 || p[0]["requestID"] == "" || p[0]["requestID"] != p[1]["requestID"] {
		t.Fatalf("requests %+v: want 2 with one generated request id", p)
	}
	if n := strings.Count(errs, "request id "); n != 1 {
		t.Fatalf("stderr %q: the generated request id is printed %d times, want once", errs, n)
	}
}

// The loop ends on the first non-retryable refusal and relays it.
func TestChunkLoopStopsOnANonRetryableRefusal(t *testing.T) {
	shortBound(t, 200*time.Millisecond)
	srv, posts := scriptedChunkServer(t, scriptedReply{409, refused}, scriptedReply{200, published})
	code, out, _ := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "5s")
	if code == 0 || len(posts()) != 1 || out != refused {
		t.Fatalf("exit %d, %d requests, stdout %q: want one request and its refusal relayed", code, len(posts()), out)
	}
}

// The loop ends at --timeout: no request starts after it, and the last answer is relayed.
func TestChunkLoopEndsAtTheTimeout(t *testing.T) {
	shortBound(t, 100*time.Millisecond)
	srv, posts := scriptedChunkServer(t, scriptedReply{503, timedOut})
	start := time.Now()
	code, out, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "500ms")
	took := time.Since(start)
	if code == 0 || out != timedOut {
		t.Fatalf("exit %d stdout %q: want the last timed-out answer relayed", code, out)
	}
	if took > 2*time.Second {
		t.Fatalf("took %v for --timeout 500ms", took)
	}
	if n := len(posts()); n < 2 || n > 20 {
		t.Fatalf("%d requests for 500 ms of 100 ms requests", n)
	}
	if !strings.Contains(errs, "--timeout 500ms passed") {
		t.Fatalf("stderr %q: want the end of the loop named", errs)
	}
}

// A --timeout within the per-request bound sends one request, as before B2.
func TestChunkTimeoutWithinTheBoundSendsOneRequest(t *testing.T) {
	shortBound(t, 10*time.Second)
	srv, posts := scriptedChunkServer(t, scriptedReply{503, timedOut}, scriptedReply{200, published})
	code, out, _ := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "2s")
	if code == 0 || len(posts()) != 1 || out != timedOut {
		t.Fatalf("exit %d, %d requests, stdout %q: want exactly one request, as before", code, len(posts()), out)
	}
	if ms := posts()[0]["timeoutMs"]; ms != float64(2000) {
		t.Fatalf("timeoutMs %v, want 2000", ms)
	}
}

// cut and the plain detach loop the same way.
func TestChunkCutAndPlainDetachRepeatTimedOutRequests(t *testing.T) {
	shortBound(t, 200*time.Millisecond)
	srv, posts := scriptedChunkServer(t, scriptedReply{503, timedOut}, scriptedReply{200, `{"volumeID":"v1","cutLSN":9}`})
	if code, out, errs := runChunk("cut", "--volume", "v1", "--status-addr", srv.URL, "--timeout", "5s"); code != 0 || out != `{"volumeID":"v1","cutLSN":9}` || len(posts()) != 2 {
		t.Fatalf("cut: exit %d stdout %q stderr %q, %d requests", code, out, errs, len(posts()))
	}
	seal := `{"volumeID":"v1","cutLSN":9,"headGen":5,"headEpoch":1,"lastWrittenLSN":9}`
	srv2, posts2 := scriptedChunkServer(t, scriptedReply{503, timedOut}, scriptedReply{200, seal})
	code, out, errs := runChunk("detach", "--plain", "--volume", "v1", "--status-addr", srv2.URL, "--timeout", "5s")
	if code != 0 || out != seal || len(posts2()) != 2 {
		t.Fatalf("plain detach: exit %d stdout %q stderr %q, %d requests", code, out, errs, len(posts2()))
	}
	if n := strings.Count(errs, "sealed HEAD generation: 5"); n != 1 {
		t.Fatalf("stderr %q: the sealed generation is printed %d times, want once", errs, n)
	}
}
