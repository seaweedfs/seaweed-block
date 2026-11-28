package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type scriptedReply struct {
	status int
	body   string
}

type postRecord struct {
	at   time.Time
	body map[string]any
}

// scriptedChunkServer answers the i-th POST with replies[i] (the last one repeats) after holding it for hold, records
// every POST and when it arrived, and answers GET /status/durable with status(n) for the n-th read (404 when nil).
type scripted struct {
	replies []scriptedReply
	hold    time.Duration
	status  func(n int, volume string) (int, string, time.Duration)

	mu    sync.Mutex
	posts []postRecord
	gets  int
}

func (s *scripted) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			s.mu.Lock()
			n := s.gets
			s.gets++
			s.mu.Unlock()
			if s.status == nil || r.URL.Path != "/status/durable" {
				http.NotFound(w, r)
				return
			}
			code, body, delay := s.status(n, r.URL.Query().Get("volume"))
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
			w.WriteHeader(code)
			_, _ = io.WriteString(w, body)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		s.mu.Lock()
		i := len(s.posts)
		s.posts = append(s.posts, postRecord{at: time.Now(), body: m})
		s.mu.Unlock()
		rep := s.replies[len(s.replies)-1]
		if i < len(s.replies) {
			rep = s.replies[i]
		}
		select {
		case <-time.After(s.hold):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rep.status)
		_, _ = io.WriteString(w, rep.body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (s *scripted) recorded() []postRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]postRecord(nil), s.posts...)
}

// shortLoop makes the per-request bound (the server's OpDeadline, 5 min in production) and the pause small for a test.
func shortLoop(t *testing.T, bound, pauseStart, pauseMax time.Duration) {
	t.Helper()
	ob, os, om := chunkRequestBound, chunkPauseStart, chunkPauseMax
	chunkRequestBound, chunkPauseStart, chunkPauseMax = bound, pauseStart, pauseMax
	t.Cleanup(func() { chunkRequestBound, chunkPauseStart, chunkPauseMax = ob, os, om })
}

const (
	timedOut  = `{"code":"remote_unavailable","message":"storage: capture upload: storage: chunk remote unavailable","retryable":true}`
	refused   = `{"code":"snapshot_limit","message":"volume v1 holds the maximum number of snapshots","retryable":false}`
	published = `{"volumeID":"v1","name":"s1","headGen":4,"headEpoch":1,"cutLSN":65536}`
	// the holder's answers around a detach that ran out (core/host/volume chunkErrorCode)
	detachUnknown = `{"code":"outcome_unknown","message":"durable: detach outcome unknown; I/O stays stopped: context deadline exceeded","retryable":true}`
	detachAborted = `{"code":"detach_aborted","message":"durable: detach aborted; the volume keeps serving on this host","retryable":true}`
	detachFenced  = `{"code":"fenced","message":"HEAD belongs to a newer epoch or another holder","retryable":false}`
	sealed        = `{"volumeID":"v1","cutLSN":9,"headGen":5,"headEpoch":1,"lastWrittenLSN":9}`
	cutUnknown    = `{"code":"outcome_unknown","message":"storage: chunk HEAD CAS outcome unknown","retryable":true}`
)

// B2 (V1-SPEC r2.439): a snapshot whose work outlasts one request finishes across requests, every request carrying
// the same body. The server answers a name it already holds from HEAD (TestLayoutProviderSnapshotVolume in
// core/frontend/durable: a repeat of "daily" keeps the first CreatedNs), so a snapshot committed during a timed-out
// request is found by the next one, not taken again.
func TestChunkSnapshotRepeatsTimedOutRequestsWithTheSameBody(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, timedOut}, {503, timedOut}, {200, published}}}
	srv := s.start(t)
	code, out, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "5s")
	if code != 0 || out != published {
		t.Fatalf("exit %d stdout %q (want 0 and only the final answer) stderr %q", code, out, errs)
	}
	p := s.recorded()
	if len(p) != 3 {
		t.Fatalf("%d requests, want 3", len(p))
	}
	for i, r := range p {
		if r.body["name"] != "s1" || r.body["requestID"] != "rid-1" {
			t.Fatalf("request %d: name %v request id %v, want s1 / rid-1 in every request", i+1, r.body["name"], r.body["requestID"])
		}
		if ms, _ := r.body["timeoutMs"].(float64); ms <= 0 || ms > 200 {
			t.Fatalf("request %d: timeoutMs %v, want 0 < t <= the per-request bound (200)", i+1, r.body["timeoutMs"])
		}
	}
	for _, want := range []string{"request 1 ended: remote_unavailable (storage: capture upload", "request 2 ended: remote_unavailable"} {
		if !strings.Contains(errs, want) {
			t.Fatalf("stderr %q: want %q", errs, want)
		}
	}
	if strings.Contains(errs, "stopped guest I/O") {
		t.Fatalf("stderr %q: a snapshot does not stop guest I/O", errs)
	}
}

// A generated request id is printed once, and every request of the loop carries exactly that id.
func TestChunkSnapshotLoopReusesTheGeneratedRequestID(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, timedOut}, {200, published}}}
	srv := s.start(t)
	code, _, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--timeout", "5s")
	if code != 0 {
		t.Fatalf("exit %d: %q", code, errs)
	}
	m := regexp.MustCompile(`request id ([0-9a-f]{32}) `).FindAllStringSubmatch(errs, -1)
	if len(m) != 1 {
		t.Fatalf("stderr %q: the generated request id is printed %d times, want once", errs, len(m))
	}
	p := s.recorded()
	if len(p) != 2 {
		t.Fatalf("%d requests, want 2", len(p))
	}
	for i, r := range p {
		if id, ok := r.body["requestID"].(string); !ok || id != m[0][1] {
			t.Fatalf("request %d: requestID %#v, want the printed %s", i+1, r.body["requestID"], m[0][1])
		}
	}
}

// The loop ends on the first non-retryable refusal and relays it.
func TestChunkLoopStopsOnANonRetryableRefusal(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{409, refused}, {200, published}}}
	srv := s.start(t)
	code, out, _ := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "5s")
	if code == 0 || len(s.recorded()) != 1 || out != refused {
		t.Fatalf("exit %d, %d requests, stdout %q: want one request and its refusal relayed", code, len(s.recorded()), out)
	}
}

// A --timeout within the per-request bound sends one request, as before B2.
func TestChunkTimeoutWithinTheBoundSendsOneRequest(t *testing.T) {
	shortLoop(t, 10*time.Second, 20*time.Millisecond, 20*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, timedOut}, {200, published}}}
	srv := s.start(t)
	code, out, _ := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "2s")
	if code == 0 || len(s.recorded()) != 1 || out != timedOut {
		t.Fatalf("exit %d, %d requests, stdout %q: want exactly one request, as before", code, len(s.recorded()), out)
	}
	if ms := s.recorded()[0].body["timeoutMs"]; ms != float64(2000) {
		t.Fatalf("timeoutMs %v, want 2000", ms)
	}
}

// runChunkWithin runs the command and fails the test if it has not returned within limit: a loop that misses its
// own --timeout fails here at once, not at the test binary's timeout.
func runChunkWithin(t *testing.T, limit time.Duration, args ...string) (int, string, string) {
	t.Helper()
	type res struct {
		code     int
		out, err string
	}
	done := make(chan res, 1)
	go func() {
		c, o, e := runChunk(args...)
		done <- res{c, o, e}
	}()
	select {
	case r := <-done:
		return r.code, r.out, r.err
	case <-time.After(limit):
		t.Fatalf("the command did not return within %v: the loop does not end at its --timeout", limit)
		return 0, "", ""
	}
}

// checkInsideT: no request starts after T, none carries a non-positive bound, and the command returns by T + slack.
func checkInsideT(t *testing.T, s *scripted, start time.Time, timeout, took time.Duration) {
	t.Helper()
	if took > timeout+300*time.Millisecond {
		t.Fatalf("the command took %v for --timeout %v", took, timeout)
	}
	for i, r := range s.recorded() {
		if at := r.at.Sub(start); at > timeout {
			t.Fatalf("request %d started %v after the start, past --timeout %v", i+1, at, timeout)
		}
		if ms, _ := r.body["timeoutMs"].(float64); ms <= 0 {
			t.Fatalf("request %d: timeoutMs %v, want a positive bound", i+1, r.body["timeoutMs"])
		}
	}
}

// The loop ends at --timeout, relays the last answer, and does not ask a store that refuses at once in a busy loop.
func TestChunkLoopEndsAtTheTimeout(t *testing.T) {
	shortLoop(t, 100*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, timedOut}}}
	srv := s.start(t)
	start := time.Now()
	code, out, errs := runChunkWithin(t, 5*time.Second, "snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "500ms")
	took := time.Since(start)
	if code == 0 || out != timedOut || !strings.Contains(errs, "--timeout 500ms passed") {
		t.Fatalf("exit %d stdout %q stderr %q: want the last answer relayed and the end named", code, out, errs)
	}
	checkInsideT(t, s, start, 500*time.Millisecond, took)
	if n := len(s.recorded()); n < 2 || n > 11 {
		t.Fatalf("%d requests in 500 ms with a 50 ms pause: want 2..11 (paused, not a busy loop)", n)
	}
}

// A progress read the holder does not answer cannot push a request past --timeout (it is cut at the deadline).
func TestChunkLoopHeldProgressReadSendsNoRequestAfterTheTimeout(t *testing.T) {
	shortLoop(t, 100*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, timedOut}},
		status: func(int, string) (int, string, time.Duration) { return 200, `{}`, 3 * time.Second }}
	srv := s.start(t)
	start := time.Now()
	code, out, _ := runChunkWithin(t, 5*time.Second, "snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "500ms")
	took := time.Since(start)
	if code == 0 || out != timedOut {
		t.Fatalf("exit %d stdout %q: want the last answer relayed", code, out)
	}
	checkInsideT(t, s, start, 500*time.Millisecond, took)
}

// A holder that answers late cannot hold the command past --timeout: the client's wait for each answer (the bound
// plus a cushion) ends inside it.
func TestChunkLoopSlowAnswersAreBoundedByTheTimeout(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, timedOut}}, hold: 2 * time.Second}
	srv := s.start(t)
	start := time.Now()
	code, _, errs := runChunkWithin(t, 5*time.Second, "snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "600ms")
	took := time.Since(start)
	if code == 0 || !strings.Contains(errs, "--timeout 600ms passed") {
		t.Fatalf("exit %d stderr %q: want the loop to end at --timeout", code, errs)
	}
	checkInsideT(t, s, start, 600*time.Millisecond, took)
}

// The pause after an early end doubles up to its cap (a constant pause does not pass).
func TestChunkLoopPauseDoublesToItsCap(t *testing.T) {
	shortLoop(t, 2*time.Second, 40*time.Millisecond, 160*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, timedOut}, {503, timedOut}, {503, timedOut}, {503, timedOut}, {503, timedOut}, {200, published}}}
	srv := s.start(t)
	if code, _, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "10s"); code != 0 {
		t.Fatalf("exit %d: %q", code, errs)
	}
	p := s.recorded()
	if len(p) != 6 {
		t.Fatalf("%d requests, want 6", len(p))
	}
	var gap []time.Duration
	for i := 1; i < len(p); i++ {
		gap = append(gap, p[i].at.Sub(p[i-1].at))
	}
	// want about 40, 80, 160, 160, 160 ms
	if !(gap[1] > gap[0]*3/2 && gap[2] > gap[1]*3/2) {
		t.Fatalf("gaps %v: want the pause to double (40, 80, 160 ms)", gap)
	}
	if gap[3] > 240*time.Millisecond || gap[4] > 240*time.Millisecond || gap[3] < 120*time.Millisecond {
		t.Fatalf("gaps %v: want the pause held at its 160 ms cap", gap)
	}
}

// Between requests the holder's staged chunks are reported as the change since the first request, read with the
// volume's id; a holder that does not answer the read (the other tests: 404) leaves the progress part out.
func TestChunkLoopReportsTheHolderProgressSinceTheFirstRequest(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	staged := []uint64{1000, 1050, 1400}
	var asked []string
	var mu sync.Mutex
	s := &scripted{replies: []scriptedReply{{503, timedOut}, {503, timedOut}, {200, published}},
		status: func(n int, volume string) (int, string, time.Duration) {
			mu.Lock()
			asked = append(asked, volume)
			mu.Unlock()
			v := staged[min(n, len(staged)-1)]
			return 200, fmt.Sprintf(`{"VolumeID":"v1","Volumes":[{"Chunk":{"Stats":{"StagedChunks":%d,"DirtyChunks":7}}}]}`, v), 0
		}}
	srv := s.start(t)
	code, _, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "5s")
	if code != 0 {
		t.Fatalf("exit %d: %q", code, errs)
	}
	for _, want := range []string{"50 chunks staged since the first request, 7 dirty", "400 chunks staged since the first request, 7 dirty"} {
		if !strings.Contains(errs, want) {
			t.Fatalf("stderr %q: want %q", errs, want)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, v := range asked {
		if v != "v1" {
			t.Fatalf("progress read for volume %q, want v1", v)
		}
	}
}

// cut and the plain detach loop the same way; the sealed generation is printed once.
func TestChunkCutAndPlainDetachRepeatTimedOutRequests(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, cutUnknown}, {200, `{"volumeID":"v1","cutLSN":9}`}}}
	srv := s.start(t)
	code, out, errs := runChunk("cut", "--volume", "v1", "--status-addr", srv.URL, "--timeout", "5s")
	if code != 0 || out != `{"volumeID":"v1","cutLSN":9}` || len(s.recorded()) != 2 {
		t.Fatalf("cut: exit %d stdout %q stderr %q, %d requests", code, out, errs, len(s.recorded()))
	}
	if strings.Contains(errs, "stopped guest I/O") {
		t.Fatalf("cut stderr %q: an outcome_unknown cut does not stop guest I/O", errs)
	}
	s2 := &scripted{replies: []scriptedReply{{503, timedOut}, {200, sealed}}}
	srv2 := s2.start(t)
	code, out, errs = runChunk("detach", "--plain", "--volume", "v1", "--status-addr", srv2.URL, "--timeout", "5s")
	if code != 0 || out != sealed || len(s2.recorded()) != 2 {
		t.Fatalf("plain detach: exit %d stdout %q stderr %q, %d requests", code, out, errs, len(s2.recorded()))
	}
	if n := strings.Count(errs, "sealed HEAD generation: 5"); n != 1 {
		t.Fatalf("stderr %q: the sealed generation is printed %d times, want once", errs, n)
	}
}

// A detach that ends outcome_unknown has stopped guest I/O until the holder settles it in the background. The holder
// answers outcome_unknown again while it settles (detachVolume: h.settling), then from the settled HEAD: sealed (an
// applied detach repeats as its answer, TestChunkDetach_RepeatedDetachSettlesAppliedDetach), aborted (serving resumed;
// the next request detaches anew), or fenced (non-retryable). The loop says the volume is stopped, ends on the settled
// answer, and an already detached volume is a success.
func TestChunkDetachOutcomeUnknownSettles(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	for _, tc := range []struct {
		name    string
		replies []scriptedReply
		code    int
		out     string
	}{
		{"applied", []scriptedReply{{503, detachUnknown}, {503, detachUnknown}, {200, sealed}}, 0, sealed},
		{"not published, then detached anew", []scriptedReply{{503, detachUnknown}, {503, detachUnknown}, {409, detachAborted}, {200, sealed}}, 0, sealed},
		{"fenced", []scriptedReply{{503, detachUnknown}, {409, detachFenced}}, chunkExitFailed, detachFenced},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, plain := range []bool{false, true} {
				s := &scripted{replies: tc.replies}
				srv := s.start(t)
				args := []string{"detach", "--volume", "v1", "--status-addr", srv.URL, "--timeout", "5s"}
				if plain {
					args = append(args, "--plain")
				}
				code, out, errs := runChunk(args...)
				if code != tc.code || out != tc.out || len(s.recorded()) != len(tc.replies) {
					t.Fatalf("plain=%v: exit %d stdout %q (want %d %q), %d requests (want %d); stderr %q", plain, code, out, tc.code, tc.out, len(s.recorded()), len(tc.replies), errs)
				}
				if !strings.Contains(errs, "request 1 ended: outcome_unknown") || !strings.Contains(errs, "the volume has stopped guest I/O until this detach settles") {
					t.Fatalf("plain=%v stderr %q: want the stopped-I/O line after the unknown answer", plain, errs)
				}
			}
		})
	}
}

// A detach still unknown at --timeout says the volume stays stopped and how to read the settled answer.
func TestChunkDetachStillUnknownAtTheTimeoutSaysTheVolumeIsStopped(t *testing.T) {
	shortLoop(t, 100*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond)
	s := &scripted{replies: []scriptedReply{{503, detachUnknown}}}
	srv := s.start(t)
	code, out, errs := runChunkWithin(t, 5*time.Second, "detach", "--volume", "v1", "--status-addr", srv.URL, "--timeout", "500ms")
	if code == 0 || out != detachUnknown || !strings.Contains(errs, "the volume stays stopped until the holder settles this detach; repeat the detach") {
		t.Fatalf("exit %d stdout %q stderr %q", code, out, errs)
	}
}

// INV-CLI-CHUNK-LOOP-001: an outcome_unknown answer is repeated even when it is not marked retryable.
func TestChunkLoopRepeatsAnOutcomeUnknownNotMarkedRetryable(t *testing.T) {
	shortLoop(t, 200*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	unknown := `{"code":"outcome_unknown","message":"storage: chunk HEAD CAS outcome unknown","retryable":false}`
	s := &scripted{replies: []scriptedReply{{503, unknown}, {200, published}}}
	srv := s.start(t)
	code, out, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1", "--timeout", "5s")
	if code != 0 || out != published || len(s.recorded()) != 2 {
		t.Fatalf("exit %d stdout %q stderr %q, %d requests: want the unknown answer repeated and the second answer relayed", code, out, errs, len(s.recorded()))
	}
}

// INV-CLI-CHUNK-LOOP-001: a non-positive --timeout is refused before any request, for snapshot, cut and detach.
func TestChunkNonPositiveTimeoutIsRefused(t *testing.T) {
	s := &scripted{replies: []scriptedReply{{200, published}}}
	srv := s.start(t)
	for _, cmd := range [][]string{
		{"snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--request-id", "rid-1"},
		{"cut", "--volume", "v1", "--status-addr", srv.URL},
		{"detach", "--volume", "v1", "--status-addr", srv.URL},
	} {
		for _, tmo := range []string{"0s", "-1s"} {
			code, out, _ := runChunk(append(cmd, "--timeout", tmo)...)
			if code == 0 || !strings.Contains(out, `"code":"bad_request"`) {
				t.Fatalf("%s --timeout %s: exit %d stdout %q, want a bad_request refusal", cmd[0], tmo, code, out)
			}
		}
	}
	if n := len(s.recorded()); n != 0 {
		t.Fatalf("%d requests sent for a non-positive --timeout, want none", n)
	}
}

// The detach's --timeout help says a long bound can mean a long guest stop.
func TestChunkDetachTimeoutHelpNamesTheGuestStop(t *testing.T) {
	_, _, errs := runChunk("detach", "-h")
	if !strings.Contains(errs, "a long --timeout can mean a guest stop that long") {
		t.Fatalf("detach help %q", errs)
	}
}
