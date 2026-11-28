package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
)

// chunkTimeoutHelp is the --timeout text of the commands that use chunkPostLoop.
const chunkTimeoutHelp = "operation bound; above the server's 5m bound on one request, requests are repeated with the same body (for a snapshot the same name and request id) until one answers, a non-retryable refusal ends them, or this passes"

// chunkPostLoop is B2 of the cut budget (v1.0.1, CUT-BUDGET-r1): a cut,
// snapshot or detach whose work outlasts the server's bound on one request
// (chunkRequestBound) finishes across requests. Each request carries the same
// body, so a snapshot keeps its name and request id: a commit that landed
// during a timed-out request is found by the next request, not repeated. The
// holder keeps an unfinished cut's uploads between requests, so each request
// sends only what is missing.
//
// The loop ends on the first answer, on a non-retryable refusal, or when
// timeout has passed; the answer it ends on is relayed as chunkPost relays
// one. Between requests it prints the request's end and, when the holder
// answers, its staged and dirty chunk counts. A timeout within the bound sends
// exactly one request, as before. A request that ends before its bound (the
// store refused at once) is followed by a pause, doubling to 30 s, so a store
// that is down is not asked in a busy loop.
func chunkPostLoop(name, statusAddr, route string, timeout time.Duration, body map[string]any, stdout, stderr io.Writer) int {
	if timeout <= chunkRequestBound {
		return chunkPost(name, statusAddr, route, timeout, body, stdout, stderr)
	}
	start := time.Now()
	deadline := start.Add(timeout)
	pause := min(time.Second, chunkRequestBound)
	maxPause := min(30*time.Second, chunkRequestBound)
	volumeID, _ := body["volumeID"].(string)
	relay := func(code int, out, errb *bytes.Buffer) int {
		_, _ = stderr.Write(errb.Bytes())
		if err := writeAll(stdout, out.Bytes()); err != nil {
			fmt.Fprintf(stderr, "sw-block ops chunk %s: write the result: %v\n", name, err)
			return chunkExitFailed
		}
		return code
	}
	for n := 1; ; n++ {
		per := min(chunkRequestBound, time.Until(deadline))
		sent := time.Now()
		var out, errb bytes.Buffer
		code := chunkPost(name, statusAddr, route, per, body, &out, &errb)
		var e chunkErrBody
		retry := code != ops.VolumeStatusExitOK && json.Unmarshal(out.Bytes(), &e) == nil && e.Code != "" && e.Retryable
		if !retry {
			return relay(code, &out, &errb) // an answer, or a refusal a repeat cannot change
		}
		took := time.Since(sent)
		wait := time.Duration(0)
		if took < per*9/10 {
			wait = pause
			pause = min(2*pause, maxPause)
		}
		// a next request needs time for its own work: end when what is left after the pause is under a second
		// (or under the per-request bound when that is shorter, as in tests)
		if left := time.Until(deadline) - wait; left < min(time.Second, chunkRequestBound/2) {
			fmt.Fprintf(stderr, "sw-block ops chunk %s: request %d ended: %s; --timeout %s passed after %d requests\n", name, n, e.Code, timeout, n)
			return relay(code, &out, &errb)
		}
		fmt.Fprintf(stderr, "sw-block ops chunk %s: request %d ended: %s after %s; request %d follows%s, %s left%s\n",
			name, n, e.Code, took.Round(time.Millisecond), n+1, waitNote(wait), time.Until(deadline).Round(time.Second), chunkProgress(statusAddr, volumeID))
		if wait > 0 {
			time.Sleep(wait)
		}
	}
}

func waitNote(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return " in " + d.Round(time.Millisecond).String()
}

// chunkProgress reads the holder's staged and dirty chunk counts (GET
// /status/durable) for the progress line; empty when it does not answer.
func chunkProgress(statusAddr, volumeID string) string {
	if volumeID == "" {
		return ""
	}
	base := statusAddr
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/status/durable?volume="+url.QueryEscape(volumeID), nil)
	if err != nil {
		return ""
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var st struct {
		Volumes []struct {
			Chunk *struct {
				Stats struct {
					StagedChunks uint64
					DirtyChunks  int
				}
			}
		}
	}
	if json.NewDecoder(io.LimitReader(resp.Body, chunkResponseLimit)).Decode(&st) != nil || len(st.Volumes) == 0 || st.Volumes[0].Chunk == nil {
		return ""
	}
	s := st.Volumes[0].Chunk.Stats
	return fmt.Sprintf(" (holder: %d chunks staged so far, %d dirty)", s.StagedChunks, s.DirtyChunks)
}
