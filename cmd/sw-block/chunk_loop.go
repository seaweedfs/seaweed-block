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

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// chunkTimeoutHelp is the --timeout text of the commands that use chunkPostLoop.
const chunkTimeoutHelp = "operation bound; above the server's 5m bound on one request, requests are repeated with the same body (for a snapshot the same name) until one answers, a non-retryable refusal ends them, or this passes"

// chunkDetachTimeoutHelp adds what a long bound means for a detach.
const chunkDetachTimeoutHelp = chunkTimeoutHelp + "; a detach stops guest I/O while it runs, and after an outcome_unknown answer until the holder settles it, so a long --timeout can mean a guest stop that long"

var (
	// chunkHTTPClient is the one client of the /chunk/* requests and the progress reads.
	chunkHTTPClient = http.DefaultClient
	// The pause after a request the store ended early: it starts here and doubles to the cap.
	// Vars only so tests can shorten them.
	chunkPauseStart = time.Second
	chunkPauseMax   = 30 * time.Second
)

// chunkCushion is how long the client waits past a request's own bound for
// its answer (5 s, as chunkPost; less when the bound is short, as in tests).
func chunkCushion() time.Duration { return min(5*time.Second, chunkRequestBound/4) }

// chunkMinRequest is the shortest request the loop sends: with less time left
// it ends instead.
func chunkMinRequest() time.Duration { return min(time.Second, chunkRequestBound/2) }

// chunkReadBound bounds one progress read, so a holder that holds the read
// cannot take a request's time (2 s; less when the bound is short, as in tests).
func chunkReadBound() time.Duration { return min(2*time.Second, chunkRequestBound/10) }

// chunkPostLoop is B2 of the cut budget (v1.0.1, V1-SPEC r2.439): a cut,
// snapshot or detach whose work outlasts the server's bound on one request
// (chunkRequestBound) finishes across requests. Each request carries the same
// body; the server answers a snapshot name it already holds from HEAD, so a
// snapshot that committed during a timed-out request is found by the next one,
// not taken again. The holder keeps an unfinished cut's uploads between
// requests, so each request sends only what is missing.
//
// One deadline, now + timeout, bounds everything the loop does: each request's
// own bound and the client's wait for its answer end inside it, the progress
// read and the pause are cut at it, and a request is sent only while at least
// chunkMinRequest is left (never with a zero or negative bound). The loop ends
// on the first answer, on a non-retryable refusal, or at the deadline, and
// relays the answer it ends on as chunkPost relays one.
//
// Between requests it prints how the request ended and, when the holder
// answers GET /status/durable, the chunks it staged since the first request.
// A request that ended before its bound (the store refused at once) is
// followed by a pause, doubling to chunkPauseMax, so a store that is down is
// not asked in a busy loop. After an outcome_unknown detach the holder has
// stopped guest I/O until it settles the detach in the background; the line
// says so, and a repeat answers from the settled HEAD. A timeout within the
// bound sends exactly one request, as before.
func chunkPostLoop(name, statusAddr, route string, timeout time.Duration, body map[string]any, stdout, stderr io.Writer) int {
	if timeout <= 0 { // INV-CLI-CHUNK-LOOP-001: a non-positive --timeout is refused, never sent as the server's default
		return chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--timeout must be positive", false)
	}
	if timeout <= chunkRequestBound {
		return chunkPost(name, statusAddr, route, timeout, body, stdout, stderr)
	}
	deadline := time.Now().Add(timeout)
	cushion := chunkCushion()
	pause := chunkPauseStart
	volumeID, _ := body["volumeID"].(string)
	first, haveBase := chunkStats(statusAddr, volumeID, deadline) // StagedChunks counts from process start: report the change
	base := first.StagedChunks
	var (
		last           chunkErrBody
		lastCode       int
		lastOut, lastE bytes.Buffer
	)
	relay := func(code int, out, errb *bytes.Buffer) int {
		_, _ = stderr.Write(errb.Bytes())
		if err := writeAll(stdout, out.Bytes()); err != nil {
			fmt.Fprintf(stderr, "sw-block ops chunk %s: write the result: %v\n", name, err)
			return chunkExitFailed
		}
		return code
	}
	for n := 1; ; n++ {
		per := min(chunkRequestBound, time.Until(deadline)-cushion)
		if n == 1 && per < chunkMinRequest() { // not reachable with the bounded first read; never send a request without time for its work
			return chunkFail(name, stdout, stderr, chunkExitFailed, codeBadRequestCLI, fmt.Sprintf("--timeout %s left no time for a request", timeout), false)
		}
		if per < chunkMinRequest() {
			fmt.Fprintf(stderr, "sw-block ops chunk %s: request %d ended: %s; --timeout %s passed after %d requests\n", name, n-1, last.Code, timeout, n-1)
			if name == "detach" && last.Code == chunkstore.CodeOutcomeUnknown {
				fmt.Fprintf(stderr, "sw-block ops chunk %s: the volume stays stopped until the holder settles this detach; repeat the detach to read the settled answer\n", name)
			}
			return relay(lastCode, &lastOut, &lastE)
		}
		sent := time.Now()
		var out, errb bytes.Buffer
		code := chunkPostUntil(name, statusAddr, route, per, sent.Add(per+cushion), body, &out, &errb)
		var e chunkErrBody
		// repeated only after a retryable or outcome_unknown answer (INV-CLI-CHUNK-LOOP-001)
		retry := code != ops.VolumeStatusExitOK && json.Unmarshal(out.Bytes(), &e) == nil && e.Code != "" &&
			(e.Retryable || e.Code == chunkstore.CodeOutcomeUnknown)
		if !retry {
			return relay(code, &out, &errb) // an answer, or a refusal a repeat cannot change
		}
		last, lastCode = e, code
		lastOut.Reset()
		lastOut.Write(out.Bytes())
		lastE.Reset()
		lastE.Write(errb.Bytes())
		took := time.Since(sent)
		wait := time.Duration(0)
		if took < per*9/10 {
			wait = pause
			pause = min(2*pause, chunkPauseMax)
		}
		stopped := ""
		if name == "detach" && e.Code == chunkstore.CodeOutcomeUnknown {
			stopped = "; the volume has stopped guest I/O until this detach settles"
		}
		fmt.Fprintf(stderr, "sw-block ops chunk %s: request %d ended: %s (%s) after %s%s; next request%s, %s left%s\n",
			name, n, e.Code, chunkShort(e.Message), took.Round(time.Millisecond), stopped, waitNote(wait),
			time.Until(deadline).Round(time.Second), chunkProgress(statusAddr, volumeID, base, haveBase, deadline))
		if d := min(wait, time.Until(deadline)); d > 0 {
			time.Sleep(d)
		}
	}
}

func waitNote(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return " in " + d.Round(time.Millisecond).String()
}

func chunkShort(s string) string {
	if s = strings.TrimSpace(s); len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}

// chunkProgress is the progress part of a line: the chunks the holder staged
// since the first request (its counter runs from process start) and how many
// are dirty; empty when the holder does not answer before the deadline.
func chunkProgress(statusAddr, volumeID string, base uint64, haveBase bool, deadline time.Time) string {
	s, ok := chunkStats(statusAddr, volumeID, deadline)
	if !ok {
		return ""
	}
	if !haveBase || s.StagedChunks < base {
		return fmt.Sprintf(" (holder: %d chunks dirty)", s.DirtyChunks)
	}
	return fmt.Sprintf(" (holder: %d chunks staged since the first request, %d dirty)", s.StagedChunks-base, s.DirtyChunks)
}

type chunkStagedStats struct {
	StagedChunks uint64
	DirtyChunks  int
}

// chunkStats reads Volumes[0].Chunk.Stats from GET /status/durable?volume=,
// with the requests' own client, for at most chunkReadBound and never past deadline.
func chunkStats(statusAddr, volumeID string, deadline time.Time) (chunkStagedStats, bool) {
	if volumeID == "" {
		return chunkStagedStats{}, false
	}
	base := statusAddr
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	ctx, cancel := context.WithDeadline(context.Background(), minTime(time.Now().Add(chunkReadBound()), deadline))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/status/durable?volume="+url.QueryEscape(volumeID), nil)
	if err != nil {
		return chunkStagedStats{}, false
	}
	resp, err := chunkHTTPClient.Do(req)
	if err != nil {
		return chunkStagedStats{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return chunkStagedStats{}, false
	}
	var st struct {
		Volumes []struct {
			Chunk *struct{ Stats chunkStagedStats }
		}
	}
	if json.NewDecoder(io.LimitReader(resp.Body, chunkResponseLimit)).Decode(&st) != nil || len(st.Volumes) == 0 || st.Volumes[0].Chunk == nil {
		return chunkStagedStats{}, false
	}
	return st.Volumes[0].Chunk.Stats, true
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
