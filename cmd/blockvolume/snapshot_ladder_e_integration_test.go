//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"testing"
	"time"
)

// Rung E of the snapshot ladder: faults. E1 kills the holder (SIGKILL) while
// a snapshot is being taken, at several points of the snapshot, and restarts
// it within its lease. The per-step crash cases of the catalog half (after
// the name record, after the intent, with the attempt create unknown) are
// pinned by a unit test of the catalog; this rung is the same property
// through the real processes.

// nameRecorded reports whether vol's catalog holds a name record for the
// snapshot name, read from the filer (not through the product).
func (env *handoffEnv) nameRecorded(t *testing.T, vol, name string) bool {
	t.Helper()
	c := &http.Client{Timeout: 10 * time.Second}
	p := path.Join(env.chunkPath, vol, "snapnames", url.PathEscape(name))
	resp, err := c.Get("http://" + env.cluster.filerHTTP + p + "?metadata=true")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusNotFound:
		return false
	}
	t.Fatalf("filer lookup of %s: status %d", p, resp.StatusCode)
	return false
}

// TestSnapshotLadderE1CrashDuringASnapshot: rounds on one volume. In each,
// blocks in two new chunks are written and acknowledged, a snapshot with a
// fixed request id is started, and the holder is killed after a different
// delay (from at once to well after the answer), then restarted within its
// lease. Each round is classified from what the kill left: ANSWERED (the
// command answered success first), MID (no answer, but the catalog holds the
// snapshot's name record: the kill landed inside the snapshot), or BEFORE
// (no answer and no name record). The test fails as inconclusive unless at
// least one round is MID. After each restart, before the retry:
//   - the snapshot is listed at most once and only active; an ANSWERED one
//     is listed, at the cut its answer gave;
//   - one more block is written (acknowledged).
//
// Then a retry with the same request id completes the snapshot, listed once:
// at the cut listed before the retry if it was listed (a replay), else at a
// new cut that holds the post-restart block. At the end a next snapshot is
// taken after more writes. Every round's snapshot and the next one restore
// (to independence, per the restore status) exactly as expected: the blocks
// acknowledged before it, the post-restart block only when the retry cut
// anew, and every block written after it reading zero.
func TestSnapshotLadderE1CrashDuringASnapshot(t *testing.T) {
	delays := []time.Duration{0, 25 * time.Millisecond, 50 * time.Millisecond, 75 * time.Millisecond,
		100 * time.Millisecond, 125 * time.Millisecond, 150 * time.Millisecond, 200 * time.Millisecond, 600 * time.Millisecond}
	var targets []string
	for i := 0; i <= len(delays); i++ {
		targets = append(targets, fmt.Sprintf("v%d", i+2))
	}
	env := startHandoffEnvWith(t, 1, targets)
	env.volExtra = []string{"--chunk-capture-interval", "0", "--chunk-snapshot-schedule", "off"}
	holder := env.start(t, "blockvolume-v1", "s1")
	e := env.adopt(t, holder, "s1")
	marker := pattern("ladder-e1-marker")
	env.writeBlock(t, "s1", 0, marker)

	type result struct {
		out, errOut string
		code        int
		err         error
	}
	var images []map[uint64][]byte // what each round's snapshot must hold
	cum := map[uint64][]byte{0: marker}
	classes := map[string]int{}
	for k, delay := range delays {
		img := ladderImage(fmt.Sprintf("e1r%d", k), []uint64{uint64(k + 1), uint64(20 + k)}, 3)
		env.writeImage(t, "s1", img)
		cum = mergeImages(cum, img)
		name, req := fmt.Sprintf("e1-%d", k), fmt.Sprintf("r-e1-%d", k)

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		done := make(chan result, 1)
		go func() {
			out, errOut, code, err := runCLI(ctx, env.h, "ops", "chunk", "snapshot", "--volume", "v1", "--status-addr", env.hosts["s1"].status,
				"--name", name, "--request-id", req)
			done <- result{out, errOut, code, err}
		}()
		time.Sleep(delay)
		holder.kill(t)
		killed := time.Now()
		var r result
		select {
		case r = <-done:
		case <-time.After(60 * time.Second):
			cancel()
			select { // join the worker before failing
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("the snapshot worker did not finish within 10 s of its cancellation")
			}
			t.Fatalf("round %d: the snapshot command did not return within 60 s of the holder's kill", k)
		}
		cancel()
		answered := r.err == nil && r.code == 0
		class := "BEFORE"
		switch {
		case answered:
			class = "ANSWERED"
		case env.nameRecorded(t, "v1", name):
			class = "MID"
		}
		classes[class]++

		// Restarted within the lease, as the unit's RestartSec does: the same
		// epoch, serving the blocks written before the kill.
		holder = env.start(t, fmt.Sprintf("blockvolume-v1-r%d", k), "s1")
		if got := env.adopt(t, holder, "s1"); got != e {
			t.Fatalf("round %d: the restarted holder adopted epoch %d; want %d (restarted %v after the kill: within the lease?)", k, got, e, time.Since(killed).Round(time.Millisecond))
		}
		adopted := time.Since(killed)
		poll(t, 60*time.Second, fmt.Sprintf("round %d: the restarted holder never reported its frontend ready", k), func() bool {
			return env.frontendReadyAt("s1")
		})
		t.Logf("round %d (kill after %v): %s (exit %d err %v); adopted %v and frontend ready %v after the kill", k, delay, class, r.code, r.err,
			adopted.Round(time.Millisecond), time.Since(killed).Round(time.Millisecond))
		env.requireImage(t, "s1", cum)

		pre := env.snapshots(t, "v1", name)
		switch {
		case len(pre) > 1:
			t.Fatalf("round %d: %s listed %d times: %+v", k, name, len(pre), pre)
		case len(pre) == 1 && pre[0].State != "active":
			t.Fatalf("round %d: %s listed but not active: %+v", k, name, pre[0])
		case answered && len(pre) == 0:
			t.Fatalf("round %d: %s answered success before the kill but is not listed: %s", k, name, r.out)
		}
		if answered {
			var a snapshotInfo
			if err := json.Unmarshal([]byte(r.out), &a); err != nil || a.CutLSN != pre[0].CutLSN || a.HeadGen != pre[0].HeadGen {
				t.Fatalf("round %d: %s answered %s before the kill but is listed as %+v", k, name, r.out, pre[0])
			}
		}
		post := map[uint64][]byte{uint64(50+k) * ladderChunkBlocks: pattern(fmt.Sprintf("e1-post-%02d-blk!", k))}
		env.writeImage(t, "s1", post)

		out, _ := env.h.command(t, "ops", "chunk", "snapshot", "--volume", "v1", "--status-addr", env.hosts["s1"].status,
			"--name", name, "--request-id", req)
		var info snapshotInfo
		if err := json.Unmarshal([]byte(out), &info); err != nil || info.Name != name {
			t.Fatalf("round %d: retry of %s: %v %s", k, name, err, out)
		}
		if len(pre) == 1 && (info.CutLSN != pre[0].CutLSN || info.HeadGen != pre[0].HeadGen) {
			t.Fatalf("round %d: %s was listed as %+v before the retry, which answered %+v: a listed snapshot was taken again", k, name, pre[0], info)
		}
		if l := env.snapshots(t, "v1", name); len(l) != 1 || l[0].State != "active" || l[0].CutLSN != info.CutLSN {
			t.Fatalf("round %d: after the retry %s is listed as %+v; its retry answered %+v", k, name, l, info)
		}
		if len(pre) == 1 {
			images = append(images, cum) // a replay: the post-restart block is not in it
		} else {
			images = append(images, mergeImages(cum, post)) // cut anew after the restart
		}
		cum = mergeImages(cum, post)
	}
	t.Logf("rounds: %v", classes)
	if classes["MID"] == 0 {
		t.Fatalf("inconclusive: no kill landed inside a snapshot (rounds %v)", classes)
	}

	next := ladderImage("e1next", []uint64{30, 31}, 3)
	env.writeImage(t, "s1", next)
	env.snapshot(t, "s1", "v1", "e1-next")
	final := mergeImages(cum, next)

	for k := range delays {
		later := []map[uint64][]byte{final}
		env.restoreChecked(t, fmt.Sprintf("e1-%d", k), targets[k], images[k], later...)
	}
	env.restoreChecked(t, "e1-next", targets[len(delays)], final)
}
