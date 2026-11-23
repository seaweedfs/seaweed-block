//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// Rung E of the snapshot ladder: faults. E1 kills the holder (SIGKILL) while
// a snapshot is being taken, at several points of the snapshot, and restarts
// it within its lease. The per-step crash cases of the catalog half (after
// the name record, after the intent, with the attempt create unknown) are
// pinned by a unit test of the catalog; this rung is the same property
// through the real processes.

// TestSnapshotLadderE1CrashDuringASnapshot: four rounds on one volume. In
// each, blocks in two new chunks are written and acknowledged, a snapshot
// with a fixed request id is started, and the holder is killed after a
// different delay (from at once to well into the snapshot), then restarted
// within its lease. After each restart:
//   - the snapshot is listed at most once and only active; if its command
//     answered success before the kill, it is listed;
//   - a retry with the same request id completes it, listed once.
//
// At the end a next snapshot is taken after more writes. Every round's
// snapshot and the next one restore (to independence, per the restore
// status) exactly as the volume was when each was requested, with every
// block written after it reading zero: nothing partial, nothing lost.
func TestSnapshotLadderE1CrashDuringASnapshot(t *testing.T) {
	delays := []time.Duration{0, 50 * time.Millisecond, 200 * time.Millisecond, 600 * time.Millisecond}
	targets := []string{"v2", "v3", "v4", "v5", "v6"}
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
	for k, delay := range delays {
		img := ladderImage(fmt.Sprintf("e1r%d", k), []uint64{uint64(k + 1), uint64(20 + k)}, 3)
		env.writeImage(t, "s1", img)
		cum = mergeImages(cum, img)
		images = append(images, cum)
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
		var r result
		select {
		case r = <-done:
		case <-time.After(60 * time.Second):
			cancel()
			t.Fatalf("round %d: the snapshot command did not return within 60 s of the holder's kill", k)
		}
		cancel()
		answered := r.err == nil && r.code == 0
		t.Logf("round %d (kill after %v): snapshot command exit %d err %v", k, delay, r.code, r.err)

		// Restarted within the lease, as the unit's RestartSec does: the same
		// epoch, serving the blocks written before the kill.
		holder = env.start(t, fmt.Sprintf("blockvolume-v1-r%d", k), "s1")
		if got := env.adopt(t, holder, "s1"); got != e {
			t.Fatalf("round %d: the restarted holder adopted epoch %d; want %d", k, got, e)
		}
		env.requireImage(t, "s1", cum)

		switch l := env.snapshots(t, "v1", name); {
		case len(l) > 1:
			t.Fatalf("round %d: %s listed %d times: %+v", k, name, len(l), l)
		case len(l) == 1 && l[0].State != "active":
			t.Fatalf("round %d: %s listed but not active: %+v", k, name, l[0])
		case len(l) == 0 && answered:
			t.Fatalf("round %d: %s answered success before the kill but is not listed: %s", k, name, r.out)
		}
		out, _ := env.h.command(t, "ops", "chunk", "snapshot", "--volume", "v1", "--status-addr", env.hosts["s1"].status,
			"--name", name, "--request-id", req)
		var info snapshotInfo
		if err := json.Unmarshal([]byte(out), &info); err != nil || info.Name != name {
			t.Fatalf("round %d: retry of %s: %v %s", k, name, err, out)
		}
		if l := env.snapshots(t, "v1", name); len(l) != 1 || l[0].State != "active" || l[0].CutLSN != info.CutLSN {
			t.Fatalf("round %d: after the retry %s is listed as %+v; its retry answered %+v", k, name, l, info)
		}
	}

	next := ladderImage("e1next", []uint64{30, 31}, 3)
	env.writeImage(t, "s1", next)
	env.snapshot(t, "s1", "v1", "e1-next")
	final := mergeImages(cum, next)

	for k := range delays {
		var later []map[uint64][]byte
		for j := k + 1; j < len(images); j++ {
			later = append(later, images[j])
		}
		later = append(later, next)
		env.restoreChecked(t, fmt.Sprintf("e1-%d", k), targets[k], images[k], later...)
	}
	env.restoreChecked(t, "e1-next", targets[len(delays)], final)
}
