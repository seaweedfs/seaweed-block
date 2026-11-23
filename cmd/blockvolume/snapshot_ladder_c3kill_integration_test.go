//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// restoreProgress is the restore driver's status of a target.
type restoreProgress struct {
	Phase   string `json:"phase"`
	Running bool   `json:"running"`
	Filled  int    `json:"filled"`
	Owned   int    `json:"owned"`
	Foreign int    `json:"foreign"`
	Batches int    `json:"batches"`
}

// awaitRestorePhase polls target's restore status every 50 ms until ok
// holds and returns that status; it fails the test if the restore failed,
// became independent first (the moment was missed), or bound passed.
func (env *handoffEnv) awaitRestorePhase(t *testing.T, target, what string, bound time.Duration, ok func(restoreProgress) bool) restoreProgress {
	t.Helper()
	end := time.Now().Add(bound)
	ctx, cancel := context.WithDeadline(context.Background(), end)
	defer cancel()
	for time.Now().Before(end) {
		out, _, code, err := runCLI(ctx, env.h, "ops", "chunk", "restore", "--volume", target, "--status-addr", env.hosts["n-"+target].status, "--status")
		var st restoreProgress
		if err == nil && code == 0 && json.Unmarshal([]byte(out), &st) == nil {
			switch {
			case ok(st):
				return st
			case st.Phase == "failed":
				t.Fatalf("restore of %s failed waiting for %s: %s", target, what, out)
			case st.Phase == "independent":
				t.Fatalf("restore of %s became independent before %s was seen (the restore is too small to catch it): %s", target, what, out)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("restore of %s: %s not seen within %v", target, what, bound)
	return restoreProgress{}
}

// startTarget starts target's blockvolume and waits until it serves.
func (env *handoffEnv) startTarget(t *testing.T, target, name string) *daemon {
	t.Helper()
	b := env.hosts["n-"+target]
	d := env.h.start(t, name, env.h.volume, env.volArgsFor(b)...)
	d.waitLog(t, 60*time.Second, fmt.Sprintf("blockvolume: volume %s adopted as PRIMARY at r1@", target))
	poll(t, 60*time.Second, fmt.Sprintf("%s never reported its frontend ready", target), func() bool {
		return volumeFrontendReady(b.status, target)
	})
	return d
}

// TestSnapshotLadderC3KilledMidHydrateAndMidBatch: a restore of a snapshot
// of 48 written chunks into v2 (about 192 MiB to make local and re-upload).
// Its blockvolume is killed (SIGKILL) while the driver is hydrating, started
// again and the driver started again. While the driver then owns chunks, the
// guest writes to v2 (acknowledged), and the blockvolume is killed again in
// the middle of the batch, restarted, and the driver started again; the
// guest writes once more. The driver resumes from the target's remote facts
// to independence (per its status). Then v2 holds the snapshot with every
// acknowledged write over it, and the source's snapshot, restored again
// into v3, is unchanged (none of v2's writes in it).
func TestSnapshotLadderC3KilledMidHydrateAndMidBatch(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	var chunks []uint64
	for c := uint64(0); c < 48; c++ {
		chunks = append(chunks, c)
	}
	src := ladderImage("c3ksrc", chunks, 2)
	env.writeImage(t, "s1", src)
	env.snapshot(t, "s1", "v1", "base")

	env.h.command(t, "ops", "chunk", "restore", "--source", "v1", "--snapshot", "base", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	target := env.startTarget(t, "v2", "blockvolume-v2")
	drive := func() {
		t.Helper()
		env.h.command(t, "ops", "chunk", "restore", "--volume", "v2", "--status-addr", env.hosts["n-v2"].status)
	}
	drive()

	// Killed while hydrating: some chunks local, not all.
	st := env.awaitRestorePhase(t, "v2", "hydrating with part of the chunks local", 120*time.Second, func(s restoreProgress) bool {
		return s.Phase == "hydrating" && s.Filled > 0 && s.Filled < len(chunks)
	})
	target.kill(t)
	t.Logf("killed while hydrating: %+v", st)
	target = env.startTarget(t, "v2", "blockvolume-v2-after-hydrate-kill")
	env.requireImage(t, "n-v2", src) // serves the snapshot at once after the restart
	drive()

	// Written while owning, then killed in the middle of the batch.
	env.awaitRestorePhase(t, "v2", "owning", 180*time.Second, func(s restoreProgress) bool { return s.Phase == "owning" })
	w1 := ladderImage("c3kw1", []uint64{3, 17, 50}, 3)
	env.writeImage(t, "n-v2", w1)
	st = env.awaitRestorePhase(t, "v2", "owning with chunks still foreign", 180*time.Second, func(s restoreProgress) bool {
		return s.Phase == "owning" && s.Foreign > 0
	})
	target.kill(t)
	t.Logf("killed while owning: %+v", st)
	target = env.startTarget(t, "v2", "blockvolume-v2-after-batch-kill")
	env.requireImage(t, "n-v2", mergeImages(src, w1)) // every acknowledged write survived the kill
	drive()
	w2 := ladderImage("c3kw2", []uint64{17, 40, 60}, 2)
	env.writeImage(t, "n-v2", w2)

	env.waitIndependent(t, "v2")
	env.requireImage(t, "n-v2", mergeImages(src, w1, w2))
	env.restoreChecked(t, "base", "v3", src, w1, w2)
}
