//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
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

// restoreStatus reads target's restore status straight from its status
// server (a GET answers the status without starting a run).
func (env *handoffEnv) restoreStatus(c *http.Client, target string) (restoreProgress, string, bool) {
	resp, err := c.Get("http://" + env.hosts["n-"+target].status + "/chunk/restore?volume=" + target)
	if err != nil {
		return restoreProgress{}, "", false
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	var st restoreProgress
	if resp.StatusCode != http.StatusOK || json.Unmarshal(b, &st) != nil {
		return restoreProgress{}, string(b), false
	}
	return st, string(b), true
}

// awaitRestorePhase polls target's restore status every 10 ms until ok
// holds and returns that status, logging each change with when it was seen
// (the record of where a kill landed). It fails the test if the restore
// failed, became independent first (the moment was missed), or bound passed.
// The driver's counters (filled, owned, foreign) are written when a step
// ends, not while it runs: a moment inside a step is chosen by the phase and
// the time spent in it, not by a counter.
func (env *handoffEnv) awaitRestorePhase(t *testing.T, target, what string, bound time.Duration, ok func(restoreProgress) bool) restoreProgress {
	t.Helper()
	c := &http.Client{Timeout: 2 * time.Second}
	began := time.Now()
	last := restoreProgress{Phase: "-"}
	for time.Since(began) < bound {
		if st, raw, good := env.restoreStatus(c, target); good {
			if st != last {
				t.Logf("restore of %s at +%v (waiting for %s): %+v", target, time.Since(began).Round(time.Millisecond), what, st)
				last = st
			}
			switch {
			case ok(st):
				return st
			case st.Phase == "failed":
				t.Fatalf("restore of %s failed waiting for %s: %s", target, what, raw)
			case st.Phase == "independent":
				t.Fatalf("restore of %s became independent before %s was seen (the restore is too small to catch it): %s", target, what, raw)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("restore of %s: %s not seen within %v", target, what, bound)
	return restoreProgress{}
}

// requireRestoreState reads target's restore status once and fails the test
// unless the run is still going and ok holds: the check, right before a
// kill, that the kill lands where the test says.
func (env *handoffEnv) requireRestoreState(t *testing.T, target, what string, ok func(restoreProgress) bool) restoreProgress {
	t.Helper()
	st, raw, good := env.restoreStatus(&http.Client{Timeout: 2 * time.Second}, target)
	if !good || !st.Running || !ok(st) {
		t.Fatalf("restore of %s is no longer %s before the kill: %s", target, what, raw)
	}
	return st
}

// denseImage fills every block of the given chunks with incompressible
// bytes (seeded by tag and LBA), so that their chunks are full size in the
// store and a restore of them takes measurable time at the driver's rate.
func denseImage(tag string, chunks []uint64) map[uint64][]byte {
	img := map[uint64][]byte{}
	var seed int64
	for _, c := range tag {
		seed = seed*31 + int64(c)
	}
	for _, c := range chunks {
		for i := uint64(0); i < ladderChunkBlocks; i++ {
			lba := c*ladderChunkBlocks + i
			b := make([]byte, 4096)
			rand.New(rand.NewSource(seed ^ int64(lba))).Read(b)
			img[lba] = b
		}
	}
	return img
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

// TestSnapshotLadderC3KilledHydratingAndBeforeIndependence: a restore of a
// snapshot of 32 full chunks of incompressible data into v2 (128 MiB to make
// local at the driver's 64 MiB/s: about 2 s of hydrating). Its blockvolume
// is killed (SIGKILL) one second into hydrating, started again, and read
// back in full (a partly filled chunk must never be served), and the driver
// is started again. When the status then shows every chunk filled and no
// batch done, still running and not independent (after hydration completed,
// before independence), the guest writes to v2 (acknowledged) and the
// blockvolume is killed again, restarted (every acknowledged write reads
// back), and the driver started again; the guest writes once more. The
// driver resumes from the target's remote facts to independence (per its
// status). Then v2 holds the snapshot with every acknowledged write over it,
// and the source's snapshot, restored again into v3, is unchanged (none of
// v2's writes in it).
//
// Not covered: guest writes while the driver reports Owning. With one batch
// (32 chunks, under the 64-chunk batch) the status goes from hydrating to
// independent without ever showing owning, so the second kill is only known
// to land after hydration completed and before independence.
func TestSnapshotLadderC3KilledHydratingAndBeforeIndependence(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	var chunks []uint64
	for c := uint64(0); c < 32; c++ {
		chunks = append(chunks, c)
	}
	src := denseImage("c3ksrc", chunks)
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

	// Killed one second into hydrating.
	env.awaitRestorePhase(t, "v2", "hydrating", 120*time.Second, func(s restoreProgress) bool { return s.Phase == "hydrating" })
	time.Sleep(time.Second)
	st := env.requireRestoreState(t, "v2", "hydrating", func(s restoreProgress) bool { return s.Phase == "hydrating" })
	target.kill(t)
	t.Logf("killed 1 s into hydrating: %+v", st)
	target = env.startTarget(t, "v2", "blockvolume-v2-after-hydrate-kill")
	env.requireImage(t, "n-v2", src) // serves the snapshot, no partly filled chunk
	drive()

	// Written after hydration completed and before independence, then killed.
	afterFill := func(s restoreProgress) bool {
		return s.Filled == len(chunks) && s.Batches == 0 && s.Phase != "independent"
	}
	env.awaitRestorePhase(t, "v2", "hydration completed, not independent", 180*time.Second, func(s restoreProgress) bool { return s.Running && afterFill(s) })
	w1 := ladderImage("c3kw1", []uint64{3, 17, 50}, 3)
	env.writeImage(t, "n-v2", w1)
	st = env.requireRestoreState(t, "v2", "after hydration and before independence", afterFill)
	target.kill(t)
	t.Logf("killed after hydration completed and before independence, after the write: %+v", st)
	target = env.startTarget(t, "v2", "blockvolume-v2-after-batch-kill")
	env.requireImage(t, "n-v2", mergeImages(src, w1)) // every acknowledged write survived the kill
	drive()
	w2 := ladderImage("c3kw2", []uint64{17, 44, 60}, 2) // not chunk 40: requireEdges keeps it untouched
	env.writeImage(t, "n-v2", w2)

	env.waitIndependent(t, "v2")
	env.requireImage(t, "n-v2", mergeImages(src, w1, w2))
	env.restoreChecked(t, "base", "v3", src, w1, w2)
}
