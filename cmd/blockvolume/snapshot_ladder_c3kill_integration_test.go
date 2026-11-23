//go:build subprocess && chunkstore_integration

package main_test

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path"
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

// requireBlocksOf reads every block of img on the host and requires each to
// equal img (whole 4 KiB blocks compared byte for byte).
func (env *handoffEnv) requireBlocksOf(t *testing.T, hostID string, img map[uint64][]byte) {
	t.Helper()
	c := dialNVMe(t, env.hosts[hostID].nvme, plannedHandoffNQN)
	defer c.close()
	for lba, want := range img {
		if got := c.read(t, lba, 4096); string(got) != string(want) {
			t.Fatalf("%s: block %d differs from what was written", hostID, lba)
		}
	}
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

// independent reports whether target's independence marker exists, read
// from the filer (not through the product's status, which reports only a
// run of this process).
func (env *handoffEnv) independent(t *testing.T, target string) bool {
	t.Helper()
	c := &http.Client{Timeout: 10 * time.Second}
	p := path.Join(env.chunkPath, target, "RESTORE-INDEPENDENT")
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

// subsetOf returns the blocks of img in the given chunks.
func subsetOf(img map[uint64][]byte, chunks []uint64) map[uint64][]byte {
	in := map[uint64]bool{}
	for _, c := range chunks {
		in[c] = true
	}
	out := map[uint64][]byte{}
	for lba, b := range img {
		if in[lba/ladderChunkBlocks] {
			out[lba] = b
		}
	}
	return out
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
// local at the driver's 64 MiB/s: about 2 s of hydrating).
//
//  1. Its blockvolume is killed (SIGKILL) one second into hydrating and
//     started again. Every other chunk is read back (a partly filled chunk
//     must never be served), which makes those chunks local and leaves the
//     rest to the driver, and the driver is started again.
//  2. The resumed run must report, when its hydrating step ends, that it
//     filled some chunks and not all (0 < filled < 32: it resumed from a
//     partly local volume). Then, the run still going and not independent,
//     the guest writes to v2 (acknowledged); checked still running and not
//     independent, the blockvolume is killed again and restarted. Before the
//     driver is started again the independence marker must be absent (the
//     kill landed before independence); every acknowledged write reads back.
//  3. The driver is started again, the guest writes once more, and the run
//     reaches independence (per its status and the marker). Then v2 holds
//     the snapshot with every acknowledged write over it, read back in full,
//     and the source's snapshot, restored again into v3, is unchanged (none
//     of v2's writes in it).
//
// Not covered: guest writes while the driver reports Owning. The status
// shows owning only once the single batch (32 chunks, under the 64-chunk
// batch) has ended, a few ms before independent.
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
	var even []uint64
	for c := uint64(0); c < 32; c += 2 {
		even = append(even, c)
	}
	env.requireBlocksOf(t, "n-v2", subsetOf(src, even)) // no partly filled chunk is served
	drive()

	// The resumed run: it fills what is not local yet; then, before
	// independence, written and killed.
	notDone := func(s restoreProgress) bool { return s.Phase != "independent" }
	st = env.awaitRestorePhase(t, "v2", "the resumed run's hydrating step ended", 60*time.Second, func(s restoreProgress) bool {
		return s.Running && s.Filled > 0 && notDone(s)
	})
	if st.Filled >= len(chunks) {
		t.Fatalf("the resumed run filled %d chunks: it did not resume from a partly local volume (%d read back before it)", st.Filled, len(even))
	}
	w1 := ladderImage("c3kw1", []uint64{3, 17, 50}, 3)
	env.writeImage(t, "n-v2", w1)
	st = env.requireRestoreState(t, "v2", "running and not independent", notDone)
	target.kill(t)
	t.Logf("killed in the resumed run after it filled %d of %d chunks, after the write: %+v", st.Filled, len(chunks), st)
	target = env.startTarget(t, "v2", "blockvolume-v2-after-batch-kill")
	if env.independent(t, "v2") {
		t.Fatal("the independence marker exists after the second kill: the kill did not land before independence")
	}
	env.requireBlocksOf(t, "n-v2", w1) // every acknowledged write survived the kill
	drive()
	w2 := ladderImage("c3kw2", []uint64{17, 44, 60}, 2) // not chunk 40: requireEdges keeps it untouched
	env.writeImage(t, "n-v2", w2)

	env.waitIndependent(t, "v2")
	if !env.independent(t, "v2") {
		t.Fatal("the restore status says independent but the marker is absent")
	}
	env.requireImage(t, "n-v2", mergeImages(src, w1, w2))
	env.restoreChecked(t, "base", "v3", src, w1, w2)
}
