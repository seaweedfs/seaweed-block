//go:build subprocess && chunkstore_integration

package main_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// awaitAbsent polls target's chunk status every 10 ms until ok holds for its
// count of chunks not yet local, and returns that count. A volume that was
// already fully local before ok held fails the test: the moment was missed.
func (env *handoffEnv) awaitAbsent(t *testing.T, target, what string, bound time.Duration, ok func(absent int) bool) int {
	t.Helper()
	b := env.hosts["n-"+target]
	began := time.Now()
	last := -1
	for time.Since(began) < bound {
		absent := kzReadStats(t, b).AbsentChunks
		if absent != last {
			t.Logf("%s at +%v (waiting for %s): %d chunks absent", target, time.Since(began).Round(time.Millisecond), what, absent)
			last = absent
		}
		if ok(absent) {
			return absent
		}
		if absent == 0 {
			t.Fatalf("%s hydrated before %s was seen (the restore is too small to catch it)", target, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s: %s not seen within %v (last %d chunks absent)", target, what, bound, last)
	return -1
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

// TestSnapshotLadderC3KilledHydrating: a restore of a snapshot of 32 full
// chunks of incompressible data into v2 (128 MiB its background fill makes
// local), its blockvolume killed (SIGKILL) while the fill is under way.
//
//  1. Killed with some chunks local and some not, then started again. Every
//     other chunk is read back: each is the snapshot's (a partly filled
//     chunk is never served), and reading makes it local.
//  2. The fill resumes from what is local: still some chunks absent, then
//     fewer. The guest writes (acknowledged); killed again before the fill
//     ends and restarted, every acknowledged write reads back.
//  3. The guest writes once more and the fill completes (no chunk absent).
//     v2 holds the snapshot with every acknowledged write over it, read back
//     in full, and the source's snapshot, restored again into v3, is
//     unchanged (none of v2's writes in it).
func TestSnapshotLadderC3KilledHydrating(t *testing.T) {
	env := startLadderEnv(t, "v2", "v3")
	var chunks []uint64
	for c := uint64(0); c < 32; c++ {
		chunks = append(chunks, c)
	}
	src := denseImage("c3ksrc", chunks)
	env.writeImage(t, "s1", src)
	env.snapshot(t, "s1", "v1", "base")

	env.h.command(t, "ops", "chunk", "restore", "--master", env.h.masterAddr, "--source", "v1", "--snapshot", "base", "--target", "v2",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath)
	target := env.startTarget(t, "v2", "blockvolume-v2")

	// (1) killed mid-fill
	partly := func(absent int) bool { return absent > 0 && absent < len(chunks) }
	absent := env.awaitAbsent(t, "v2", "a partly filled volume", 120*time.Second, partly)
	target.kill(t)
	t.Logf("killed with %d of %d chunks absent", absent, len(chunks))
	target = env.startTarget(t, "v2", "blockvolume-v2-after-fill-kill")
	var even []uint64
	for c := uint64(0); c < 32; c += 2 {
		even = append(even, c)
	}
	env.requireBlocksOf(t, "n-v2", subsetOf(src, even)) // no partly filled chunk is served

	// (2) the fill resumes; written and killed again before it ends
	resumed := env.awaitAbsent(t, "v2", "the resumed fill", 60*time.Second, func(a int) bool { return a > 0 })
	if resumed >= len(chunks) {
		t.Fatalf("after the restart %d of %d chunks are absent: nothing local survived the kill", resumed, len(chunks))
	}
	w1 := ladderImage("c3kw1", []uint64{3, 17, 50}, 3)
	env.writeImage(t, "n-v2", w1)
	env.awaitAbsent(t, "v2", "the fill still running after the write", time.Second, func(a int) bool { return a > 0 })
	target.kill(t)
	target = env.startTarget(t, "v2", "blockvolume-v2-after-write-kill")
	env.requireBlocksOf(t, "n-v2", w1) // every acknowledged write survived the kill

	// (3) completes
	w2 := ladderImage("c3kw2", []uint64{17, 44, 60}, 2) // not chunk 40: requireEdges keeps it untouched
	env.writeImage(t, "n-v2", w2)
	env.waitHydrated(t, "v2")
	env.requireImage(t, "n-v2", mergeImages(src, w1, w2))
	env.restoreChecked(t, "base", "v3", src, w1, w2)
}
