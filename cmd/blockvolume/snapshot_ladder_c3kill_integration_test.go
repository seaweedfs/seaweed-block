//go:build subprocess && chunkstore_integration

package main_test

import (
	"fmt"
	"math/rand"
	"sync"
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

// fillReader reads one block of each of its chunks, in order, on its own
// connection: v1 fills a restore target only on access, so this is the fill
// the test kills into. It stops at its first error (the kill, or a failure
// before it, which state reports) or at stop, which waits for it. Its stop
// also runs at the test's cleanup, so a failure before the explicit stop
// still ends and joins the reader.
type fillReader struct {
	mu       sync.Mutex
	reads    int64
	err      error
	finished bool
	quit     chan struct{}
	done     chan struct{}
	close    func()
	once     sync.Once
}

func (env *handoffEnv) readConcurrently(t *testing.T, target string, chunks []uint64) *fillReader {
	t.Helper()
	c := dialNVMe(t, env.hosts["n-"+target].nvme, plannedHandoffNQN)
	r := &fillReader{quit: make(chan struct{}), done: make(chan struct{}), close: c.close}
	t.Cleanup(r.stop)
	go func() {
		defer close(r.done)
		for _, ch := range chunks {
			select {
			case <-r.quit:
				return
			default:
			}
			_ = c.io.SetDeadline(time.Now().Add(60 * time.Second))
			_, err := c.tryRead(ch*ladderChunkBlocks, 4096)
			r.mu.Lock()
			if err != nil {
				r.err = err
			} else {
				r.reads++
			}
			r.mu.Unlock()
			if err != nil {
				return
			}
		}
		r.mu.Lock()
		r.finished = true
		r.mu.Unlock()
	}()
	return r
}

// progress is the reader's state against baseline absent chunks and absent now.
func (r *fillReader) progress(baseline, absent int) fillProgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fillProgress{baseline: baseline, absent: absent, reads: r.reads, readerErr: r.err, finished: r.finished}
}

func (r *fillReader) stop() {
	r.once.Do(func() {
		close(r.quit)
		r.close()
		<-r.done
	})
}

// awaitFill waits until fillWitness accepts the reader's progress on target
// (a fill observably running) and fails the test with the last reason when
// it does not within bound.
func (env *handoffEnv) awaitFill(t *testing.T, target string, r *fillReader, baseline int, bound time.Duration) fillProgress {
	t.Helper()
	var p fillProgress
	var why error
	for end := time.Now().Add(bound); ; time.Sleep(20 * time.Millisecond) {
		p = r.progress(baseline, kzReadStats(t, env.hosts["n-"+target]).AbsentChunks)
		if why = fillWitness(p); why == nil {
			return p
		}
		if p.readerErr != nil || p.finished || time.Now().After(end) {
			t.Fatalf("%s: no running fill to kill into within %v: %v (%+v)", target, bound, why, p)
		}
	}
}

// TestSnapshotLadderC3KilledHydrating: a restore of a snapshot of 32 full
// chunks of incompressible data into v2 (128 MiB), its blockvolume killed
// (SIGKILL) while chunks are being made local. v1 fills a restore target only
// on access, so a reader drives the fill.
//
//  1. Killed with some chunks local and some not, then started again (fill
//     progress not yet written at a checkpoint may be lost: those chunks
//     are fetched again). Every other chunk is read back: each is the
//     snapshot's (a partly filled chunk is never served).
//  2. The guest writes (acknowledged); killed again while a fill is running
//     and restarted, every acknowledged write reads back.
//  3. The guest writes once more and every chunk is made local (no chunk
//     absent).
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
	reader := env.readConcurrently(t, "v2", chunks)
	absent := env.awaitAbsent(t, "v2", "a partly filled volume", 120*time.Second, partly)
	if p := reader.progress(len(chunks), absent); p.readerErr != nil {
		t.Fatalf("the first fill's reader failed before the kill: %v", p.readerErr)
	}
	target.kill(t)
	reader.stop()
	t.Logf("killed with %d of %d chunks absent", absent, len(chunks))
	target = env.startTarget(t, "v2", "blockvolume-v2-after-fill-kill")
	// Fill progress is written at the store's next checkpoint, so a kill
	// may lose some or all of it (standalone-deployment.md, restore step 4):
	// such chunks are absent again and are fetched again on their next read.
	t.Logf("after the restart %d of %d chunks are absent", kzReadStats(t, env.hosts["n-v2"]).AbsentChunks, len(chunks))
	var even []uint64
	for c := uint64(0); c < 32; c += 2 {
		even = append(even, c)
	}
	env.requireBlocksOf(t, "n-v2", subsetOf(src, even)) // no partly filled chunk is served

	// (2) written, then killed again while a fill is running
	w1 := ladderImage("c3kw1", []uint64{3, 17, 50}, 3)
	env.writeImage(t, "n-v2", w1)
	var odd []uint64
	for c := uint64(1); c < 32; c += 2 {
		odd = append(odd, c)
	}
	baseline := kzReadStats(t, env.hosts["n-v2"]).AbsentChunks
	reader = env.readConcurrently(t, "v2", odd)
	p := env.awaitFill(t, "v2", reader, baseline, 60*time.Second)
	target.kill(t)
	reader.stop()
	t.Logf("killed again with a fill running: %d absent (%d when its reader started), %d reads done", p.absent, p.baseline, p.reads)
	target = env.startTarget(t, "v2", "blockvolume-v2-after-write-kill")
	env.requireBlocksOf(t, "n-v2", w1) // every acknowledged write survived the kill

	// (3) every chunk made local
	w2 := ladderImage("c3kw2", []uint64{17, 44, 60}, 2) // not chunk 40: requireEdges keeps it untouched
	env.writeImage(t, "n-v2", w2)
	env.hydrate(t, "v2")
	env.requireImage(t, "n-v2", mergeImages(src, w1, w2))
	env.restoreChecked(t, "base", "v3", src, w1, w2)
}
