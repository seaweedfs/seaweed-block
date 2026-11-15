package storage

// The barrier bound by pre-copy (B9 W3 bound ruling): while more chunks
// than the bound would be copied under the barrier, a cut stages chunks
// outside it; a staged chunk stands for the barrier copy only while no
// write reached it since staging, so the image at H stays exact.

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// precopyLayer attaches a fresh layer with the given barrier bound and
// writes one block into each of the first n chunks.
func precopyLayer(t *testing.T, bound, n int) (*WALStore, *fakeChunkRemote, *chunkLayerImpl, *writeLog) {
	t.Helper()
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.FreshFromHead = true
	cfg.BarrierBound = bound
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	w := newWriteLog()
	for c := 0; c < n; c++ {
		w.write(t, s, uint32(c*ctChunkBlocks+1), byte(0x10+c))
	}
	return s, f, l, w
}

func assertNoIntentsLeft(t *testing.T, f *fakeChunkRemote) {
	t.Helper()
	in, err := f.ListIntents(context.Background())
	if err != nil || len(in) != 0 {
		t.Fatalf("intents left after the cut settled: %d (%v)", len(in), err)
	}
}

// A chunk re-dirtied between staging and the barrier is copied again under
// the barrier: HEAD has the newer bytes, never the stale stage. Both ways a
// write can reach a staged chunk are covered: flushed to the extent (its
// chunk version moves) and still in the WAL.
func TestChunkPrecopy_RedirtiedStagedChunkIsRecopied(t *testing.T) {
	for _, flushed := range []bool{true, false} {
		name := map[bool]string{true: "flushed", false: "in the WAL"}[flushed]
		t.Run(name, func(t *testing.T) {
			s, f, l, w := precopyLayer(t, 2, 3) // 3 candidates, bound 2
			l.testAfterStageHook = func(round int) {
				if round == 0 { // chunks 0 and 1 are staged now
					w.write(t, s, 1, 0x99) // chunk 0 again
					if flushed {
						mustSyncFlush(t, s)
					}
				}
			}
			r, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if r.Stats.StageRounds != 1 || r.Stats.StagedChunks != 2 {
				t.Fatalf("staging: %d rounds, %d chunks; want 1 round of 2", r.Stats.StageRounds, r.Stats.StagedChunks)
			}
			if r.Stats.StagedChunksUsed != 1 || r.Stats.BarrierCopyChunks != 2 || r.Stats.BarrierOverBound {
				t.Fatalf("barrier: %d staged used, %d copied, over bound %v; want chunk 1 from staging, chunks 0 and 2 copied, within the bound",
					r.Stats.StagedChunksUsed, r.Stats.BarrierCopyChunks, r.Stats.BarrierOverBound)
			}
			assertEveryHeadIsPrefixImage(t, f, w)
			assertNoIntentsLeft(t, f)
		})
	}
}

// Writes that outpace the staging: after three rounds the barrier copies
// the rest even above the bound, counts it, and the image is still exact.
func TestChunkPrecopy_WritesOutpacingStagingGoOverBound(t *testing.T) {
	s, f, l, w := precopyLayer(t, 2, 4)
	l.testAfterStageHook = func(round int) {
		for c := 0; c < 4; c++ { // every chunk dirty again after each round
			w.write(t, s, uint32(c*ctChunkBlocks+2), byte(0x40+round*4+c))
		}
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.StageRounds != 3 { // the ruling: at most three rounds
		t.Fatalf("%d staging rounds, want 3", r.Stats.StageRounds)
	}
	if !r.Stats.BarrierOverBound || r.Stats.BarrierCopyChunks != 4 {
		t.Fatalf("barrier copied %d chunks, over bound %v; want all 4, over the bound of 2", r.Stats.BarrierCopyChunks, r.Stats.BarrierOverBound)
	}
	if got := l.Stats().BarrierOverBound; got != 1 {
		t.Fatalf("BarrierOverBound counter %d, want 1", got)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoIntentsLeft(t, f)
}

// Control: no bound, no staging; within the bound, no staging either.
func TestChunkPrecopy_NoStagingWithoutNeed(t *testing.T) {
	for _, tc := range []struct{ bound, n int }{{0, 5}, {8, 5}} {
		_, f, l, w := precopyLayer(t, tc.bound, tc.n)
		r, err := l.Cut(ctTimeout(t))
		if err != nil {
			t.Fatal(err)
		}
		if r.Stats.StageRounds != 0 || r.Stats.BarrierCopyChunks != tc.n || r.Stats.BarrierOverBound {
			t.Fatalf("bound %d, %d dirty: %d rounds, %d copied, over %v; want no staging, all copied under the barrier",
				tc.bound, tc.n, r.Stats.StageRounds, r.Stats.BarrierCopyChunks, r.Stats.BarrierOverBound)
		}
		assertEveryHeadIsPrefixImage(t, f, w)
	}
}

// A detach with more dirty chunks than the bound uses the same path: it
// stages, and its final HEAD is the exact image.
func TestChunkPrecopy_DetachUsesTheSamePath(t *testing.T) {
	_, f, l, w := precopyLayer(t, 2, 5)
	r, err := l.Detach(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.StageRounds == 0 || r.Stats.BarrierCopyChunks > 2 {
		t.Fatalf("detach: %d staging rounds, %d copied under the barrier; want staging and at most 2 copied", r.Stats.StageRounds, r.Stats.BarrierCopyChunks)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoIntentsLeft(t, f)
}

// W5 holds for staged keys: every upload, staged or at the barrier, is
// named by an intent written before it; a superseded staged key stays
// under its intent until the cut settles, and every intent of the cut is
// deleted only after the confirmed CAS.
func TestChunkPrecopy_StagedUploadsAreCoveredByIntents(t *testing.T) {
	s, f, l, w := precopyLayer(t, 2, 5)
	l.testAfterStageHook = func(round int) {
		if round == 0 {
			w.write(t, s, 1, 0x77) // supersedes chunk 0's staged upload
		}
	}
	mark := len(f.stats().ops)
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	ops := opsSince(f, mark)
	f.mu.Lock()
	keysOf := map[string][]string{}
	for _, in := range f.putIntents {
		keysOf[in.ID] = in.Keys
	}
	f.mu.Unlock()
	covered := map[string]bool{}
	iCAS := indexOf(ops, func(op string) bool { return op == "cas:ok" })
	intents := 0
	for i, op := range ops {
		if id, ok := strings.CutPrefix(op, "putintent:"); ok {
			intents++
			for _, k := range keysOf[id] {
				covered[k] = true
			}
		}
		if k, ok := strings.CutPrefix(op, "put:"); ok && !covered[k] {
			t.Fatalf("upload %q (op %d) not named by an earlier intent: ops %q", k, i, ops)
		}
		if strings.HasPrefix(op, "deleteintent:") && (iCAS < 0 || i < iCAS) {
			t.Fatalf("intent deleted before the confirmed CAS: ops %q", ops)
		}
	}
	if intents < 2 {
		t.Fatalf("%d intents written; want staging rounds and the barrier covered (ops %q)", intents, ops)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoIntentsLeft(t, f)
}

// A write that lands right after a chunk's staging read (flushed before
// the barrier) makes that stage stale: the version is taken before the
// read, so the barrier copies the chunk again and HEAD has the new bytes.
func TestChunkPrecopy_WriteRightAfterStagingReadIsNotLost(t *testing.T) {
	s, f, l, w := precopyLayer(t, 2, 3)
	done := false
	l.testAfterStageReadHook = func(idx uint64) {
		if idx == 0 && !done {
			done = true
			w.write(t, s, 1, 0x5a)
			mustSyncFlush(t, s)
		}
	}
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// A replica apply may land a block at an LSN below another block of the
// same chunk (the highest LSN in the chunk does not move). It still makes
// the staged copy stale: the barrier copies the chunk again and HEAD has
// the applied bytes, whether the apply is still in the WAL or flushed.
func TestChunkPrecopy_LowerLSNApplyAfterStagingIsNotLost(t *testing.T) {
	for _, flushed := range []bool{false, true} {
		name := map[bool]string{true: "flushed", false: "in the WAL"}[flushed]
		t.Run(name, func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			f := newFakeChunkRemote()
			cfg := ctConfig(1)
			cfg.FreshFromHead = true
			cfg.BarrierBound = 1
			l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
			a, b, b2, c := makeBlock(ctBlockSize, 0xa0), makeBlock(ctBlockSize, 0xb5), makeBlock(ctBlockSize, 0xb6), makeBlock(ctBlockSize, 0xc1)
			for _, e := range []struct {
				lba  uint32
				data []byte
				lsn  uint64
			}{{1, b, 5}, {0, a, 10}, {ctChunkBlocks, c, 11}} { // chunk 0 holds a@10 and b@5; chunk 1 c@11
				if err := s.ApplyEntry(e.lba, e.data, e.lsn); err != nil {
					t.Fatal(err)
				}
			}
			applied := false
			l.testAfterStageReadHook = func(idx uint64) {
				if idx == 0 && !applied { // chunk 0 is staged with b@5
					applied = true
					if err := s.ApplyEntry(1, b2, 6); err != nil { // the production apply path
						t.Errorf("apply b@6: %v", err)
					}
					if flushed {
						mustSyncFlush(t, s)
					}
				}
			}
			r, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if !applied || r.Stats.StagedChunks == 0 {
				t.Fatalf("schedule did not run: applied=%v staged=%d", applied, r.Stats.StagedChunks)
			}
			img := f.imageOf(t, r.Head)
			for _, want := range []struct {
				lba  int
				data []byte
			}{{0, a}, {1, b2}, {ctChunkBlocks, c}} {
				if got := img[want.lba*ctBlockSize : (want.lba+1)*ctBlockSize]; !bytes.Equal(got, want.data) {
					t.Fatalf("HEAD at H=%d: LBA %d is %#x..., want %#x... (the b@6 apply after staging lost)", r.H, want.lba, got[0], want.data[0])
				}
			}
			assertNoIntentsLeft(t, f)
		})
	}
}

// A barrier Sync that fails after staging uploaded keys is a clean abort:
// the cut's staging intents are deleted (a failed delete is kept and
// retried by the next cut), and the next cut publishes the exact image.
func TestChunkPrecopy_BarrierSyncFailureSettlesStagedIntents(t *testing.T) {
	s, f, l, w := precopyLayer(t, 2, 5)
	orig := s.committer.syncFunc
	l.testAfterStageHook = func(round int) {
		if round == 0 {
			s.committer.syncFunc = func() error { return errors.New("injected local fsync failure") }
		}
	}
	before := f.stats()
	if _, err := l.Cut(ctTimeout(t)); err == nil || !strings.Contains(err.Error(), "capture sync") {
		t.Fatalf("cut with a failing barrier Sync: %v; want a capture sync error", err)
	}
	if st := f.stats(); st.puts == before.puts {
		t.Fatal("schedule did not stage any upload before the barrier")
	}
	assertNoIntentsLeft(t, f)

	// The same abort with the intent delete failing: kept, retried next cut.
	w.write(t, s, 2, 0x22)
	w.write(t, s, uint32(ctChunkBlocks+2), 0x23)
	w.write(t, s, uint32(2*ctChunkBlocks+2), 0x24)
	f.mu.Lock()
	f.failDeleteIntent = 100
	f.mu.Unlock()
	if _, err := l.Cut(ctTimeout(t)); err == nil {
		t.Fatal("second cut with a failing barrier Sync succeeded")
	}
	if left := len(liveIntents(t, f)); left == 0 {
		t.Fatal("schedule: the failing delete left no intent to retry")
	}
	f.mu.Lock()
	f.failDeleteIntent = 0
	f.mu.Unlock()
	l.testAfterStageHook = nil
	s.committer.syncFunc = orig
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoIntentsLeft(t, f)
}
