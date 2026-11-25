package storage

// The barrier bound by pre-copy (B9 W3 bound ruling): while more chunks
// than the bound would be copied under the barrier, a cut stages chunks
// outside it; a staged chunk stands for the barrier copy only while no
// write reached it since staging, so the image at H stays exact.

import (
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
	if bound > 0 {
		cfg.COWSpillLimit = 2 * int64(bound) * ctChunkBytes
	}
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	// A new v3 map is conservatively UNKNOWN. Publish the zero baseline
	// first so this control isolates subsequent pre-copy redirty behavior.
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w := newWriteLog()
	for c := 0; c < n; c++ {
		w.write(t, s, uint32(c*ctChunkBlocks+1), byte(0x10+c))
	}
	return s, f, l, w
}

// A chunk re-dirtied between staging and the barrier is copied again in
// the COW window: HEAD has the newer bytes, never the stale stage. Both ways a
// write can reach a staged chunk are covered: flushed to the extent (its
// chunk version moves) and still in the WAL.
func TestChunkPrecopy_RedirtiedStagedChunkIsRecopied(t *testing.T) {
	for _, flushed := range []bool{true, false} {
		name := map[bool]string{true: "flushed", false: "in the WAL"}[flushed]
		t.Run(name, func(t *testing.T) {
			s, f, l, w := precopyLayer(t, 2, 3) // 3 candidates, bound 2
			l.testAfterStageHook = func(round int) {
				if round == 0 { // recent chunks 2 and 1 are staged now
					w.write(t, s, 9, 0x99) // chunk 2 again
					if flushed {
						mustSyncFlush(t, s)
					}
				}
			}
			r, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if r.Stats.StageRounds != 1 || r.Stats.StagedChunks != 3 {
				t.Fatalf("staging: %d rounds, %d chunks; want 1 round of 3", r.Stats.StageRounds, r.Stats.StagedChunks)
			}
			if r.Stats.StagedChunksUsed != 2 || r.Stats.BarrierCopyChunks != 0 || r.Stats.CandidateChunks != 3 || r.Stats.BarrierOverBound {
				t.Fatalf("barrier: %d staged used, %d copied, over bound %v; want chunks 0 and 1 from staging, redirtied chunk 2 read by COW",
					r.Stats.StagedChunksUsed, r.Stats.BarrierCopyChunks, r.Stats.BarrierOverBound)
			}
			assertEveryHeadIsPrefixImage(t, f, w)
			assertNoSessionOpen(t, f)
		})
	}
}

// Writes that outpace staging: after three rounds the COW window copies
// all remaining chunks outside the barrier, keeping the image exact.
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
	if r.Stats.StageRounds != 1 { // no shrink: fix the frontier instead of repeating the same round
		t.Fatalf("%d staging rounds, want 1", r.Stats.StageRounds)
	}
	if r.Stats.BarrierOverBound || r.Stats.BarrierCopyChunks != 0 || r.Stats.CandidateChunks != 4 {
		t.Fatalf("barrier copied %d chunks, over bound %v; want all 4 candidates copied outside the barrier", r.Stats.BarrierCopyChunks, r.Stats.BarrierOverBound)
	}
	if got := l.Stats().BarrierOverBound; got != 0 {
		t.Fatalf("BarrierOverBound counter %d, want 0 for COW", got)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoSessionOpen(t, f)
}

// Control: no bound, no staging; within the bound, no staging either.
func TestChunkPrecopy_NoStagingWithoutNeed(t *testing.T) {
	for _, tc := range []struct{ bound, n int }{{0, 5}, {8, 5}} {
		_, f, l, w := precopyLayer(t, tc.bound, tc.n)
		r, err := l.Cut(ctTimeout(t))
		if err != nil {
			t.Fatal(err)
		}
		if r.Stats.StageRounds != 0 || r.Stats.BarrierCopyChunks != 0 || r.Stats.CandidateChunks != tc.n || r.Stats.BarrierOverBound {
			t.Fatalf("bound %d, %d dirty: %d rounds, %d copied, over %v; want no staging, all copied outside the barrier",
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
	assertNoSessionOpen(t, f)
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
	open, begins := false, 0
	for i, op := range ops {
		switch {
		case strings.HasPrefix(op, "begin:") && op != "begin:fail":
			open = true
			begins++
		case strings.HasPrefix(op, "abort:"), op == "cas:ok":
			open = false
		}
		if k, ok := strings.CutPrefix(op, "put:"); ok && !open {
			t.Fatalf("upload %q (op %d) outside an open session: ops %q", k, i, ops)
		}
	}
	if begins < 2 {
		t.Fatalf("%d Begins; want the staging rounds and the barrier each in the session (ops %q)", begins, ops)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoSessionOpen(t, f)
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

// A barrier Sync that fails after staging uploaded keys keeps the session
// with the staged uploads open, and the next cut publishes the exact image
// in it.
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
	if f.openSession() == "" {
		t.Fatal("completed uploads lost their open session")
	}
	l.testAfterStageHook = nil
	s.committer.syncFunc = orig
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	assertNoSessionOpen(t, f)
}
