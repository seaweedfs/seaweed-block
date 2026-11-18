package storage

// W3: per-cut stats (B9 7.1) and cumulative counters (7.2).

import (
	"errors"
	"testing"
)

func TestChunkCutStats_PublishedZeroedUnchangedAndNoop(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)                                            // chunk 0
	w.write(t, s, 5, 0x25)                                            // chunk 1
	if err := flushWithin(t, s, "flush before the cut"); err != nil { // the bitmap counts flushed writes
		t.Fatal(err)
	}
	if d := layer.Stats().DirtyChunks; d != ctNumBlocks/ctChunkBlocks {
		t.Fatalf("dirty chunks before the first cut: %d, want all UNKNOWN chunks", d)
	}

	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	st := r.Stats
	if st.UploadedChunks != 2 || st.UploadedBytes != 2*ctChunkBytes || st.ZeroedChunks != ctNumBlocks/ctChunkBlocks-2 || st.CandidateChunks != ctNumBlocks/ctChunkBlocks {
		t.Fatalf("first cut stats %+v: want 2 uploaded chunks (%d bytes), remaining UNKNOWN chunks zeroed, all chunks candidates", st, 2*ctChunkBytes)
	}
	if st.CASResult != "ok" || st.CASNs < 0 || st.UploadNs < 0 || st.TotalNs <= 0 {
		t.Fatalf("first cut timings/result %+v", st)
	}
	if st.BarrierPauseNs <= 0 || st.BarrierPauseNs < st.SyncNs || st.BarrierPauseNs < st.CopyNs {
		t.Fatalf("barrier %d ns must cover sync %d and copy %d", st.BarrierPauseNs, st.SyncNs, st.CopyNs)
	}
	if st.TotalNs < st.BarrierPauseNs+st.UploadNs+st.CASNs {
		t.Fatalf("total %d ns shorter than barrier %d + upload %d + CAS %d", st.TotalNs, st.BarrierPauseNs, st.UploadNs, st.CASNs)
	}

	w.write(t, s, 2, 0x00) // chunk 0 back to zero: dropped
	w.write(t, s, 5, 0x25) // chunk 1 rewritten with the same bytes: unchanged
	r2, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if st2 := r2.Stats; st2.UploadedChunks != 0 || st2.ZeroedChunks != 1 || st2.UnchangedChunks != 1 || st2.CASResult != "ok" {
		t.Fatalf("second cut stats %+v: want 0 uploaded, 1 zeroed, 1 unchanged, CAS ok", st2)
	}

	r3, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !r3.Noop || r3.Stats.CASResult != "" || r3.Stats.UploadedChunks != 0 || r3.Stats.CASNs != 0 {
		t.Fatalf("no-op cut: noop=%v stats %+v; want no CAS and no upload", r3.Noop, r3.Stats)
	}

	c := layer.Stats()
	if c.DirtyChunks != 0 {
		t.Fatalf("dirty chunks after the cuts: %d, want 0", c.DirtyChunks)
	}
	if c.CutsOK != 2 || c.CutsNoop != 1 || c.CutsFailed != 0 || len(c.CutsRefused) != 0 {
		t.Fatalf("counters %+v: want 2 ok, 1 noop", c)
	}
	if c.LastCutH != r3.H || c.LastCutNs == 0 || c.LastCut.TotalNs != r3.Stats.TotalNs {
		t.Fatalf("last cut %+v (H %d), want the no-op cut H %d", c.LastCut, c.LastCutH, r3.H)
	}
}

func TestChunkCutStats_RefusalsConflictsAndUnknown(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	refuse := false
	cfg.CaptureAllowed = func() error {
		if refuse {
			return errors.New("not primary")
		}
		return nil
	}
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()

	// Admission refused.
	w.write(t, s, 2, 0x22)
	refuse = true
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
		t.Fatalf("refused cut: %v", err)
	}
	refuse = false

	// Unknown outcome, then settled by the next cut.
	f.mu.Lock()
	f.casModes = []fakeCASMode{casUnknownNotApplied}
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("unknown cut: %v", err)
	}
	if c := layer.Stats(); c.CASUnknown != 1 || !c.PendingUnresolved || c.CutsFailed != 1 {
		t.Fatalf("after an unknown CAS outcome: %+v", c)
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("settling cut: %v", err)
	}
	// The settling cut lands the held proposal first; with nothing new
	// after it, the cut itself is a no-op.
	if c := layer.Stats(); c.PendingUnresolved || c.CutsOK+c.CutsNoop != 1 {
		t.Fatalf("after settlement: %+v", c)
	}

	// Taken over: the CAS conflicts and the layer fences.
	w.write(t, s, 5, 0x25)
	f.mu.Lock()
	f.beforeCAS = func(f *fakeChunkRemote) { f.takeOverLocked("host-b") }
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("taken-over cut: %v", err)
	}
	c := layer.Stats()
	if c.CutsRefused["admission"] != 1 || c.CutsRefused["fenced"] != 1 || c.CASConflicts != 1 || c.CASUnknown != 1 {
		t.Fatalf("counters %+v: want 1 admission refusal, 1 fenced, 1 CAS conflict", c)
	}
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("cut on a fenced layer: %v", err)
	}
	if c := layer.Stats(); c.CutsRefused["fenced"] != 2 {
		t.Fatalf("fenced refusals %d, want 2", c.CutsRefused["fenced"])
	}
}
