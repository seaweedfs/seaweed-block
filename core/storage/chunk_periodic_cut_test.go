package storage

import (
	"testing"
	"time"
)

// A periodic cut decided before another cut completed is skipped; decided
// after the last cut, it cuts.
func TestChunkPeriodicCut_SkipsAfterANewerCut(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	pc, ok := layer.(ChunkPeriodicCutter)
	if !ok {
		t.Fatal("the chunk layer has no periodic cut")
	}
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	decided := layer.Stats().CutSeq                    // the timer decides here...
	if _, err := layer.Cut(ctTimeout(t)); err != nil { // ...then another cut completes first
		t.Fatal(err)
	}
	w.write(t, s, 9, 0x29)
	casBefore := f.stats().cas
	if _, skipped, err := pc.CutUnlessCutSince(ctTimeout(t), decided); err != nil || !skipped {
		t.Fatalf("periodic cut decided before a newer cut: skipped=%v err=%v; want skipped", skipped, err)
	}
	if f.stats().cas != casBefore {
		t.Fatal("a skipped periodic cut published")
	}
	r, skipped, err := pc.CutUnlessCutSince(ctTimeout(t), layer.Stats().CutSeq)
	if err != nil || skipped || r.Noop {
		t.Fatalf("periodic cut decided after the last cut: skipped=%v noop=%v err=%v; want a cut", skipped, r.Noop, err)
	}
	if f.stats().cas != casBefore+1 {
		t.Fatalf("CAS calls %d -> %d; want one", casBefore, f.stats().cas)
	}
}

// Cuts are ordered by CutSeq, not by their wall stamps: a newer cut whose
// stamp reads earlier than the one the timer decided on (a backward clock
// step between them) still makes the stale periodic decision skip.
func TestChunkPeriodicCut_SkipsANewerCutWithAnOlderStamp(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	pc := layer.(ChunkPeriodicCutter)
	impl := layer.(*chunkLayerImpl)
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	decided, decidedNs := layer.Stats().CutSeq, layer.Stats().LastCutNs
	w.write(t, s, 5, 0x25)
	if _, err := layer.Cut(ctTimeout(t)); err != nil { // a newer cut completes...
		t.Fatal(err)
	}
	impl.stMu.Lock()
	impl.stats.LastCutNs = decidedNs - int64(time.Hour) // ...stamped by a clock stepped back an hour
	impl.stMu.Unlock()
	w.write(t, s, 9, 0x29)
	casBefore := f.stats().cas
	if _, skipped, err := pc.CutUnlessCutSince(ctTimeout(t), decided); err != nil || !skipped {
		t.Fatalf("stale periodic decision after a newer cut with an older stamp: skipped=%v err=%v; want skipped", skipped, err)
	}
	if f.stats().cas != casBefore {
		t.Fatal("the stale periodic decision published")
	}
}
