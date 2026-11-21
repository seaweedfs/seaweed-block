package storage

import "testing"

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
	decided := layer.Stats().LastCutNs                 // the timer decides here...
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
	r, skipped, err := pc.CutUnlessCutSince(ctTimeout(t), layer.Stats().LastCutNs)
	if err != nil || skipped || r.Noop {
		t.Fatalf("periodic cut decided after the last cut: skipped=%v noop=%v err=%v; want a cut", skipped, r.Noop, err)
	}
	if f.stats().cas != casBefore+1 {
		t.Fatalf("CAS calls %d -> %d; want one", casBefore, f.stats().cas)
	}
}
