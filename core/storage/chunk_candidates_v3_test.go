package storage

import "testing"

func TestChunkLayerV3CapturedUncheckpointedWALIsCleanUntilRewritten(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	f := newFakeChunkRemote()
	l := mustAttach(t, s, f, ctConfig(1))
	if _, err := s.Write(1, blk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if s.dm.len() != 1 {
		t.Fatal("control requires the captured WAL entry still retained in dirty map")
	}
	if d := l.Stats().DirtyChunks; d != 0 {
		t.Fatalf("already captured WAL counts as dirty: %d", d)
	}
	if _, err := s.Write(1, blk(2)); err != nil {
		t.Fatal(err)
	}
	if d := l.Stats().DirtyChunks; d != 1 {
		t.Fatalf("post-HEAD write missing: %d", d)
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := f.imageOf(t, r.Head)[ctBlockSize]; got != 2 {
		t.Fatalf("post-HEAD write lost: %d", got)
	}
}
