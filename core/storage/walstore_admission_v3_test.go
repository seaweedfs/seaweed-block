package storage

import (
	"path/filepath"
	"testing"
)

func TestWALStoreV3AdmissionUsesNativeOccupancy(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "volume"), 256, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if s.v3 == nil {
		t.Fatal("public selector did not create v3")
	}
	if got := s.admission.walUsed(); got != 0 {
		t.Fatalf("initial occupancy=%g", got)
	}
	blocks := make([][]byte, 32)
	for i := range blocks {
		blocks[i] = blk(byte(i))
	}
	if _, err := s.WriteBatch(0, blocks); err != nil {
		t.Fatal(err)
	}
	want := float64(32*4096+70) / float64(64*1024*1024)
	if got := s.admission.walUsed(); got != want {
		t.Fatalf("native occupancy=%g want%g; legacy=%g", got, want, s.wal.usedFraction())
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.admission.walUsed(); got != 0 {
		t.Fatalf("both-slot-released occupancy=%g", got)
	}
}
