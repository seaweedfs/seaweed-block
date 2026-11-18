package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWALStoreV3CheckpointReclaimsAcrossRingLaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	s, err := createMappedWALStoreV3ForTest(path, 16, 4096, 64*1024, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	for i := 1; i <= 40; i++ {
		if _, err := s.Write(0, blk(byte(i))); err != nil {
			t.Fatalf("cycle %d after prior synced checkpoints: %v", i, err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
	}
	if floor, valid := s.v3.pub.reuseFloor(); !valid || floor <= 64*1024 {
		t.Fatalf("both-slot reuse floor did not cross a ring lap: floor=%d valid=%v", floor, valid)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Read(0); err != nil || !bytes.Equal(got, blk(40)) {
		t.Fatalf("latest acknowledged bytes after ring reuse/reopen: %v", err)
	}
}
