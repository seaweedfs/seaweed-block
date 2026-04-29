package storage

// INV-G7-REBUILD-SUBSTRATE-NO-STALE-EXPOSED — durability-side regression
// guard. WALStore.ResetForRebuild must produce on-disk state that
// survives a clean restart: after Reset → Close → OpenWALStore → Recover,
// every LBA still reads as zero and the dirty map is empty. Without
// physically zeroing the WAL region, recoverWAL's defensive scan can
// re-decode old WAL records and re-populate the dirty map, dragging
// stale state back over the freshly-zeroed extent.

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWALStore_ResetForRebuild_SurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reset.walstore")
	const numBlocks = 16
	const blockSize = 4096

	// Phase 1: open fresh, write some non-zero data, sync, close.
	s, err := CreateWALStore(path, numBlocks, blockSize)
	if err != nil {
		t.Fatalf("CreateWALStore: %v", err)
	}
	for lba := uint32(0); lba < 4; lba++ {
		data := make([]byte, blockSize)
		for i := range data {
			data[i] = byte(lba) + 0x80
		}
		if _, err := s.Write(lba, data); err != nil {
			t.Fatalf("Write lba=%d: %v", lba, err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Phase 2: ResetForRebuild — extent + WAL region zeroed, in-memory
	// metadata cleared, superblock persisted.
	if err := s.ResetForRebuild(); err != nil {
		t.Fatalf("ResetForRebuild: %v", err)
	}

	// In-memory invariants immediately after reset.
	for lba := uint32(0); lba < numBlocks; lba++ {
		got, err := s.Read(lba)
		if err != nil {
			t.Fatalf("Read lba=%d after reset: %v", lba, err)
		}
		zero := make([]byte, blockSize)
		if !bytes.Equal(got, zero) {
			t.Fatalf("INV-G7-REBUILD-SUBSTRATE-NO-STALE-EXPOSED in-memory: lba=%d non-zero after reset (got[0]=%02x)", lba, got[0])
		}
	}
	if r, _, h := s.Boundaries(); r != 0 || h != 0 {
		t.Fatalf("Boundaries after reset: R=%d H=%d, want 0/0", r, h)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 3: reopen + Recover. The on-disk state must equate to a
	// freshly-zeroed substrate, NOT the pre-reset state.
	s2, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("OpenWALStore: %v", err)
	}
	defer s2.Close()
	frontier, err := s2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if frontier != 0 {
		t.Fatalf("Recover frontier after reset: got %d, want 0", frontier)
	}

	for lba := uint32(0); lba < numBlocks; lba++ {
		got, err := s2.Read(lba)
		if err != nil {
			t.Fatalf("Read lba=%d after restart: %v", lba, err)
		}
		zero := make([]byte, blockSize)
		if !bytes.Equal(got, zero) {
			t.Fatalf("INV-G7-REBUILD-SUBSTRATE-NO-STALE-EXPOSED across restart: lba=%d non-zero after Reset+Restart+Recover (got[0]=%02x) — recoverWAL likely re-decoded surviving WAL bytes", lba, got[0])
		}
	}
	if r, _, h := s2.Boundaries(); r != 0 || h != 0 {
		t.Fatalf("Boundaries after restart: R=%d H=%d, want 0/0", r, h)
	}
}
