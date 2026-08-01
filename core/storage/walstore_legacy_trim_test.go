package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWALStoreRecoverReplaysLegacyRangeTrim(t *testing.T) {
	const (
		blockSize  = 4096
		trimBlocks = 3
	)
	path := filepath.Join(t.TempDir(), "store.bin")

	func() {
		s, err := CreateWALStore(path, 16, blockSize)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		for lba := uint32(3); lba < 3+trimBlocks; lba++ {
			if err := s.WriteExtentDirect(lba, makeBlock(blockSize, 0xA5)); err != nil {
				t.Fatal(err)
			}
		}
		if stable, err := s.Sync(); err != nil || stable != 0 {
			t.Fatalf("base Sync stable=%d err=%v want 0,nil", stable, err)
		}
		trim := &walEntry{
			LSN:    1,
			Type:   walEntryTrim,
			LBA:    3,
			Length: trimBlocks * blockSize,
		}
		if _, err := s.wal.append(trim); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.nextLSN = 2
		s.walHead = 1
		s.walTail = 1
		s.mu.Unlock()
		if stable, err := s.Sync(); err != nil || stable != 1 {
			t.Fatalf("Sync stable=%d err=%v want 1,nil", stable, err)
		}
		crashRecoveryHardeningStore(t, s)
	}()

	reader, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	for lba := uint32(3); lba < 3+trimBlocks; lba++ {
		got, err := reader.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, blockSize)) {
			t.Fatalf("read-only trim LBA %d did not return zeros", lba)
		}
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	if recovered, err := s.Recover(); err != nil || recovered != 1 {
		t.Fatalf("Recover=%d err=%v want 1,nil", recovered, err)
	}

	entries := recoverySnapshotEntriesByLBA(s.dm.snapshot())
	if len(entries) != trimBlocks {
		t.Fatalf("trim dirty entries=%d want %d", len(entries), trimBlocks)
	}
	wantOffset := entries[3].WALOffset
	for index, lba := range []uint64{3, 4, 5} {
		entry := entries[lba]
		if entry.WALOffset != wantOffset ||
			entry.DataOffset != uint32(index*blockSize) {
			t.Fatalf("trim LBA %d identity=(%d,%d) want=(%d,%d)",
				lba, entry.WALOffset, entry.DataOffset,
				wantOffset, index*blockSize)
		}
		got, err := s.Read(uint32(lba))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, blockSize)) {
			t.Fatalf("dirty trim LBA %d did not return zeros", lba)
		}
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.dm.len(); got != 0 {
		t.Fatalf("trim dirty entries after flush=%d want 0", got)
	}
	if got := s.CheckpointLSN(); got != 1 {
		t.Fatalf("trim checkpoint=%d want 1", got)
	}
	for lba := uint32(3); lba < 3+trimBlocks; lba++ {
		got, err := s.readFromExtent(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, blockSize)) {
			t.Fatalf("flushed trim LBA %d did not zero extent", lba)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
