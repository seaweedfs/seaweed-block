package storage

import (
	"path/filepath"
	"testing"
)

// Recovery is last-writer-wins by LSN. Two valid versions of one LBA
// survive in the WAL; the persisted
// head/tail predate the wrap, so the range scanned first holds the newer
// version (wrapped to offset 0) and the range scanned later the older one.
// Recovery must still serve the newer bytes.
func TestWALStoreRecoverLastWriterWinsAcrossScanOrder(t *testing.T) {
	const (
		blockSize = 4096
		walSize   = 24 * 1024
	)
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, walSize)
	s.DisableAutoFlushForRecoveryTest()
	for lba := uint32(0); lba < 3; lba++ {
		if _, err := s.Write(lba, makeBlock(blockSize, byte(0x10+lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil { // checkpoint 3: head/tail persisted here
		t.Fatal(err)
	}
	older := makeBlock(blockSize, 0xA4)
	if _, err := s.Write(20, older); err != nil { // LSN 4, at the high end
		t.Fatal(err)
	}
	newer := makeBlock(blockSize, 0xB5)
	s.enableMultiBlockRecordsForTest(true)
	if _, err := s.WriteBatch(20, [][]byte{newer, makeBlock(blockSize, 0xB6)}); err != nil { // LSN 5-6, wraps to 0
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if e := recoverySnapshotEntriesByLBA(s.dm.snapshot())[20]; e.WALOffset != 0 || e.LSN != 5 {
		t.Fatalf("newest LBA 20 entry at offset %d LSN %d; want the wrapped batch at 0, LSN 5", e.WALOffset, e.LSN)
	}
	crashRecoveryHardeningStore(t, s)
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := r.Read(20)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != newer[0] {
		t.Fatalf("LBA 20 after recovery = %02x; want the newer version %02x (LSN 5), not the older %02x (LSN 4)", got[0], newer[0], older[0])
	}
}
