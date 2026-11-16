package storage

import (
	"path/filepath"
	"testing"
)

// claude05's condition (1) for any prefix-cut recovery rule (2026-11-16
// 07:33Z): an intentional LSN gap must not drop synced entries above it. A
// rebuild installs BASE through WriteExtentDirect (no LSN) and applies
// WAL-lane entries with their real LSNs through ApplyEntry; the frontier
// jump (AdvanceFrontier at MarkBaseComplete) is not persisted before them.
// So a replica can hold a synced LSN 101 with a persisted checkpoint of 0 and
// no LSN 1..100 anywhere. Recovery must keep it (a rule that replays only the
// contiguous LSN prefix above the checkpoint keeps nothing here).
func TestWALStoreRecoverKeepsSyncedEntriesAboveAnUnpersistedFrontierJump(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, 24*1024)
	s.DisableAutoFlushForRecoveryTest()
	if err := s.WriteExtentDirect(0, makeBlock(blockSize, 0xBA)); err != nil { // BASE, no LSN
		t.Fatal(err)
	}
	live := makeBlock(blockSize, 0x65)
	if err := s.ApplyEntry(3, live, 101); err != nil { // session-live entry above the BASE hint
		t.Fatal(err)
	}
	if err := s.ApplyEntry(4, makeBlock(blockSize, 0x66), 102); err != nil {
		t.Fatal(err)
	}
	synced, err := s.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if synced < 102 {
		t.Fatalf("Sync frontier %d; want 102 (the applied entries are synced)", synced)
	}
	crashRecoveryHardeningStore(t, s)
	if sb := diskSuperblockForGapTest(t, path); sb.WALCheckpointLSN != 0 {
		t.Fatalf("persisted checkpoint %d; want 0 (the frontier jump is not persisted)", sb.WALCheckpointLSN)
	}
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertRecoveryHardeningBlock(t, r, 3, live)
	assertRecoveryHardeningBlock(t, r, 4, makeBlock(blockSize, 0x66))
}

func diskSuperblockForGapTest(t *testing.T, path string) superblock {
	t.Helper()
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	sb := *r.sb
	r.committer.Stop()
	_ = r.fd.Close()
	return sb
}
