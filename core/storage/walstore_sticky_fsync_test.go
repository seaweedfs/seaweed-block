package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

// A failed fsync is sticky for the open store (the "fsyncgate" pattern: on
// Linux the kernel may already have dropped the dirty pages, so a later
// successful fsync proves nothing about them). After the first failure the
// store acknowledges no further Sync, accepts no further write and advances
// no checkpoint, with a typed SubstrateIO failure; only a reopen clears it.

var errInjectedFsync = errors.New("injected fsync failure")

func assertStickyFsync(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("err %v; want the sticky ErrFsyncFailed", err)
	}
	var rf *RecoveryFailure
	if !errors.As(err, &rf) || rf.Kind != StorageRecoveryFailureSubstrateIO {
		t.Fatalf("err %v; want a SubstrateIO RecoveryFailure (io_error)", err)
	}
}

func stickyStore(t *testing.T) (*WALStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, 4096, 64*1024)
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Write(0, makeBlock(4096, 0x10)); err != nil { // acked before the fault
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	return s, path
}

// assertRefusesAfterFault: no Sync ack (and no physical WAL fsync), no write,
// no checkpoint; a reopen clears it and keeps the data acked before the fault.
func assertRefusesAfterFault(t *testing.T, s *WALStore, path string) {
	t.Helper()
	before := s.WALSyncCount()
	_, err := s.Sync()
	assertStickyFsync(t, err)
	if n := s.WALSyncCount() - before; n != 0 {
		t.Fatalf("a Sync after the fault fsynced again (%d); it must refuse without retrying", n)
	}
	if _, err := s.Write(2, makeBlock(4096, 0x22)); err == nil {
		t.Fatal("a write after an fsync failure was accepted")
	} else {
		assertStickyFsync(t, err)
	}
	if _, err := s.WriteBatch(3, [][]byte{makeBlock(4096, 0x33)}); err == nil {
		t.Fatal("a batch write after an fsync failure was accepted")
	}
	ckpt := s.CheckpointLSN()
	_ = s.flusher.flushOnce()
	if got := s.CheckpointLSN(); got != ckpt {
		t.Fatalf("the flusher advanced the checkpoint %d -> %d after an fsync failure", ckpt, got)
	}
	_ = s.Close()
	r, err := openLegacyWALStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatalf("reopen after an fsync failure: %v", err)
	}
	assertRecoveryHardeningBlock(t, r, 0, makeBlock(4096, 0x10))
	if _, err := r.Write(4, makeBlock(4096, 0x44)); err != nil {
		t.Fatalf("write after a reopen: %v", err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatalf("Sync after a reopen: %v", err)
	}
}

func TestWALStoreWALFsyncFailureIsSticky(t *testing.T) {
	s, path := stickyStore(t)
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	real := s.syncWALFile
	s.syncWALFile = func() error { return errInjectedFsync }
	if _, err := s.Sync(); !errors.Is(err, errInjectedFsync) {
		t.Fatalf("Sync with a failing fsync: %v", err)
	}
	s.syncWALFile = real // the disk "recovers": the next fsync would succeed
	assertRefusesAfterFault(t, s, path)
}

func TestWALStoreExtentFsyncFailureIsSticky(t *testing.T) {
	s, path := stickyStore(t)
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	real := s.syncExtentFile
	s.syncExtentFile = func() error { return errInjectedFsync }
	if err := s.flusher.flushOnce(); !errors.Is(err, errInjectedFsync) {
		t.Fatalf("flush with a failing extent fsync: %v", err)
	}
	s.syncExtentFile = real
	assertRefusesAfterFault(t, s, path)
}

func TestWALStoreCheckpointFsyncFailureIsSticky(t *testing.T) {
	s, path := stickyStore(t)
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	real := s.syncSuperblockMetadata
	s.syncSuperblockMetadata = func() error { return errInjectedFsync }
	if err := s.flusher.flushOnce(); !errors.Is(err, errInjectedFsync) {
		t.Fatalf("flush with a failing checkpoint fsync: %v", err)
	}
	s.syncSuperblockMetadata = real
	assertRefusesAfterFault(t, s, path)
}
