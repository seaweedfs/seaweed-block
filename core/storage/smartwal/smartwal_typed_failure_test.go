package smartwal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// T4d-1: smartwal-side typed RecoveryFailure pin. ScanLBAs must
// return *storage.RecoveryFailure on recycle, kind WALRecycled,
// preserving errors.Is(_, ErrWALRecycled) for migration-window
// callers.

func TestStorageRecoveryFailureKind_Smartwal_RecycleWrappedAsKind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smartwal-recycled.sw")
	s, err := CreateStore(path, 16, 4096)
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	defer s.Close()

	// Write enough entries to roll the ring past capacity, so
	// ScanLBAs(1, ...) trips fromLSN < oldestPreserved.
	capacity := s.RingCapacity()
	totalWrites := capacity + 10
	for i := uint64(0); i < totalWrites; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		if _, werr := s.Write(uint32(i%16), data); werr != nil {
			t.Fatal(werr)
		}
	}

	scanErr := s.ScanLBAs(1, func(_ storage.RecoveryEntry) error { return nil })
	if scanErr == nil {
		t.Fatal("expected ErrWALRecycled when fromLSN < oldestPreserved")
	}
	if !errors.Is(scanErr, storage.ErrWALRecycled) {
		t.Errorf("smartwal ScanLBAs error must satisfy errors.Is(_, ErrWALRecycled); got: %v", scanErr)
	}
	var rf *storage.RecoveryFailure
	if !errors.As(scanErr, &rf) {
		t.Fatalf("smartwal ScanLBAs error must be *storage.RecoveryFailure via errors.As; got: %v", scanErr)
	}
	if rf.Kind != storage.StorageRecoveryFailureWALRecycled {
		t.Errorf("Kind = %v, want StorageRecoveryFailureWALRecycled", rf.Kind)
	}
}

func TestStorageRecoveryFailureKind_Smartwal_CRCMismatchWrappedAsWALIntegrity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smartwal-integrity.sw")
	s, err := CreateStore(path, 16, 4096)
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	for i := uint32(0); i < 3; i++ {
		data := make([]byte, 4096)
		data[0] = byte(0xA0 + i)
		if _, werr := s.Write(i, data); werr != nil {
			t.Fatal(werr)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	layout, rec, err := LatestRecord(path)
	if err != nil {
		t.Fatalf("LatestRecord: %v", err)
	}
	extentOffset := layout.ExtentStart + int64(rec.LBA)*int64(layout.BlockSize)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteAt([]byte{0x5A}, extentOffset); err != nil {
		_ = f.Close()
		t.Fatalf("corrupt extent: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close corrupted file: %v", err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer reopened.Close()
	_, recoverErr := reopened.Recover()
	if recoverErr == nil {
		t.Fatal("expected WAL integrity failure after extent/WAL CRC mismatch")
	}
	if !errors.Is(recoverErr, storage.ErrWALIntegrityFault) {
		t.Fatalf("Recover error must satisfy errors.Is(_, ErrWALIntegrityFault); got: %v", recoverErr)
	}
	var rf *storage.RecoveryFailure
	if !errors.As(recoverErr, &rf) {
		t.Fatalf("Recover error must be *storage.RecoveryFailure via errors.As; got: %v", recoverErr)
	}
	if rf.Kind != storage.StorageRecoveryFailureWALIntegrity {
		t.Fatalf("Kind = %v, want StorageRecoveryFailureWALIntegrity", rf.Kind)
	}
}

// TestSmartwal_AppliedLSNs_ReportsRingEntries pins smartwal's
// AppliedLSNs (ring-based reduction).
func TestSmartwal_AppliedLSNs_ReportsRingEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smartwal-applied.sw")
	s, err := CreateStore(path, 16, 4096)
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	defer s.Close()

	// Write 3 distinct LBAs.
	for i := uint32(0); i < 3; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		if _, werr := s.Write(i, data); werr != nil {
			t.Fatal(werr)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	got, err := s.AppliedLSNs()
	if err != nil {
		t.Fatalf("AppliedLSNs: %v", err)
	}
	if len(got) < 3 {
		t.Errorf("AppliedLSNs returned %d entries, want >= 3", len(got))
	}
	for i := uint32(0); i < 3; i++ {
		if _, ok := got[i]; !ok {
			t.Errorf("LBA %d missing from AppliedLSNs", i)
		}
	}
}
