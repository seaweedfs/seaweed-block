package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

// B5 (independent review): a batch append whose pwrite lands every record
// and then reports an error left nextLSN and the head unchanged, so the next
// write reused the batch's first LSN and offset while the batch's later
// record (a higher LSN) stayed on disk past it; after that write's Sync was
// acknowledged and the process crashed, recovery served the failed batch's
// stale record. A failed append is now sticky for the open store: no further
// append (so no LSN reuse) until a reopen. Recovery of the reopen may replay
// the failed, never-acknowledged records; an acknowledged write is never
// replaced by one of them.
func TestWALStoreFailedBatchAppendDoesNotRevertALaterAckedWrite(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createLegacyWALStoreWithWALSizeForTest(t, path, 64, blockSize, 64*1024)
	s.DisableAutoFlushForRecoveryTest()
	crashed := false
	t.Cleanup(func() {
		if !crashed {
			_ = s.Close()
		}
	})

	eio := errors.New("injected EIO after the batch bytes were written")
	s.wal.batchWriteAt = func(b []byte, off int64) (int, error) {
		n, _ := s.fd.WriteAt(b, off) // the bytes reach the file...
		return n, eio                // ...but the call reports failure
	}
	stale := makeBlock(blockSize, 0x5A)
	if _, err := s.WriteBatch(10, [][]byte{makeBlock(blockSize, 0x5B), stale}); !errors.Is(err, eio) {
		t.Fatalf("WriteBatch with a failing pwrite: %v", err)
	}
	s.wal.batchWriteAt = nil

	// The failed append is sticky: no write reuses its LSNs, nothing is acked.
	acked := makeBlock(blockSize, 0xAC)
	if _, err := s.Write(11, acked); !errors.Is(err, ErrAppendFailed) {
		t.Fatalf("write after a failed append: %v; want the sticky ErrAppendFailed", err)
	} else {
		var rf *RecoveryFailure
		if !errors.As(err, &rf) || rf.Kind != StorageRecoveryFailureSubstrateIO {
			t.Fatalf("write after a failed append: %v; want a SubstrateIO RecoveryFailure (io_error)", err)
		}
	}
	if _, err := s.Sync(); !errors.Is(err, ErrAppendFailed) {
		t.Fatalf("Sync after a failed append: %v; want ErrAppendFailed", err)
	}
	crashRecoveryHardeningStore(t, s)
	crashed = true

	// Reopen: recovery may replay the failed batch (never acked). A write
	// acknowledged now must survive the next crash.
	r := reopenForB5(t, path)
	lsn, err := r.Write(11, acked)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	t.Logf("acked write of LBA 11 after the reopen got LSN %d", lsn)
	crashRecoveryHardeningStore(t, r)

	q := reopenForB5(t, path)
	t.Cleanup(func() { _ = q.Close() })
	got, err := q.Read(11)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != acked[0] {
		t.Fatalf("LBA 11 after recovery = %02x; want the acknowledged %02x (the failed batch's stale record is %02x)", got[0], acked[0], stale[0])
	}
}

func reopenForB5(t *testing.T, path string) *WALStore {
	t.Helper()
	r, err := openLegacyWALStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	if _, err := r.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	return r
}
