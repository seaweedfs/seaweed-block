package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

// B5 (independent review): a batch append whose pwrite lands every record
// and then reports an error leaves nextLSN and the head unchanged. The next
// write reuses the batch's first LSN and offset, overwriting only the first
// stale record; the batch's later record (a higher LSN) stays on disk past
// it. After an acknowledged Sync and a crash, recovery must serve the
// acknowledged write, not the failed batch's stale record.
func TestWALStoreFailedBatchAppendDoesNotRevertALaterAckedWrite(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, 64*1024)
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

	acked := makeBlock(blockSize, 0xAC)
	lsn, err := s.Write(11, acked)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	t.Logf("acked write of LBA 11 got LSN %d", lsn)

	crashRecoveryHardeningStore(t, s)
	crashed = true
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got, err := r.Read(11)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != acked[0] {
		t.Fatalf("LBA 11 after recovery = %02x; want the acknowledged %02x (the failed batch's stale record is %02x)", got[0], acked[0], stale[0])
	}
}
