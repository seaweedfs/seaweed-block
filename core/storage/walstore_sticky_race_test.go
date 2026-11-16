package storage

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Held-caller controls for the sticky I/O fault: callers that passed the
// entry check before the fault was latched must still be refused.

// A Sync queued behind an fsync that then fails must not be acknowledged by
// a second fsync of the same batch run.
func TestWALStoreQueuedSyncAfterAFailedFsyncIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, 4096, 64*1024)
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Write(0, makeBlock(4096, 0x10)); err != nil {
		t.Fatal(err)
	}

	real := s.syncWALFile
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fault := errors.New("injected fsync failure")
	s.syncWALFile = func() error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return fault
		}
		return real() // a later fsync would "succeed"
	}

	errA := make(chan error, 1)
	go func() { _, err := s.Sync(); errA <- err }()
	<-entered // A's fsync is in flight
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	errB := make(chan error, 1)
	go func() { _, err := s.Sync(); errB <- err }()
	waitQueuedSync(t, s, 1) // B passed the entry check and waits in the committer
	close(release)

	if err := <-errA; !errors.Is(err, fault) {
		t.Fatalf("Sync A: %v; want the injected fault", err)
	}
	if err := <-errB; !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("queued Sync B after the failed fsync: %v; want ErrFsyncFailed", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("physical fsyncs=%d; want 1 (no fsync after the fault)", n)
	}
	if r, _, _ := s.Boundaries(); r != 0 {
		t.Fatalf("published R=%d after the fault", r)
	}
}

func waitQueuedSync(t *testing.T, s *WALStore, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.committer.mu.Lock()
		q := len(s.committer.pending)
		s.committer.mu.Unlock()
		if q >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no queued Sync after 5s")
}

// Writers waiting for the store lock while an append fails must not reuse
// its LSNs or offsets: Write, WriteBatch and ApplyEntry are all refused.
func TestWALStoreWritersQueuedBehindAFailedAppendAreRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, 4096, 64*1024)
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })

	entered := make(chan struct{})
	release := make(chan struct{})
	eio := errors.New("injected EIO after the batch bytes were written")
	var batchCalls atomic.Int32
	s.wal.batchWriteAt = func(b []byte, off int64) (int, error) {
		if batchCalls.Add(1) == 1 {
			close(entered)
			<-release
		}
		n, _ := s.fd.WriteAt(b, off)
		return n, eio
	}
	nextBefore := s.NextLSN()
	headBefore := s.wal.logicalHeadValue()

	errA := make(chan error, 1)
	go func() {
		_, err := s.WriteBatch(10, [][]byte{makeBlock(4096, 0x5B), makeBlock(4096, 0x5A)})
		errA <- err
	}()
	<-entered // A holds the store lock inside its append
	var wg sync.WaitGroup
	errs := make([]error, 3)
	wg.Add(3)
	go func() { defer wg.Done(); _, errs[0] = s.Write(11, makeBlock(4096, 0xAC)) }()
	go func() { defer wg.Done(); _, errs[1] = s.WriteBatch(20, [][]byte{makeBlock(4096, 0xAD)}) }()
	go func() { defer wg.Done(); errs[2] = s.ApplyEntry(21, makeBlock(4096, 0xAE), nextBefore) }()
	time.Sleep(200 * time.Millisecond) // they pass the entry check and wait for the lock
	close(release)

	if err := <-errA; !errors.Is(err, eio) {
		t.Fatalf("failing batch: %v", err)
	}
	wg.Wait()
	for i, name := range []string{"Write", "WriteBatch", "ApplyEntry"} {
		if !errors.Is(errs[i], ErrAppendFailed) {
			t.Errorf("%s queued behind the failed append: %v; want ErrAppendFailed", name, errs[i])
		}
	}
	if n := batchCalls.Load(); n != 1 {
		t.Errorf("batch pwrites=%d; want 1 (no append after the fault)", n)
	}
	if got := s.NextLSN(); got != nextBefore {
		t.Errorf("NextLSN %d -> %d after refused appends", nextBefore, got)
	}
	if got := s.wal.logicalHeadValue(); got != headBefore {
		t.Errorf("WAL head %d -> %d after refused appends", headBefore, got)
	}
}
