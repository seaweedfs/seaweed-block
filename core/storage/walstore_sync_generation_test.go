package storage

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newGenerationStore(t *testing.T) *WALStore {
	t.Helper()
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	return s
}
func waitGenerationPending(t *testing.T, s *WALStore, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.committer.mu.Lock()
		got := len(s.committer.pending)
		s.committer.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("did not queue %d followers", n)
}
func generationResult(t *testing.T, ch <-chan uint64) uint64 {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(3 * time.Second):
		t.Fatal("Sync did not join")
		return 0
	}
}

func TestWALGenerationSyncFollowersAndNewWrite(t *testing.T) {
	for _, newWrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "same mutation", true: "write after batch cut"}[newWrite], func(t *testing.T) {
			s := newGenerationStore(t)
			if _, err := s.Write(0, makeBlock(4096, 0x31)); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			// Release before store close even if an assertion fails.
			defer close(release)
			realSync := s.syncWALFile
			var attempts atomic.Int32
			s.syncWALFile = func() error {
				s.committer.mu.Lock()
				s.committer.mu.Unlock()
				if attempts.Add(1) == 1 {
					close(entered)
					<-release
				}
				return realSync()
			}
			results := make(chan uint64, 9)
			firstResult := make(chan uint64, 1)
			errs := make(chan error, 9)
			start := func(out chan<- uint64) { go func() { n, err := s.Sync(); errs <- err; out <- n }() }
			start(firstResult)
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("no physical Sync")
			}
			if newWrite {
				if _, err := s.Write(1, makeBlock(4096, 0x42)); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 8; i++ {
				start(results)
			}
			waitGenerationPending(t, s, 8)
			release <- struct{}{}
			first := generationResult(t, firstResult)
			if first != 1 {
				t.Fatalf("first batch frontier=%d want 1", first)
			}
			for i := 0; i < 8; i++ {
				n := generationResult(t, results)
				want := uint64(1)
				if newWrite {
					want = 2
				}
				if n != want {
					t.Fatalf("follower frontier=%d want %d", n, want)
				}
			}
			for i := 0; i < 9; i++ {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			want := uint64(1)
			if newWrite {
				want = 2
			}
			if n := s.WALSyncCount(); n != want {
				t.Fatalf("physical syncs=%d want %d", n, want)
			}
		})
	}
}

func TestWALGenerationSyncFailureAndShutdown(t *testing.T) {
	s := newGenerationStore(t)
	if _, err := s.Write(0, makeBlock(4096, 0x61)); err != nil {
		t.Fatal(err)
	}
	realSync := s.syncWALFile
	var attempts atomic.Int32
	fault := errors.New("injected fsync error")
	s.syncWALFile = func() error {
		if attempts.Add(1) == 1 {
			return fault
		}
		return realSync()
	}
	if _, err := s.Sync(); !errors.Is(err, fault) {
		t.Fatalf("first sync err=%v", err)
	}
	if r, _, _ := s.Boundaries(); r != 0 {
		t.Fatalf("failed sync published R=%d", r)
	}
	// A failed fsync is sticky (ErrFsyncFailed): the retry is refused
	// without another physical fsync, and nothing is published.
	if n, err := s.Sync(); !errors.Is(err, ErrFsyncFailed) || n != 0 {
		t.Fatalf("retry after a failed fsync frontier=%d err=%v; want ErrFsyncFailed", n, err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("a retry after a failed fsync fsynced again: attempts=%d", attempts.Load())
	}
	if r, _, _ := s.Boundaries(); r != 0 {
		t.Fatalf("refused retry published R=%d", r)
	}
	_ = s.Close()

	s = newGenerationStore(t)
	if _, err := s.Write(0, makeBlock(4096, 0x61)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Sync(); err != nil || n != 1 {
		t.Fatalf("sync frontier=%d err=%v", n, err)
	}
	s.committer.Stop()
	if _, err := s.Sync(); !errors.Is(err, ErrGroupCommitShutdown) {
		t.Fatalf("cached request after shutdown err=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err == nil {
		t.Fatal("cached request after Close accepted")
	}
}

func TestWALGenerationBypassesUntrackedMutationPaths(t *testing.T) {
	for _, kind := range []string{"old LSN apply", "direct extent", "direct frontier", "chunk layer"} {
		t.Run(kind, func(t *testing.T) {
			s := newGenerationStore(t)
			if _, err := s.Write(0, makeBlock(4096, 0x81)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			before := s.WALSyncCount()
			switch kind {
			case "old LSN apply":
				if err := s.ApplyEntry(0, makeBlock(4096, 0x82), 1); err != nil {
					t.Fatal(err)
				}
			case "direct extent":
				if err := s.WriteExtentDirect(1, makeBlock(4096, 0x83)); err != nil {
					t.Fatal(err)
				}
			case "direct frontier":
				s.AdvanceFrontier(5)
			case "chunk layer":
				mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
			}
			for i := 0; i < 2; i++ {
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if n := s.WALSyncCount() - before; n != 2 {
				t.Fatalf("bypass physical syncs=%d want 2", n)
			}
		})
	}
}

func TestWALGenerationBatchPublication(t *testing.T) {
	for _, multi := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal records", true: "multiblock record"}[multi], func(t *testing.T) {
			s := newGenerationStore(t)
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			} // force the initial file to disk
			s.SetMultiBlockRecords(multi)
			for i := 0; i < 2; i++ {
				before := s.WALSyncCount()
				if _, err := s.WriteBatch(0, [][]byte{makeBlock(4096, byte(i+1)), makeBlock(4096, byte(i+2))}); err != nil {
					t.Fatal(err)
				}
				for j := 0; j < 2; j++ {
					if _, err := s.Sync(); err != nil {
						t.Fatal(err)
					}
				}
				if n := s.WALSyncCount() - before; n != 1 {
					t.Fatalf("batch %d physical syncs=%d want 1", i, n)
				}
			}
		})
	}
}

// An actual partial pwrite leaves a valid first record and a torn second record,
// but appendBatch returns before publishing its planned head. Same-open Recover
// must not let the prefix reuse the pre-failure durable generation.
func TestWALGenerationPartialAppendRecoverySync(t *testing.T) {
	for _, recoverFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed append without recovery", true: "same-open recovery"}[recoverFirst], func(t *testing.T) {
			s := newGenerationStore(t)
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			beforeHead := s.wal.logicalHeadValue()
			want := makeBlock(4096, 0xa7)
			pwriteCalls := 0
			s.wal.batchWriteAt = func(data []byte, off int64) (int, error) {
				pwriteCalls++
				limit := walEntryHeaderSize + 4096 + walEntryHeaderSize + 17
				if len(data) <= limit {
					return 0, errors.New("fixture needs two whole records")
				}
				n, err := s.fd.WriteAt(data[:limit], off)
				if err != nil {
					return n, err
				}
				return n, io.ErrShortWrite
			}
			_, err := s.WriteBatch(0, [][]byte{want, makeBlock(4096, 0xb8)})
			s.wal.batchWriteAt = nil
			if !errors.Is(err, io.ErrShortWrite) || pwriteCalls != 1 {
				t.Fatalf("partial batch err=%v pwrite calls=%d", err, pwriteCalls)
			}
			if got := s.wal.logicalHeadValue(); got != beforeHead {
				t.Fatalf("failed batch published head=%d want %d", got, beforeHead)
			}
			if recoverFirst {
				if n, err := s.Recover(); err != nil || n != 1 {
					t.Fatalf("same-open Recover=%d err=%v want1,nil", n, err)
				}
				if got, err := s.Read(0); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("valid prefix not recovered: err=%v", err)
				}
			}
			realSync := s.syncWALFile
			fault := errors.New("next physical sync failed")
			attempts := 0
			s.syncWALFile = func() error { attempts++; return fault }
			_, err = s.Sync()
			s.syncWALFile = realSync
			if !errors.Is(err, fault) || attempts != 1 {
				t.Fatalf("next Sync skipped required physical I/O/error: err=%v attempts=%d", err, attempts)
			}
			// The failed fsync is sticky (ErrFsyncFailed): no retry ack and
			// no further physical fsync on this open store.
			before := s.WALSyncCount()
			if _, err := s.Sync(); !errors.Is(err, ErrFsyncFailed) {
				t.Fatalf("retry after a failed fsync: %v; want ErrFsyncFailed", err)
			}
			if n := s.WALSyncCount() - before; n != 0 {
				t.Fatalf("a refused retry fsynced %d times", n)
			}
		})
	}
}

// Even unchanged recovery must invalidate the prior open-instance coverage;
// its next successful sync may then be reused normally, not permanently bypassed.
func TestWALGenerationSameOpenRecoverInvalidatesCoverage(t *testing.T) {
	s := newGenerationStore(t)
	if _, err := s.Write(0, makeBlock(4096, 0x92)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	realSync := s.syncWALFile
	attempts := 0
	fault := errors.New("post-recovery fsync error")
	s.syncWALFile = func() error { attempts++; return fault }
	_, err := s.Sync()
	s.syncWALFile = realSync
	if !errors.Is(err, fault) || attempts != 1 {
		t.Fatalf("Recover reused old sync coverage: err=%v attempts=%d", err, attempts)
	}
	// The failed fsync is sticky (ErrFsyncFailed); dedup after a Recover is
	// checked on a store whose syncs succeed.
	if _, err := s.Sync(); !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("retry after a failed fsync: %v; want ErrFsyncFailed", err)
	}
	s2 := newGenerationStore(t)
	if _, err := s2.Write(0, makeBlock(4096, 0x92)); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	before := s2.WALSyncCount()
	for i := 0; i < 3; i++ {
		if _, err := s2.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if n := s2.WALSyncCount() - before; n != 1 {
		t.Fatalf("Recover permanently disabled dedup: syncs=%d want1", n)
	}
}
