package storage

import (
	"errors"

	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newGenerationStore(t *testing.T) *WALStore {
	t.Helper()
	// This control holds the standalone WAL fsync seam. Mode A retains
	// that phase; mode C combines WAL durability with descriptor publication.
	s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "store.bin"), 32, 4096, WALStoreOptions{WMode: WModeStrict})
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
			var attempts, completed atomic.Int32
			s.syncWALFile = func() error {
				s.committer.mu.Lock()
				s.committer.mu.Unlock()
				if attempts.Add(1) == 1 {
					close(entered)
					<-release
				}
				err := realSync()
				if err == nil {
					completed.Add(1)
				}
				return err
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
			// Sync returns the global stable frontier at publication. Its
			// caller may resume after the next batch has already fsynced and
			// published 2. Accept that widening only with independent physical
			// completion evidence; never acknowledge an unflushed new write.
			if first != 1 && !(newWrite && first == 2 && completed.Load() >= 2) {
				t.Fatalf("first caller frontier=%d, newWrite=%v completed fsyncs=%d", first, newWrite, completed.Load())
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
