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
	if n, err := s.Sync(); err != nil || n != 1 {
		t.Fatalf("retry frontier=%d err=%v", n, err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("error was cached: attempts=%d", attempts.Load())
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatal("durable duplicate resynced")
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
