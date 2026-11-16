package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Control the legal scheduler order between a completed physical callback and
// its caller publishing/reading the global stable frontier in Sync.
func TestWALGenerationFirstCallerReturnOrdering(t *testing.T) {
	for _, laterPublishesFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("later_publishes_first=%v", laterPublishesFirst), func(t *testing.T) {
			s := newGenerationStore(t)
			firstEntered, secondEntered, firstCacheReturned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			releaseFirst, releaseSecond, publishFirst := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var workers sync.WaitGroup
			closeOnce := func(ch chan struct{}) {
				select {
				case <-ch:
				default:
					close(ch)
				}
			}
			t.Cleanup(func() {
				closeOnce(releaseFirst)
				closeOnce(releaseSecond)
				closeOnce(publishFirst)
				done := make(chan struct{})
				go func() { workers.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("Sync callers did not settle during cleanup")
				}
			})
			realSync := s.syncWALFile
			var attempts, completed atomic.Int32
			s.syncWALFile = func() error {
				n := attempts.Add(1)
				switch n {
				case 1:
					close(firstEntered)
					<-releaseFirst
				case 2:
					close(secondEntered)
					<-releaseSecond
				default:
					return fmt.Errorf("unexpected physical sync %d", n)
				}
				err := realSync()
				if err == nil {
					completed.Store(n)
				}
				return err
			}
			realCache := s.syncCache
			var calls atomic.Int32
			s.syncCache = func() error {
				n := calls.Add(1)
				err := realCache()
				if n == 1 {
					close(firstCacheReturned)
					<-publishFirst
				}
				return err
			}
			type result struct {
				frontier uint64
				err      error
			}
			leader, followers := make(chan result, 1), make(chan result, 8)
			start := func(out chan<- result) {
				workers.Add(1)
				go func() { defer workers.Done(); n, err := s.Sync(); out <- result{n, err} }()
			}
			await := func(ch <-chan struct{}, label string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(2 * time.Second):
					t.Fatal("missing", label)
				}
			}
			receive := func(ch <-chan result) result {
				t.Helper()
				select {
				case r := <-ch:
					if r.err != nil {
						t.Fatal(r.err)
					}
					return r
				case <-time.After(2 * time.Second):
					t.Fatal("Sync result did not settle")
					return result{}
				}
			}
			if _, err := s.Write(0, makeBlock(4096, 0x31)); err != nil {
				t.Fatal(err)
			}
			start(leader)
			await(firstEntered, "first physical callback")
			if _, err := s.Write(1, makeBlock(4096, 0x42)); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 8; i++ {
				start(followers)
			}
			waitGenerationPending(t, s, 8)
			closeOnce(releaseFirst)
			await(firstCacheReturned, "first caller after physical completion")
			await(secondEntered, "second physical callback")
			if completed.Load() != 1 {
				t.Fatalf("second sync completed while held: %d", completed.Load())
			}
			select {
			case r := <-followers:
				t.Fatalf("follower ack before second fsync: %+v", r)
			default:
			}
			if laterPublishesFirst {
				closeOnce(releaseSecond)
				for i := 0; i < 8; i++ {
					if r := receive(followers); r.frontier != 2 {
						t.Fatal("follower frontier", r)
					}
				}
				if completed.Load() != 2 {
					t.Fatal("later frontier has no second completed physical sync")
				}
				closeOnce(publishFirst)
			} else {
				closeOnce(publishFirst)
			}
			first := receive(leader)
			// Sync returns the current global stable frontier, not its own cut.
			// A later return may include a newer independently completed fsync.
			wantFirst := uint64(1)
			if laterPublishesFirst {
				wantFirst = 2
			}
			if first.frontier != wantFirst {
				t.Fatalf("first batch frontier=%d want %d (completed physical syncs=%d laterPublishesFirst=%v)", first.frontier, wantFirst, completed.Load(), laterPublishesFirst)
			}
			if !laterPublishesFirst {
				if completed.Load() != 1 {
					t.Fatal("held second fsync unexpectedly completed")
				}
				closeOnce(releaseSecond)
				for i := 0; i < 8; i++ {
					if r := receive(followers); r.frontier != 2 {
						t.Fatal("follower frontier", r)
					}
				}
			}
			if completed.Load() != 2 || s.WALSyncCount() != 2 {
				t.Fatalf("physical counts completed=%d attempts=%d", completed.Load(), s.WALSyncCount())
			}
		})
	}
}
