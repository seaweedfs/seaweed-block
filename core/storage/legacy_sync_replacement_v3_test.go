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
func TestNativeReplacementSyncCallerReturnOrdering(t *testing.T) {
	for _, laterPublishesFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("later_publishes_first=%v", laterPublishesFirst), func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
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
				frontier  uint64
				completed int32
				err       error
			}
			leader, followers := make(chan result, 1), make(chan result, 1)
			start := func(out chan<- result) {
				workers.Add(1)
				go func() {
					defer workers.Done()
					n, err := s.Sync()
					out <- result{frontier: n, completed: completed.Load(), err: err}
				}()
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
			closeOnce(releaseFirst)
			await(firstCacheReturned, "first caller after physical completion")
			// This write occurs AFTER the first real fsync completed, so only
			// the later physical fsync can make it durable.
			if _, err := s.Write(1, makeBlock(4096, 0x42)); err != nil {
				t.Fatal(err)
			}
			start(followers)
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
				for i := 0; i < 1; i++ {
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
			// Strict v3 may wait for the shared-file sync before publishing
			// its descriptor. It need not return before the second caller.
			if !laterPublishesFirst {
				closeOnce(releaseSecond)
				if r := receive(followers); r.frontier != 2 || r.completed < 2 {
					t.Fatal("follower frontier without completed physical sync", r)
				}
			}
			first := receive(leader)
			if first.frontier < 1 || first.frontier > uint64(first.completed) {
				t.Fatalf("first frontier exceeds completed physical syncs: %+v", first)
			}
			if completed.Load() != 2 || s.WALSyncCount() != 2 {
				t.Fatalf("physical counts completed=%d attempts=%d", completed.Load(), s.WALSyncCount())
			}
		})
	}
}
