package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #252 P1-1: an expired command never starts NEW payload WAL I/O. The rollover
// (its metadata I/O completes and stays owned) is held until the command has
// expired; the write must then refuse before the payload append: writer head,
// next LSN and dirty map unchanged, the block still reads its old bytes.
func TestWALStoreV3WriteBudgetRefusesNewPayloadAfterRollover(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := map[bool]string{false: "single", true: "batch"}[batch]
		t.Run(name, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			held := make(chan struct{})
			s.v3.testAfterRollover = func() { close(held); <-ctx.Done() }
			head, next, dirty := s.v3.w.head, s.NextLSN(), s.dm.len()
			var err error
			if batch {
				_, err = s.WriteBatchContext(ctx, 0, [][]byte{blk(1), blk(2)})
			} else {
				_, err = s.WriteContext(ctx, 0, blk(1))
			}
			select {
			case <-held:
			default:
				t.Fatal("the write never reached the post-rollover point")
			}
			if !errors.Is(err, ErrWALPressureTimeout) {
				t.Fatalf("expired command after rollover: %v", err)
			}
			if s.v3.w.head != head || s.NextLSN() != next || s.dm.len() != dirty {
				t.Fatalf("expired command started payload WAL I/O: head %d->%d next %d->%d dirty %d->%d",
					head, s.v3.w.head, next, s.NextLSN(), dirty, s.dm.len())
			}
			s.v3.testAfterRollover = nil
			if got, err := s.Read(0); err != nil || bytes.Equal(got, blk(1)) {
				t.Fatalf("refused write visible: %v", err)
			}
			if _, err := s.WriteContext(context.Background(), 0, blk(3)); err != nil {
				t.Fatalf("a fresh write after the refusal: %v", err)
			}
		})
	}
}

// #252 P1-2: short locks keep RWMutex writer preference. Continuous overlapping
// readers must not starve a healthy-capacity write into a busy expiry.
func TestWALStoreV3WriteKeepsWriterPreferenceUnderReaders(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := map[bool]string{false: "single", true: "batch"}[batch]
		t.Run(name, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			var stop atomic.Bool
			var wg sync.WaitGroup
			t.Cleanup(func() {
				stop.Store(true)
				joined := make(chan struct{})
				go func() { wg.Wait(); close(joined) }()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("reader goroutines did not finish")
				}
			})
			// steady overlapping readers of the store lock (each holds it ~2 ms,
			// staggered): some reader is almost always inside, as under heavy
			// read traffic. A blocking Lock registers as a waiting writer and
			// gets in; a TryLock poll would keep failing until the deadline.
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					time.Sleep(time.Duration(i) * 250 * time.Microsecond)
					for !stop.Load() {
						s.mu.RLock()
						time.Sleep(2 * time.Millisecond)
						s.mu.RUnlock()
					}
				}(i)
			}
			time.Sleep(10 * time.Millisecond)
			for i := 0; i < 5; i++ { // within the test WAL (auto-flush is off)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				var err error
				if batch {
					_, err = s.WriteBatchContext(ctx, 0, [][]byte{blk(byte(i)), blk(byte(i + 1))})
				} else {
					_, err = s.WriteContext(ctx, 0, blk(byte(i)))
				}
				cancel()
				if err != nil {
					t.Fatalf("write %d starved by readers: %v", i, err)
				}
			}
		})
	}
}

// Short locks block; an expired command refuses after the wait, without
// mutating (revises the polling-era lock tests to the agreed lock policy).
func TestWALStoreV3ExpiredWriterRefusesAfterShortLockWait(t *testing.T) {
	for _, lock := range []string{"store", "descriptor", "writer"} {
		t.Run(lock, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			var unlock func()
			switch lock {
			case "store":
				s.mu.Lock()
				unlock = s.mu.Unlock
			case "descriptor":
				s.v3.pub.descMu.Lock()
				unlock = s.v3.pub.descMu.Unlock
			default:
				s.v3.w.mu.Lock()
				unlock = s.v3.w.mu.Unlock
			}
			var once sync.Once
			release := func() { once.Do(unlock) }
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			t.Cleanup(func() {
				release()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("writer did not finish after the release")
				}
			})
			go func() { _, err := s.WriteContext(ctx, 0, blk(1)); done <- err }()
			<-ctx.Done() // the command expires while the short lock is held
			release()
			err := <-done
			done <- err // for the cleanup join
			if !errors.Is(err, ErrWALPressureTimeout) {
				t.Fatalf("expired writer after a %s lock wait: %v", lock, err)
			}
			if s.NextLSN() != 1 || s.dm.len() != 0 {
				t.Fatal("expired writer mutated after the lock wait")
			}
		})
	}
}

// #252 residual P1: the budget is checked inside the WAL writer, after its
// descriptor snapshot and writer lock wait. A real concurrent Sync holds the
// descriptor lock (its slot write, without the store lock) until the command
// has expired; the append waiting behind it must then refuse before any
// payload write, and the Sync completes normally.
func TestWALStoreV3WriteBudgetRefusesAfterDescriptorWaitBehindSync(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := map[bool]string{false: "single", true: "batch"}[batch]
		t.Run(name, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			if _, err := s.Write(7, blk(7)); err != nil { // unsynced: the Sync has a slot to publish
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			entered := make(chan struct{})
			var enterOnce sync.Once
			s.v3.testBeforeSlotWrite = func() error {
				enterOnce.Do(func() { close(entered) })
				<-ctx.Done() // the command expires while the Sync holds the descriptor lock
				return nil
			}
			// The Sync runs first and parks at its leader point (past its store
			// lock phase); the write's post-rollover hook releases it, so it takes
			// the descriptor lock for its slot write after the write's rollover
			// snapshot and before the write's own append snapshot.
			atLeader, proceed := make(chan struct{}), make(chan struct{})
			var leaderOnce, proceedOnce sync.Once
			release := func() { proceedOnce.Do(func() { close(proceed) }) }
			s.v3.testLeaderWaiting = func() {
				leaderOnce.Do(func() { close(atLeader); <-proceed })
			}
			syncDone := make(chan error, 1)
			t.Cleanup(func() {
				release()
				cancel()
				select {
				case <-syncDone:
				case <-time.After(5 * time.Second):
					t.Error("the concurrent Sync did not finish")
				}
			})
			go func() { _, err := s.Sync(); syncDone <- err }()
			<-atLeader
			s.v3.testAfterRollover = func() {
				release()
				<-entered
			}
			head := s.v3.w.head
			var err error
			if batch {
				_, err = s.WriteBatchContext(ctx, 0, [][]byte{blk(1), blk(2)})
			} else {
				_, err = s.WriteContext(ctx, 0, blk(1))
			}
			if !errors.Is(err, ErrWALPressureTimeout) {
				t.Fatalf("expired command after the descriptor wait: %v", err)
			}
			if s.v3.w.head != head {
				t.Fatalf("expired command started payload WAL I/O after the descriptor wait: head %d -> %d", head, s.v3.w.head)
			}
			if err := <-syncDone; err != nil {
				t.Fatalf("the concurrent Sync: %v", err)
			}
			syncDone <- nil
			s.v3.testAfterRollover, s.v3.testBeforeSlotWrite, s.v3.testLeaderWaiting = nil, nil, nil
			if _, err := s.WriteContext(context.Background(), 0, blk(3)); err != nil {
				t.Fatalf("a fresh write after the refusal: %v", err)
			}
		})
	}
}
