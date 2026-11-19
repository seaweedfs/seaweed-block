package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// A completed metadata rollover is retained, but it does not authorize new
// payload I/O after the command's original admission deadline.
func TestWALStoreV3WriteBudgetRechecksAfterRollover(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, outcome := range []string{"expired", "healthy", "fault"} {
			t.Run(fmt.Sprintf("batch=%t/%s", batch, outcome), func(t *testing.T) {
				s, _ := newMappedCheckpointStoreV3(t)
				s.SetRecoveryRetentionLSNs(2)
				writeSynced(t, s, 0, 1)
				writeSynced(t, s, 1, 2)
				if err := s.flusher.flushOnce(); err != nil {
					t.Fatal(err)
				}
				d := s.v3.pub.current()
				s.SetRecoveryRetentionLSNs(1)
				tail := s.v3.w.reclaimThroughV3(d.CheckpointLSN - 1)
				if _, err := s.v3.pub.publish(nil, nil, func(n *descriptorV3) error {
					n.RetainedTail = tail
					n.RetainedHead = max(n.RetainedHead, tail)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if floor, ok := s.v3.pub.reuseFloor(); !ok || floor >= tail {
					t.Fatal("no pinned fallback")
				}
				wait := 100 * time.Millisecond
				if outcome == "healthy" {
					wait = 5 * time.Second
				}
				ctx, cancel := context.WithTimeout(context.Background(), wait)
				defer cancel()
				boom := errors.New("rollover slot fault")
				writeSlot := s.v3.pub.io.writeSlot
				calls := 0
				s.v3.pub.io.writeSlot = func(off int64, b []byte) error {
					calls++
					if outcome != "healthy" {
						<-ctx.Done()
					}
					if outcome == "fault" {
						return boom
					}
					return writeSlot(off, b)
				}
				defer func() { s.v3.pub.io.writeSlot = writeSlot }()
				head, next, dirty := s.v3.w.logicalHeadV3(), s.NextLSN(), s.dm.len()
				gen := s.v3.pub.current().SlotGen
				var err error
				if batch {
					_, err = s.WriteBatchContext(ctx, 2, [][]byte{blk(3), blk(4)})
				} else {
					_, err = s.WriteContext(ctx, 2, blk(3))
				}
				if calls != 1 {
					t.Fatalf("rollover calls=%d err=%v", calls, err)
				}
				if outcome == "healthy" {
					if err != nil || s.NextLSN() <= next {
						t.Fatalf("healthy write: %v", err)
					}
					return
				}
				if s.NextLSN() != next || s.v3.w.logicalHeadV3() != head || s.dm.len() != dirty {
					t.Fatal("refused command started payload I/O")
				}
				if outcome == "fault" {
					if !errors.Is(err, boom) || s.stickyIOFault() == nil {
						t.Fatalf("terminal fault masked: %v", err)
					}
					return
				}
				if !errors.Is(err, ErrWALPressureTimeout) {
					t.Fatalf("expired command: %v", err)
				}
				if s.v3.pub.current().SlotGen <= gen {
					t.Fatal("completed rollover was rolled back")
				}
				if floor, ok := s.v3.pub.reuseFloor(); !ok || floor != tail {
					t.Fatalf("completed rollover not retained: %d %v", floor, ok)
				}
			})
		}
	}
}

type budgetCaptureWaitContext struct {
	context.Context
	parked chan struct{}
	once   sync.Once
}

func (c *budgetCaptureWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.parked) })
	return c.Context.Done()
}
func TestWALStoreV3LocalBudgetIncludesWitnessedCaptureWait(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			s.admissionTimeout = 40 * time.Millisecond
			ctx := &budgetCaptureWaitContext{Context: context.Background(), parked: make(chan struct{})}
			s.captureMu.Lock()
			locked := true
			done := make(chan error, 1)
			defer func() {
				if locked {
					s.captureMu.Unlock()
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("writer failed to join after release")
				}
			}()
			go func() {
				var err error
				if batch {
					_, err = s.WriteBatchContext(ctx, 0, [][]byte{blk(1), blk(2)})
				} else {
					_, err = s.WriteContext(ctx, 0, blk(1))
				}
				done <- err
			}()
			select {
			case <-ctx.parked:
			case <-time.After(time.Second):
				t.Fatal("capture waiter was not witnessed")
			}
			select {
			case err := <-done:
				done <- err
				if !errors.Is(err, ErrWALPressureTimeout) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("local budget restarted behind capture lock")
			}
			s.captureMu.Unlock()
			locked = false
			if s.NextLSN() != 1 || s.dm.len() != 0 {
				t.Fatal("expired local waiter mutated")
			}
		})
	}
}
