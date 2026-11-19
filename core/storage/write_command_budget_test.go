package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestWALStoreV3WriteBudgetIncludesPostAdmissionWait(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			s.admissionTimeout = 40 * time.Millisecond
			// Admission is healthy. A delayed admitted writer must not start a fresh
			// budget after waiting to reach its mutation boundary.
			s.testAfterAdmit = func() { time.Sleep(60 * time.Millisecond) }
			before := s.NextLSN()
			var err error
			if batch {
				_, err = s.WriteBatch(0, [][]byte{blk(1), blk(2)})
			} else {
				_, err = s.Write(0, blk(1))
			}
			if !errors.Is(err, ErrWALPressureTimeout) {
				t.Fatalf("expired command mutated: %v", err)
			}
			if s.NextLSN() != before || s.dm.len() != 0 {
				t.Fatal("expired write changed WAL/dirty state")
			}
			s.testAfterAdmit = nil
			if _, err := s.Write(0, blk(2)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWALStoreV3WriteContextDeadlineIncludesCaptureWait(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			s.captureMu.Lock()
			done := make(chan error, 1)
			go func() {
				var err error
				if batch {
					_, err = s.WriteBatchContext(ctx, 0, [][]byte{blk(1), blk(2)})
				} else {
					_, err = s.WriteContext(ctx, 0, blk(1))
				}
				done <- err
			}()
			defer s.captureMu.Unlock()
			select {
			case err := <-done:
				if !errors.Is(err, ErrWALPressureTimeout) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("writer did not join")
			}
			if s.NextLSN() != 1 || s.dm.len() != 0 {
				t.Fatal("expired capture waiter mutated")
			}
		})
	}
}

func TestWALStoreV3WriteBudgetKeepsTerminalFault(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	injected := errors.New("terminal fsync fault")
	s.latchV3(injected)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := s.WriteContext(ctx, 0, blk(1)); !errors.Is(err, injected) {
		t.Fatalf("fault hidden by deadline: %v", err)
	}
}

// The polling-era append/pressure lock tests are replaced by
// TestWALStoreV3ExpiredWriterRefusesAfterShortLockWait (short locks block, #252).

func TestWALStoreV3WritePressureSampleMatchesOccupancy(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	check := func() {
		t.Helper()
		got, err := s.writePressureSample(context.Background(), time.Now().Add(time.Second))()
		if err != nil || got != s.walUsedFraction() {
			t.Fatalf("sample=%v err=%v normal=%v", got, err, s.walUsedFraction())
		}
	}
	check()
	for lba := uint32(0); lba < 8; lba++ {
		if _, err := s.Write(lba, blk(byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	check()
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	check()
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	check()
}
