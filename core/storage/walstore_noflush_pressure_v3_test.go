package storage

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Write six native WAL rings with the real background flusher and no client
// Sync. Production plain/chunk stores both own a map. The earlier unmapped
// dark fixture deliberately cannot reclaim without map coverage (covered by
// TestWALStoreV3FlusherWritesTheExtentButHoldsTheCheckpointUntilTheMap).
func TestWALStoreV3NoClientSyncSixRings(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			const walSize = 64 * 1024
			const count = 6 * walSize / 4096
			path := filepath.Join(t.TempDir(), "volume")
			s, err := createMappedWALStoreV3ForTest(path, count, 4096, walSize, mode, 4)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			// Exercise the production cooperative pressure budget (four seconds).
			if s.admissionTimeout >= 5*time.Second {
				t.Fatalf("budget=%v", s.admissionTimeout)
			}
			start := time.Now()
			busyCount := 0
			var maxAttempt time.Duration
			for i := uint32(0); i < count; i++ {
				retryDeadline := time.Now().Add(30 * time.Second)
				cpBefore, wackBefore := s.CheckpointLSN(), s.v3.wack.Load()
				retried := false
				for {
					attemptStart := time.Now()
					_, err := s.Write(i, blk(byte(i+1)))
					elapsed := time.Since(attemptStart)
					maxAttempt = max(maxAttempt, elapsed)
					if elapsed >= 5*time.Second || time.Since(start) >= 2*time.Minute || time.Now().After(retryDeadline) {
						t.Fatalf("no-client-Sync exceeded wait bound write=%d/%d attempt=%v total=%v busy=%d err=%v", i, count, elapsed, time.Since(start), busyCount, err)
					}
					if err == nil {
						if retried {
							t.Logf("no-client-Sync resumed write=%d cp=%d->%d wack=%d->%d busy=%d", i, cpBefore, s.CheckpointLSN(), wackBefore, s.v3.wack.Load(), busyCount)
						}
						break
					}
					if !errors.Is(err, ErrWALPressureTimeout) || s.stickyIOFault() != nil {
						t.Fatalf("no-client-Sync non-retryable write %d/%d: %v", i, count, err)
					}
					busyCount++
					retried = true
					t.Logf("no-client-Sync busy write=%d cp=%d wack=%d busy=%d attempt=%v", i, s.CheckpointLSN(), s.v3.wack.Load(), busyCount, elapsed)
					if busyCount == 1 {
						passiveStart := time.Now()
						cpAtBusy, wackAtBusy := s.CheckpointLSN(), s.v3.wack.Load()
						target := s.v3.appended.Load()
						cpObserved, wackObserved := cpAtBusy, wackAtBusy
						for cpAtBusy < target && cpObserved <= cpAtBusy && time.Since(passiveStart) < 10*time.Second {
							time.Sleep(25 * time.Millisecond)
							cpObserved, wackObserved = s.CheckpointLSN(), s.v3.wack.Load()
						}
						t.Logf("no-client-Sync passive write=%d wait=%v cpBefore=%d cpAtBusy=%d cpObserved=%d target=%d wack=%d->%d", i, time.Since(passiveStart), cpBefore, cpAtBusy, cpObserved, target, wackAtBusy, wackObserved)
						if time.Since(passiveStart) >= 10*time.Second || (cpAtBusy < target && cpObserved <= cpAtBusy) || wackObserved < cpObserved {
							t.Fatalf("no-client-Sync passive checkpoint did not advance without another write: cp=%d wack=%d busy=%d", cpObserved, wackObserved, busyCount)
						}
					}
					time.Sleep(25 * time.Millisecond)
				}
			}
			for i := uint32(0); i < count; i++ {
				got, err := s.Read(i)
				if err != nil || !bytes.Equal(got, blk(byte(i+1))) {
					t.Fatalf("readback %d: %v", i, err)
				}
			}
			t.Logf("no-client-Sync mode=%d writes=%d busy=%d elapsed=%v maxAttempt=%v", mode, count, busyCount, time.Since(start), maxAttempt)
		})
	}
}

func pressureStoreNoSync(t *testing.T, records uint32) *WALStore {
	t.Helper()
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), 96, 4096, 64*1024, WModeLagged, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	for i := uint32(0); i < records; i++ {
		if _, err := s.Write(i, blk(byte(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestWALStoreV3PressureSyncFaultDoesNotPublishFrontiers(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		for _, stage := range []string{"fsync", "slot"} {
			t.Run(fmt.Sprintf("mode-%d/%s", mode, stage), func(t *testing.T) {
				s := pressureStoreNoSync(t, 12)
				if err := s.SetWMode(mode); err != nil {
					t.Fatal(err)
				}
				// SetWMode syncs; add fresh uncovered writes without changing pressure.
				if _, err := s.Write(0, blk(99)); err != nil {
					t.Fatal(err)
				}
				before := s.v3.pub.current()
				ack := s.v3.wack.Load()
				boom := errors.New("pressure persistence fault")
				if stage == "slot" {
					s.v3.testBeforeSlotWrite = func() error { return boom }
				} else {
					s.syncWALFile = func() error { return boom }
					s.v3.pub.io.fsync = func() error { return boom }
				}
				err := s.syncWALPressureV3()
				if !errors.Is(err, boom) || s.stickyIOFault() == nil {
					t.Fatalf("lost pressure fault: %v", err)
				}
				if s.v3.wack.Load() != ack || s.CheckpointLSN() != before.CheckpointLSN {
					t.Fatal("failed pressure persistence advanced frontier")
				}
				if _, err := s.Write(1, blk(98)); !errors.Is(err, boom) {
					t.Fatalf("subsequent write lost cause: %v", err)
				}
			})
		}
	}
}

func TestWALStoreV3PressureSyncSkipsInstalling(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	h := beginV3(t, s, 0, idA)
	for i := uint64(1); i <= 12; i++ {
		if err := h.ApplyInstallEntry(uint32(i), blk(byte(i)), i); err != nil {
			t.Fatal(err)
		}
	}
	before := s.v3.pub.current()
	ack := s.v3.wack.Load()
	if err := s.syncWALPressureV3(); err != nil {
		t.Fatal(err)
	}
	if s.v3.pub.current() != before || s.v3.wack.Load() != ack {
		t.Fatal("ordinary pressure sync bypassed install-log gate")
	}
}

func TestWALStoreV3PressureWaitBudgetAndPin(t *testing.T) {
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), 96, 4096, 64*1024, WModeLagged, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.admissionTimeout <= 0 || s.admissionTimeout >= 5*time.Second {
		t.Fatalf("pressure budget %v", s.admissionTimeout)
	}
	// Contract used by an active RF2 catch-up session: do not release its records.
	s.SetRecycleFloorSource(&fakeRecycleFloorSource{floor: 0, anyActive: true})
	start := time.Now()
	var got error
	for i := uint32(0); i < 96; i++ {
		if _, got = s.Write(i, blk(byte(i+1))); got != nil {
			break
		}
	}
	if !errors.Is(got, ErrWALPressureTimeout) || time.Since(start) >= 5*time.Second {
		t.Fatalf("pinned burst outcome %v after %v", got, time.Since(start))
	}
	if s.CheckpointLSN() != 0 {
		t.Fatal("active catch-up pin was bypassed")
	}
}

// TryRLock refuses only once the pending exclusive operation is queued: the
// parked Write still owns a read lock. No sleep guesses when the Lock queues.
func TestWALStoreV3PressureProgressWithPendingCutOrClose(t *testing.T) {
	for _, kind := range []string{"cut", "close", "lifecycle"} {
		closing := kind == "close"
		t.Run(kind, func(t *testing.T) {
			s := pressureStoreNoSync(t, 15)
			parked, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			cutRelease := make(chan struct{})
			releaseCut := sync.OnceFunc(func() { close(cutRelease) })
			var joins []chan struct{}
			t.Cleanup(func() {
				release()
				releaseCut()
				for _, done := range joins {
					select {
					case <-done:
					case <-time.After(6 * time.Second):
						t.Error("pressure worker did not join")
					}
				}
			})
			var once sync.Once
			s.admission.sleepFn = func(d time.Duration) { once.Do(func() { close(parked); <-resume }); time.Sleep(d) }
			writerDone := make(chan struct{})
			joins = append(joins, writerDone)
			var writeErr error
			go func() { defer close(writerDone); _, writeErr = s.Write(16, blk(16)) }()
			select {
			case <-parked:
			case <-time.After(2 * time.Second):
				t.Fatal("writer not parked")
			}
			exclusiveDone := make(chan struct{})
			joins = append(joins, exclusiveDone)
			go func() {
				defer close(exclusiveDone)
				if closing {
					_ = s.Close()
				} else if kind == "lifecycle" {
					s.lifecycleMu.Lock()
					defer s.lifecycleMu.Unlock()
					<-cutRelease
				} else {
					s.captureMu.Lock()
					defer s.captureMu.Unlock()
					<-cutRelease
				}
			}()
			deadline := time.Now().Add(time.Second)
			for {
				var admitted bool
				if kind == "lifecycle" {
					admitted = s.lifecycleMu.TryRLock()
					if admitted {
						s.lifecycleMu.RUnlock()
					}
				} else {
					admitted = s.captureMu.TryRLock()
					if admitted {
						s.captureMu.RUnlock()
					}
				}
				if !admitted {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("exclusive operation did not queue")
				}
				time.Sleep(time.Millisecond)
			}
			// Observe reaching physical persistence while the exclusive lock is
			// pending; disk latency is not an oracle for a Go lock inversion.
			physicalBoundary := make(chan struct{})
			var boundaryOnce sync.Once
			s.testBeforeFileSync = func() { boundaryOnce.Do(func() { close(physicalBoundary) }) }
			flushDone := make(chan struct{})
			joins = append(joins, flushDone)
			var flushErr error
			go func() { defer close(flushDone); flushErr = s.flusher.flushOnce() }()
			select {
			case <-physicalBoundary:
			case <-time.After(5 * time.Second):
				t.Fatal("pressure flusher did not reach persistence with exclusive lock pending")
			}
			// Join physical I/O before releasing the parked writer, so Close
			// cannot close its file under this manually invoked cycle. This
			// join bound is not the admission budget or a whole-I/O promise.
			select {
			case <-flushDone:
			case <-time.After(10 * time.Second):
				t.Fatal("pressure flusher physical I/O did not finish")
			}
			if flushErr != nil {
				t.Fatal(flushErr)
			}
			release()
			select {
			case <-writerDone:
			case <-time.After(3 * time.Second):
				t.Fatal("writer did not drain")
			}
			// The test holds admission at sleep while real fsync runs. A slow
			// disk may consume the unchanged production four-second budget;
			// that typed outcome is permitted, never an indefinite lock wait.
			if writeErr != nil && !errors.Is(writeErr, ErrWALPressureTimeout) {
				t.Fatal(writeErr)
			}
			releaseCut()
			select {
			case <-exclusiveDone:
			case <-time.After(10 * time.Second):
				t.Fatal("exclusive operation did not finish")
			}
		})
	}
}

func TestWALStoreV3FlushBatchCacheIsBoundToRecoveryLifetime(t *testing.T) {
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), 32, 4096, 64*1024, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.WriteBatch(0, [][]byte{blk(1), blk(2), blk(3), blk(4)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	e := s.dm.snapshot()[0]
	cache := &flushRecordCacheV3{recoveryGen: s.v3.recoverGen.Load()}
	if _, _, err := s.flusher.readDirtyRecordV3Cached(e, cache); err != nil {
		t.Fatal(err)
	}
	before := s.FlusherInstrumentation()
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	after := s.FlusherInstrumentation()
	if got := after.WALRecordDecodeOps - before.WALRecordDecodeOps; got != 1 {
		t.Fatalf("four-block batch decoded %d times, want1", got)
	}
	for i := uint32(0); i < 4; i++ {
		if got, err := s.Read(i); err != nil || !bytes.Equal(got, blk(byte(i+1))) {
			t.Fatalf("batch read%d: %v", i, err)
		}
	}
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.flusher.readDirtyRecordV3Cached(e, cache); !errors.Is(err, ErrMapCoverageStale) {
		t.Fatalf("old recovery lifetime cache accepted: %v", err)
	}
}
