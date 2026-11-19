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
			// Bound the RED and a genuinely stalled local flusher without a 30s test.
			// The healthy background path must drain this small geometry before it.
			s.admissionTimeout = 2 * time.Second
			for i := uint32(0); i < count; i++ {
				if _, err := s.Write(i, blk(byte(i+1))); err != nil {
					t.Fatalf("no-client-Sync write %d/%d: %v", i, count, err)
				}
			}
			for i := uint32(0); i < count; i++ {
				got, err := s.Read(i)
				if err != nil || !bytes.Equal(got, blk(byte(i+1))) {
					t.Fatalf("readback %d: %v", i, err)
				}
			}
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
	for _, closing := range []bool{false, true} {
		t.Run(fmt.Sprintf("close-%v", closing), func(t *testing.T) {
			s := pressureStoreNoSync(t, 15)
			s.admissionTimeout = 2 * time.Second
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
				} else {
					s.captureMu.Lock()
					defer s.captureMu.Unlock()
					<-cutRelease
				}
			}()
			deadline := time.Now().Add(time.Second)
			for {
				var admitted bool
				if closing {
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
			flushDone := make(chan struct{})
			joins = append(joins, flushDone)
			var flushErr error
			go func() { defer close(flushDone); flushErr = s.flusher.flushOnce() }()
			select {
			case <-flushDone:
			case <-time.After(time.Second):
				t.Fatal("pressure flusher waited behind pending exclusive lock")
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
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			releaseCut()
			select {
			case <-exclusiveDone:
			case <-time.After(time.Second):
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
