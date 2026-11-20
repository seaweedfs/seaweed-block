package storage

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWALStoreV3AdmissionUsesNativeOccupancy(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "volume"), 256, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if s.v3 == nil {
		t.Fatal("public selector did not create v3")
	}
	if got := s.admission.walUsed(); got != 0 {
		t.Fatalf("initial occupancy=%g", got)
	}
	blocks := make([][]byte, 32)
	for i := range blocks {
		blocks[i] = blk(byte(i))
	}
	if _, err := s.WriteBatch(0, blocks); err != nil {
		t.Fatal(err)
	}
	want := float64(32*4096+70) / float64(64*1024*1024)
	if got := s.admission.walUsed(); got != want {
		t.Fatalf("native occupancy=%g want%g", got, want)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.admission.walUsed(); got != 0 {
		t.Fatalf("both-slot-released occupancy=%g", got)
	}
}

// Five physical ring sizes, with the real background flusher and periodic
// durable Syncs. The small geometry keeps this regression a fast unit test;
// the public-provider control separately exercises the default 64 MiB WAL.
func TestWALStoreV3AdmissionBurstFiveRings(t *testing.T) {
	const walSize = 64 * 1024
	const blocks = 5 * walSize / 4096
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), blocks, 4096, walSize, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	for i := uint32(0); i < blocks; i++ {
		if _, err := s.Write(i, blk(byte(i+1))); err != nil {
			t.Fatalf("write %d of %d: %v", i, blocks, err)
		}
		if (i+1)%10 == 0 {
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := uint32(0); i < blocks; i++ {
		if got, err := s.Read(i); err != nil || !bytes.Equal(got, blk(byte(i+1))) {
			t.Fatalf("readback %d: %v", i, err)
		}
	}
	if floor, ok := s.v3.pub.reuseFloor(); !ok || floor <= walSize {
		t.Fatalf("no actual multi-lap reuse: %d %v", floor, ok)
	}
}

func admissionPressureV3(t *testing.T, records int) *WALStore {
	t.Helper()
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), 32, 4096, 64*1024, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	for i := 0; i < records; i++ {
		if _, err := s.Write(uint32(i), blk(byte(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWALStoreV3AdmissionSoftThrottles(t *testing.T) {
	s := admissionPressureV3(t, 12)
	var delay time.Duration
	s.admission.sleepFn = func(d time.Duration) { delay += d }
	if err := s.admission.Acquire(time.Second); err != nil {
		t.Fatal(err)
	}
	s.admission.Release()
	if s.admission.PressureState() != "soft" || delay <= 0 || delay > 5*time.Millisecond {
		t.Fatalf("soft throttle: state=%s delay=%v", s.admission.PressureState(), delay)
	}
}

func TestWALStoreV3AdmissionStallDeadlineAndResume(t *testing.T) {
	s := admissionPressureV3(t, 15)
	if err := s.admission.Acquire(10 * time.Millisecond); !errors.Is(err, errWALFull) {
		if err == nil {
			s.admission.Release()
		}
		t.Fatalf("stalled flusher must reach bounded deadline: %v", err)
	}
	parked, release, done := holdHardAdmissionWaitV3(t, s)
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("admission never entered hard-pressure sleep")
	}
	select {
	case err := <-done:
		t.Fatalf("admitted before actual reclaim: %v", err)
	default:
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	// This assertion budget starts AFTER disk fsync/checkpoint work. The
	// controller retains the store's real admission deadline, not an artificial1s.
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reclaim did not release waiter")
	}
	if got := s.admission.PressureState(); got != "normal" {
		t.Fatalf("post-checkpoint pressure=%s", got)
	}
}

func TestWALStoreV3AdmissionCloseEndsPressureWait(t *testing.T) {
	s := admissionPressureV3(t, 15)
	parked, release, done := holdHardAdmissionWaitV3(t, s)
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("no existing hard-pressure waiter")
	}
	// This is an existing controller waiter. An actual Write holds lifecycleMu
	// through admission; this test does not claim to change that Close protocol.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, errVolumeClosed) {
			t.Fatalf("existing pressured waiter after Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release existing controller waiter")
	}
}

// The first actual hard-pressure sleep is the barrier. Notify alone is earlier
// than the loop's closed check and therefore cannot prove an existing waiter.
func holdHardAdmissionWaitV3(t *testing.T, s *WALStore) (<-chan struct{}, func(), <-chan error) {
	t.Helper()
	if s.admission.PressureState() != "hard" {
		t.Fatal("setup lacks native hard pressure")
	}
	parked, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce, parkOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	sleep := s.admission.sleepFn
	s.admission.sleepFn = func(d time.Duration) {
		parkOnce.Do(func() { close(parked); <-resume })
		sleep(d)
	}
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		err := s.admission.Acquire(s.admissionTimeout)
		if err == nil {
			s.admission.Release()
		}
		done <- err
		close(finished)
	}()
	t.Cleanup(func() {
		release()
		_ = s.Close()
		// Even a broken close guard is bounded by the real controller deadline;
		// join the worker instead of abandoning it when an assertion fails.
		select {
		case <-finished:
		case <-time.After(s.admissionTimeout + time.Second):
			t.Error("admission worker did not join")
		}
	})
	return parked, release, done
}
