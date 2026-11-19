package storage

import (
	"bytes"
	"errors"
	"path/filepath"
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
		t.Fatalf("native occupancy=%g want%g; legacy=%g", got, want, s.wal.usedFraction())
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
	notified := make(chan struct{}, 1)
	notify := s.admission.notifyFn
	s.admission.notifyFn = func() {
		select {
		case notified <- struct{}{}:
		default:
		}
		notify()
	}
	done := make(chan error, 1)
	go func() {
		err := s.admission.Acquire(time.Second)
		if err == nil {
			s.admission.Release()
		}
		done <- err
	}()
	select {
	case <-notified:
	case <-time.After(time.Second):
		t.Fatal("pressure did not wake flusher")
	}
	select {
	case err := <-done:
		t.Fatalf("admitted before stalled flusher reclaimed: %v", err)
	default:
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
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
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.admission.Acquire(time.Second); !errors.Is(err, errVolumeClosed) {
		if err == nil {
			s.admission.Release()
		}
		t.Fatalf("closed pressured store: %v", err)
	}
}
