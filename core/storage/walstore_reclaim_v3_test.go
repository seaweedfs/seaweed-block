package storage

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestWALStoreV3CheckpointReclaimsAcrossRingLaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	s, err := createMappedWALStoreV3ForTest(path, 16, 4096, 64*1024, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	for i := 1; i <= 40; i++ {
		if _, err := s.Write(0, blk(byte(i))); err != nil {
			t.Fatalf("cycle %d after prior synced checkpoints: %v", i, err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
	}
	if floor, valid := s.v3.pub.reuseFloor(); !valid || floor <= 64*1024 {
		t.Fatalf("both-slot reuse floor did not cross a ring lap: floor=%d valid=%v", floor, valid)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Read(0); err != nil || !bytes.Equal(got, blk(40)) {
		t.Fatalf("latest acknowledged bytes after ring reuse/reopen: %v", err)
	}
}

func TestWALStoreV3ReclaimKeepsPartialBatchAndRetention(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if _, err := s.WriteBatch(0, [][]byte{blk(1), blk(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.SetRecycleFloorSource(&fakeRecycleFloorSource{floor: 1, anyActive: true})
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if d := s.v3.pub.current(); d.CheckpointLSN != 1 || d.RetainedTail != 0 {
		t.Fatalf("partial batch released before checkpoint covered its last LSN: %+v", d)
	}
	s.SetRecycleFloorSource(nil)
	s.SetRecoveryRetentionLSNs(1)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if d := s.v3.pub.current(); d.CheckpointLSN != 2 || d.RetainedTail != 0 {
		t.Fatalf("retention split and released a retained batch: %+v", d)
	}
	var got []RecoveryEntry
	if err := s.ScanLBAs(2, func(e RecoveryEntry) error { got = append(got, e); return nil }); err != nil || len(got) != 1 || !bytes.Equal(got[0].Data, blk(2)) {
		t.Fatalf("retained batch suffix is not available to catch-up: entries=%d err=%v", len(got), err)
	}
	s.SetRecoveryRetentionLSNs(0)
	if _, err := s.Write(3, blk(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if floor, ok := s.v3.pub.reuseFloor(); !ok || floor != d.RetainedTail || floor == 0 {
		t.Fatalf("idle post-flush fallback still pins the released prefix: %d %v", floor, ok)
	}
}

func TestWALStoreV3ReclaimRebuildsIndexAfterPhysicalRecovery(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 0, 1)
	writeSynced(t, s, 1, 2)
	crashReadOnlyStoreV3(t, s)
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	r.DisableAutoFlushForRecoveryTest()
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := r.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	d := r.v3.pub.current()
	if floor, ok := r.v3.pub.reuseFloor(); !ok || floor == 0 || floor != d.RetainedTail {
		t.Fatalf("physical recovery did not restore reclaimable record boundaries: %d %v", floor, ok)
	}
}

func TestWALStoreV3ReclaimSecondSlotFailureKeepsOldPin(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 0, 1)
	writeSlot := s.v3.pub.io.writeSlot
	writes := 0
	s.v3.pub.io.writeSlot = func(off int64, data []byte) error {
		writes++
		if writes == 2 {
			return errors.New("held fallback rollover write failed")
		}
		return writeSlot(off, data)
	}
	if err := s.flusher.flushOnce(); !errors.Is(err, ErrDescriptorPublishUncertain) || writes != 2 {
		t.Fatalf("rollover failure was not surfaced: writes=%d err=%v", writes, err)
	}
	if floor, valid := s.v3.pub.reuseFloor(); valid && floor != 0 {
		t.Fatalf("failed rollover granted reuse over the old slot: floor=%d", floor)
	}
	head := s.v3.w.logicalHeadV3()
	if _, err := s.Write(1, blk(2)); err == nil || s.v3.w.logicalHeadV3() != head {
		t.Fatalf("failed rollover allowed another append: %v", err)
	}
	crashReadOnlyStoreV3(t, s)
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Read(1); err != nil || !bytes.Equal(got, blk(1)) {
		t.Fatalf("acknowledged bytes lost at the fallback failure window: %v", err)
	}
}

func TestWALStoreV3ReclaimAfterInstallingPhysicalReopen(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	h, err := s.BeginBaseInstall(0, idA)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := h.ApplyInstallEntry(uint32(i), blk(byte(i)), i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SyncInstallLog(idA); err != nil {
		t.Fatal(err)
	}
	crashReadOnlyStoreV3(t, s)
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	r.DisableAutoFlushForRecoveryTest()
	if _, err := r.Recover(); !errors.Is(err, ErrBaseIncomplete) {
		t.Fatalf("installing reopen: %v", err)
	}
	h, err = r.resumeBaseInstallV3(idA)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := r.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if floor, ok := r.v3.pub.reuseFloor(); !ok || floor == 0 {
		t.Fatalf("completed recovered install suffix cannot be reclaimed: floor=%d valid=%v", floor, ok)
	}
	if got, err := r.Read(3); err != nil || !bytes.Equal(got, blk(3)) {
		t.Fatalf("recovered install bytes: %v", err)
	}
}
