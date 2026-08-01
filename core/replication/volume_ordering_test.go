package replication

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestReplicationVolume_ResequencesConcurrentArrivalByLSN(t *testing.T) {
	addr, replica := replicaHarness(t, "resequence")
	v := volumeHarness(t, "vol-resequence")
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r1", addr, 1, 1)}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lsn2Done := make(chan error, 1)
	go func() {
		data := make([]byte, 4096)
		data[0] = 0x22
		lsn2Done <- v.OnLocalWrite(ctx, LocalWrite{LBA: 2, Data: data, LSN: 2})
	}()

	deadline := time.Now().Add(time.Second)
	for {
		v.orderMu.Lock()
		_, pending := v.pending[2]
		v.orderMu.Unlock()
		if pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("LSN 2 did not enter the resequencer")
		}
		time.Sleep(time.Millisecond)
	}

	data1 := make([]byte, 4096)
	data1[0] = 0x11
	if err := v.OnLocalWrite(ctx, LocalWrite{LBA: 1, Data: data1, LSN: 1}); err != nil {
		t.Fatalf("LSN 1: %v", err)
	}
	if err := <-lsn2Done; err != nil {
		t.Fatalf("LSN 2: %v", err)
	}

	waitForReplicaLBA(t, replica, 1, 0x11, 0, time.Second)
	waitForReplicaLBA(t, replica, 2, 0x22, 0, time.Second)
	_, _, head := replica.Boundaries()
	if head != 2 {
		t.Fatalf("replica head=%d want 2", head)
	}
}

func TestPhase175ReplicationVolumeAdvancesAfterSnapshotRestore(t *testing.T) {
	store, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "restored.bin"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	v := NewReplicationVolume("restored-volume", store)
	for i := uint32(0); i < 27; i++ {
		if _, err := store.Write(i, make([]byte, 4096)); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.AdvanceAfterSnapshotRestore(27); err != nil {
		t.Fatal(err)
	}
	if err := v.AdvanceAfterSnapshotRestore(27); err != nil {
		t.Fatalf("idempotent advance: %v", err)
	}
	lsn, err := store.Write(27, make([]byte, 4096))
	if err != nil || lsn != 28 {
		t.Fatalf("first post-restore write lsn=%d err=%v", lsn, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := v.OnLocalWrite(ctx, LocalWrite{LBA: 27, Data: make([]byte, 4096), LSN: lsn}); err != nil {
		t.Fatalf("first post-restore write did not pass resequencer: %v", err)
	}
	if err := v.AdvanceAfterSnapshotRestore(27); err != nil {
		t.Fatalf("advance retry after post-restore write: %v", err)
	}
	if err := v.AdvanceAfterSnapshotRestore(26); err == nil || !strings.Contains(err.Error(), "frontier changed") {
		t.Fatalf("different restore frontier retry error=%v", err)
	}
}

func TestPhase175ReplicationVolumeRejectsUnsafeRestoreAdvance(t *testing.T) {
	store := storage.NewBlockStore(64, 4096)
	v := NewReplicationVolume("restored-volume", store)
	if _, err := store.Write(0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := v.AdvanceAfterSnapshotRestore(2); err == nil || !strings.Contains(err.Error(), "does not match storage next LSN") {
		t.Fatalf("mismatched frontier error=%v", err)
	}
	v.orderMu.Lock()
	v.pending[1] = &orderedLocalWrite{write: LocalWrite{LSN: 1}}
	v.orderMu.Unlock()
	if err := v.AdvanceAfterSnapshotRestore(1); err == nil || !strings.Contains(err.Error(), "active local writes") {
		t.Fatalf("active-write advance error=%v", err)
	}
}

func TestPhase175ReplicationVolumeRestoreAdvanceSurvivesRestartBeforeReadiness(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restored.bin")
	store, err := storage.CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	first := NewReplicationVolume("restored-volume", store)
	for i := uint32(0); i < 27; i++ {
		if _, err := store.Write(i, make([]byte, 4096)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := first.AdvanceAfterSnapshotRestore(27); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := storage.OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if frontier, err := reopened.Recover(); err != nil || frontier != 27 {
		t.Fatalf("recover frontier=%d err=%v", frontier, err)
	}
	second := NewReplicationVolume("restored-volume", reopened)
	t.Cleanup(func() { _ = second.Close() })
	if err := second.AdvanceAfterSnapshotRestore(27); err != nil {
		t.Fatalf("activation callback retry after restart: %v", err)
	}
	lsn, err := reopened.Write(27, make([]byte, 4096))
	if err != nil || lsn != 28 {
		t.Fatalf("post-restart write lsn=%d err=%v", lsn, err)
	}
	if err := second.OnLocalWrite(context.Background(), LocalWrite{LBA: 27, Data: make([]byte, 4096), LSN: lsn}); err != nil {
		t.Fatalf("post-restart write did not pass resequencer: %v", err)
	}
}

func TestReplicationVolume_CloseUnblocksWriteWaitingForMissingLSN(t *testing.T) {
	v := volumeHarness(t, "vol-close-pending")
	done := make(chan error, 1)
	go func() {
		done <- v.OnLocalWrite(context.Background(), LocalWrite{
			LBA:  2,
			Data: make([]byte, 4096),
			LSN:  2,
		})
	}()

	waitForPendingLSN(t, v, 2)
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "closed before LSN 2 shipped") {
			t.Fatalf("pending write error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending write remained blocked after Close")
	}
}

func TestReplicationVolume_SyncWaitingForMissingLSNHonorsContext(t *testing.T) {
	v := volumeHarness(t, "vol-sync-missing")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := v.Sync(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Sync error=%v want context deadline exceeded", err)
	}
}

func TestReplicationVolume_CanceledCallerDoesNotDropCommittedLSN(t *testing.T) {
	addr, replica := replicaHarness(t, "canceled-caller")
	v := volumeHarness(t, "vol-canceled")
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r1", addr, 1, 1)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	first := make([]byte, 4096)
	first[0] = 0x41
	_ = v.OnLocalWrite(ctx, LocalWrite{LBA: 1, Data: first, LSN: 1})
	waitForReplicaLBA(t, replica, 1, 0x41, 0, time.Second)

	second := make([]byte, 4096)
	second[0] = 0x42
	if err := v.OnLocalWrite(context.Background(), LocalWrite{LBA: 2, Data: second, LSN: 2}); err != nil {
		t.Fatalf("LSN 2 remained blocked after canceled LSN 1 caller: %v", err)
	}
	waitForReplicaLBA(t, replica, 2, 0x42, 0, time.Second)
}

func TestReplicationVolume_RejectsDuplicateInFlightLSN(t *testing.T) {
	v := volumeHarness(t, "vol-duplicate-inflight")
	v.orderMu.Lock()
	v.inflightLSN = 1
	v.orderMu.Unlock()

	err := v.OnLocalWrite(context.Background(), LocalWrite{LBA: 1, Data: make([]byte, 4096), LSN: 1})
	if err == nil || !strings.Contains(err.Error(), "duplicate in-flight LSN 1") {
		t.Fatalf("duplicate in-flight error=%v", err)
	}
}

func waitForPendingLSN(t *testing.T, v *ReplicationVolume, lsn uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		v.orderMu.Lock()
		_, pending := v.pending[lsn]
		v.orderMu.Unlock()
		if pending {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("LSN %d did not enter the resequencer", lsn)
		}
		time.Sleep(time.Millisecond)
	}
}
