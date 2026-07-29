package replication

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
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

func TestReplicationVolume_OnLocalWriteRejectsPreCanceledContext(t *testing.T) {
	v := volumeHarness(t, "vol-canceled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := v.OnLocalWrite(ctx, LocalWrite{LBA: 1, Data: make([]byte, 4096), LSN: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OnLocalWrite error=%v want context canceled", err)
	}
	v.orderMu.Lock()
	defer v.orderMu.Unlock()
	if len(v.pending) != 0 {
		t.Fatalf("pending writes=%d want 0", len(v.pending))
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
