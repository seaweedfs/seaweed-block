package replication

// RebaseShipLSN: the ship order follows a seeded store frontier forward,
// and never drops or reorders a write it already holds.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestReplicationVolume_RebaseShipLSN(t *testing.T) {
	store := storage.NewBlockStore(64, 4096)
	v := NewReplicationVolume("vol1", store)
	t.Cleanup(func() { _ = v.Close() })
	base := v.nextShipLSN

	if err := v.RebaseShipLSN(base); err != nil {
		t.Fatalf("equal LSN: %v; want a no-op", err)
	}
	if err := v.RebaseShipLSN(base + 5000); err != nil {
		t.Fatalf("forward rebase: %v", err)
	}
	if err := v.RebaseShipLSN(base + 10); err == nil {
		t.Fatal("rebase moved the ship order backwards")
	}

	// After the rebase, Sync of a target just below the new LSN returns
	// at once, and a write at the new LSN ships.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := v.Sync(ctx, base+4999); err != nil {
		t.Fatalf("Sync below the rebased LSN: %v", err)
	}
	if err := v.OnLocalWrite(ctx, LocalWrite{LBA: 1, Data: make([]byte, 4096), LSN: base + 5000}); err != nil {
		t.Fatalf("write at the rebased LSN: %v", err)
	}
	if err := v.Sync(ctx, base+5000); err != nil {
		t.Fatalf("Sync of the write at the rebased LSN: %v", err)
	}
}

// A write held out of order (its predecessor not yet arrived) makes the
// rebase refuse instead of dropping it.
func TestReplicationVolume_RebaseShipLSNRefusesWithPendingWrites(t *testing.T) {
	store := storage.NewBlockStore(64, 4096)
	v := NewReplicationVolume("vol1", store)
	t.Cleanup(func() { _ = v.Close() })
	next := v.nextShipLSN
	if _, err := v.enqueueLocalWrite(LocalWrite{LBA: 1, Data: make([]byte, 4096), LSN: next + 1}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		v.orderMu.Lock()
		held := len(v.pending) == 1 && !v.draining
		v.orderMu.Unlock()
		if held {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the out-of-order write was not held")
		}
		time.Sleep(time.Millisecond)
	}
	if err := v.RebaseShipLSN(next + 100); err == nil {
		t.Fatal("rebase succeeded with a pending write")
	}
	v.orderMu.Lock()
	kept := v.pending[next+1] != nil && v.nextShipLSN == next
	v.orderMu.Unlock()
	if !kept {
		t.Fatal("the refused rebase dropped the pending write or moved the ship order")
	}
}

func TestReplicationVolume_RebaseShipLSNRefusesWhenClosed(t *testing.T) {
	v := NewReplicationVolume("vol1", storage.NewBlockStore(64, 4096))
	_ = v.Close()
	err := v.RebaseShipLSN(v.nextShipLSN + 1)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("rebase on a closed volume: %v; want a refusal", err)
	}
}
