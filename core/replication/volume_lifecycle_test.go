package replication

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// T4d-4 part A — lifecycle tests for ReplicationVolume.Stop +
// BUG-005 non-repeat. Pins INV-REPL-LIFECYCLE-HANDLE-BORROWED-001.

// TestReplicationVolume_Stop_Idempotent — Stop must be safe to call
// multiple times; second + later calls are no-ops returning nil.
func TestReplicationVolume_Stop_Idempotent(t *testing.T) {
	store := storage.NewBlockStore(64, 4096)
	v := NewReplicationVolume("v1", store)

	if err := v.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := v.Stop(); err != nil {
		t.Fatalf("second Stop (must be no-op): %v", err)
	}
	if err := v.Close(); err != nil {
		t.Fatalf("Close after Stop: %v", err)
	}
}

// TestReplicationVolume_Stop_DoesNotCloseBorrowedStore pins
// INV-REPL-LIFECYCLE-HANDLE-BORROWED-001 + BUG-005 discipline:
// ReplicationVolume.Stop MUST NOT call store.Close() on the borrowed
// substrate handle. The store is owned by the caller (Provider /
// host); Stop only releases peer-side resources.
func TestReplicationVolume_Stop_DoesNotCloseBorrowedStore(t *testing.T) {
	store := storage.NewBlockStore(64, 4096)
	v := NewReplicationVolume("v1", store)

	if err := v.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Store must STILL be usable after Stop — caller owns it.
	data := make([]byte, 4096)
	data[0] = 0xAA
	if _, err := store.Write(0, data); err != nil {
		t.Fatalf("FAIL: store.Write after volume.Stop: %v — Stop closed the borrowed store (BUG-005 regression!)", err)
	}
	if got, err := store.Read(0); err != nil || got[0] != 0xAA {
		t.Fatalf("FAIL: store unusable after volume.Stop — got %v err=%v", got, err)
	}
}

// TestReplicationVolume_Stop_TearsDownPeers — Stop releases all
// peers' executor sessions. Empty-set lifecycle (no peers) is the
// minimal pin; full multi-peer teardown is exercised by integration
// scenarios.
func TestReplicationVolume_Stop_TearsDownPeers(t *testing.T) {
	store := storage.NewBlockStore(64, 4096)
	v := NewReplicationVolume("v1", store)

	if v.PeerCount() != 0 {
		t.Errorf("fresh volume PeerCount = %d, want 0", v.PeerCount())
	}

	if err := v.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if v.PeerCount() != 0 {
		t.Errorf("after Stop PeerCount = %d, want 0", v.PeerCount())
	}
}
