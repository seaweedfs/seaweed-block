package replication

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A synced scalar maximum does not establish recovery coverage for other LBAs.
func TestApplyGate_SyncedLiveMaximumDoesNotCoverRecoveryHole(t *testing.T) {
	gate, store := newGate(t)
	if err := gate.ApplyLive(liveLineage(1), 5, makeData(0xBB), 120); err != nil {
		t.Fatal(err)
	}
	if r, err := store.Sync(); err != nil || r != 120 {
		t.Fatalf("sync: R=%d err=%v", r, err)
	}
	want := makeData(0xAA)
	if err := gate.ApplyRecovery(recoveryLineage(2, 100), 6, want, 95); err != nil {
		t.Fatal(err)
	}
	got, err := store.Read(6)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("synced maximum swallowed an uncovered recovery LBA: err=%v got=%x", err, got[:8])
	}
}

// Each batch completes durability and closes coverage; retained version history
// should scale with the outstanding suffix, rather than all LBAs ever written.
func TestApplyGate_DurableFloorBoundsVersionHistory(t *testing.T) {
	const batch = 32
	store := storage.NewBlockStore(1025, 1)
	gate := NewReplicaApplyGate(store)
	for round := uint64(0); round < 32; round++ {
		session := round + 1
		for j := uint64(0); j < batch; j++ {
			lba := uint32(round*batch + j)
			if err := gate.ApplyLive(liveLineage(session), lba, []byte{0xBB}, uint64(lba)+1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := store.Sync(); err != nil {
			t.Fatal(err)
		}
		// A subsequent apply provides a lazy-pruning opportunity.
		if err := gate.ApplyLive(liveLineage(session), 1024, []byte{0xCC}, 2048+round); err != nil {
			t.Fatal(err)
		}
		tracked, _, _, _ := gate.SessionState(session)
		if tracked > batch+1 {
			t.Fatalf("durable version history grew with historical LBAs: round=%d tracked=%d bound=%d", round, tracked, batch+1)
		}
		gate.CloseSession(session)
	}
}
