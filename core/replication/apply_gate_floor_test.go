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

// Each batch has a successful recovery close and an explicit prefix certificate.
// Durability alone is insufficient; ordinary healthy traffic remains unbounded.
func TestApplyGate_CertifiedRecoveryClosesBoundVersionHistory(t *testing.T) {
	const batch = 32
	store := storage.NewBlockStore(1025, 1)
	gate := NewReplicaApplyGate(store)
	var lsn uint64
	for round := uint64(0); round < 32; round++ {
		session := round + 1
		for j := uint64(0); j < batch; j++ {
			lba := uint32(round*batch + j)
			lsn++
			if err := gate.ApplyLive(liveLineage(session), lba, []byte{0xBB}, lsn); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := store.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := gate.CertifyRecoveryPrefix(recoveryLineage(session, lsn)); err != nil {
			t.Fatal(err)
		}
		// A subsequent apply provides a lazy-pruning opportunity.
		lsn++
		if err := gate.ApplyLive(liveLineage(session), 1024, []byte{0xCC}, lsn); err != nil {
			t.Fatal(err)
		}
		tracked, _, _, _ := gate.SessionState(session)
		if tracked > batch+1 {
			t.Fatalf("durable version history grew with historical LBAs: round=%d tracked=%d bound=%d", round, tracked, batch+1)
		}
		gate.CloseSession(session)
	}
}

func TestApplyGate_PrunedDurableVersionsRemainStale(t *testing.T) {
	gate, store := newGate(t)
	want := makeData(0xBB)
	for lba := uint32(0); lba < 32; lba++ {
		if err := gate.ApplyLive(liveLineage(1), lba, want, uint64(lba)+1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := gate.CertifyRecoveryPrefix(recoveryLineage(1, 32)); err != nil {
		t.Fatal(err)
	}
	gate.CloseSession(1)
	if err := gate.ApplyRecovery(recoveryLineage(2, 32), 5, makeData(0xAA), 1); err != nil {
		t.Fatal(err)
	}
	if !gate.SessionRecoveryCoverage(2, 5) {
		t.Fatal("pruned stale recovery did not cover its own session")
	}
	if err := gate.ApplyLive(liveLineage(3), 6, makeData(0xCC), 2); err == nil {
		t.Fatal("pruned stale live write was accepted")
	}
	if gate.SessionRecoveryCoverage(3, 6) {
		t.Fatal("stale live write advanced recovery coverage")
	}
	for _, lba := range []uint32{5, 6} {
		got, err := store.Read(lba)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("pruned stale record changed LBA %d: err=%v", lba, err)
		}
	}
	tracked, _, _, _ := gate.SessionState(2)
	if tracked != 0 {
		t.Fatalf("durable versions were not pruned: tracked=%d want=0", tracked)
	}
}

func TestApplyGate_CertifiedPrefixFreezesAcrossSessions(t *testing.T) {
	gate, store := newGate(t)
	if err := gate.ApplyRecovery(recoveryLineage(1, 1), 0, makeData(1), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := gate.CertifyRecoveryPrefix(recoveryLineage(1, 1)); err != nil {
		t.Fatal(err)
	}
	gate.CloseSession(1)
	if err := gate.ApplyLive(liveLineage(2), 5, makeData(0xBB), 120); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := gate.CertifiedPrefix(); got != 1 {
		t.Fatalf("new session or Sync extended certified floor: P=%d", got)
	}
	want := makeData(0xAA)
	if err := gate.ApplyRecovery(recoveryLineage(3, 100), 6, want, 95); err != nil {
		t.Fatal(err)
	}
	got, err := store.Read(6)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("frozen prefix swallowed missing recovery: err=%v", err)
	}
	if err := gate.CertifyRecoveryPrefix(recoveryLineage(3, 121)); err == nil {
		t.Fatal("certificate above durable boundary accepted")
	}
	if err := gate.CertifyRecoveryPrefix(recoveryLineage(3, 1)); err != nil {
		t.Fatal(err)
	}
	if gate.CertifiedPrefix() != 1 {
		t.Fatal("failed/repeated certificate changed prefix")
	}
}
