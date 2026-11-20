package replication

import (
	"bytes"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Both sessions are initialized before either writes the contested LBA.
// This fixes the ordering without relying on a probabilistic goroutine race.
func TestApplyGate_OverlappingSessionsNeverRegressBytes(t *testing.T) {
	for _, live := range []bool{false, true} {
		name := "recovery_then_older_recovery"
		if live {
			name = "live_then_older_recovery"
		}
		t.Run(name, func(t *testing.T) {
			gate, store := newGate(t)
			older, newer := recoveryLineage(10, 120), recoveryLineage(11, 120)
			if err := gate.ApplyRecovery(older, 0, makeData(1), 1); err != nil {
				t.Fatal(err)
			}
			if err := gate.ApplyRecovery(newer, 1, makeData(2), 2); err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0xBB}, blockSize)
			apply := gate.ApplyRecovery
			if live {
				apply = gate.ApplyLive
			}
			if err := apply(newer, 5, want, 120); err != nil {
				t.Fatal(err)
			}
			if err := gate.ApplyRecovery(older, 5, bytes.Repeat([]byte{0xAA}, blockSize), 95); err != nil {
				t.Fatal(err)
			}
			got, err := store.Read(5)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("cross-session stale recovery regressed bytes: got %x want %x", got[:8], want[:8])
			}
			if !gate.SessionRecoveryCoverage(older.SessionID, 5) {
				t.Fatal("stale recovery did not advance its own coverage")
			}
		})
	}
}

func TestApplyGate_CloseAndLowerSeedNeverEraseReplicaVersion(t *testing.T) {
	store := &fakeStoreWithSeed{BlockStore: storage.NewBlockStore(64, blockSize), seed: map[uint32]uint64{5: 1}}
	gate := NewReplicaApplyGate(store)
	want := bytes.Repeat([]byte{0xBB}, blockSize)
	if err := gate.ApplyLive(liveLineage(1), 5, want, 120); err != nil {
		t.Fatal(err)
	}
	gate.CloseSession(1)
	if _, _, _, found := gate.SessionState(1); found {
		t.Fatal("closed session coverage retained")
	}
	if err := gate.ApplyRecovery(recoveryLineage(2, 120), 5, makeData(0xAA), 95); err != nil {
		t.Fatal(err)
	}
	if err := gate.ApplyLive(liveLineage(3), 5, makeData(0xCC), 100); err == nil {
		t.Fatal("cross-session stale live write was accepted")
	}
	got, err := store.Read(5)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("teardown/reseed regressed replica bytes: err=%v", err)
	}
	if !gate.SessionRecoveryCoverage(2, 5) || gate.SessionRecoveryCoverage(3, 5) {
		t.Fatal("stale skip/fail mixed lane coverage")
	}
}

type failOnceApplyStore struct {
	*storage.BlockStore
	fail bool
}

func (s *failOnceApplyStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	if s.fail {
		s.fail = false
		return errSubstrateBoom
	}
	return s.BlockStore.ApplyEntry(lba, data, lsn)
}

func TestApplyGate_FailedWriteDoesNotPublishReplicaVersion(t *testing.T) {
	store := &failOnceApplyStore{BlockStore: storage.NewBlockStore(64, blockSize), fail: true}
	gate := NewReplicaApplyGate(store)
	if err := gate.ApplyLive(liveLineage(1), 5, makeData(0xBB), 120); !errors.Is(err, errSubstrateBoom) {
		t.Fatalf("physical error lost: %v", err)
	}
	want := makeData(0xAA)
	if err := gate.ApplyRecovery(recoveryLineage(2, 120), 5, want, 95); err != nil {
		t.Fatal(err)
	}
	got, err := store.Read(5)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("failed write falsely blocked recovery: err=%v", err)
	}
}
