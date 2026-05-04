// G5-5A unit test for ObservationStore.SlotFact accessor.

package authority

import (
	"testing"
	"time"
)

func TestObservationStore_SlotFact_ReturnsLatestForReplica(t *testing.T) {
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	s := NewObservationStore(FreshnessConfig{FreshnessWindow: 30 * time.Second}, clock)

	// One server reports a heartbeat for v1/r2 with a usable DataAddr.
	if err := s.Ingest(Observation{
		ServerID:   "m02-replica",
		ObservedAt: now,
		Slots: []SlotFact{
			{VolumeID: "v1", ReplicaID: "r2", DataAddr: "192.168.1.184:9221", CtrlAddr: "192.168.1.184:9211"},
		},
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	got, ok := s.SlotFact("v1", "r2")
	if !ok {
		t.Fatal("SlotFact: ok=false; want true after ingest")
	}
	if got.DataAddr != "192.168.1.184:9221" || got.CtrlAddr != "192.168.1.184:9211" {
		t.Errorf("SlotFact: got %+v", got)
	}
}

func TestObservationStore_SlotFact_NoObservation_ReturnsFalse(t *testing.T) {
	s := NewObservationStore(FreshnessConfig{}, nil)
	if _, ok := s.SlotFact("v1", "r2"); ok {
		t.Fatal("SlotFact: ok=true on empty store; want false")
	}
}

func TestObservationStore_SlotFact_EmptyDataAddr_FailsClosed(t *testing.T) {
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	s := NewObservationStore(FreshnessConfig{FreshnessWindow: 30 * time.Second}, func() time.Time { return now })
	// Heartbeat names the slot but reports empty DataAddr.
	if err := s.Ingest(Observation{
		ServerID:   "m02-replica",
		ObservedAt: now,
		Slots: []SlotFact{
			{VolumeID: "v1", ReplicaID: "r2", DataAddr: ""},
		},
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, ok := s.SlotFact("v1", "r2"); ok {
		t.Fatal("SlotFact: ok=true with empty DataAddr; want false (fail-closed)")
	}
}

func TestObservationStore_SlotFact_LatestObservationWins(t *testing.T) {
	t0 := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	now := t0
	s := NewObservationStore(FreshnessConfig{FreshnessWindow: 1 * time.Hour}, func() time.Time { return now })
	// Two reports for v1/r2 from different servers (rare but possible
	// during reassignment / topology confusion). Latest ObservedAt wins.
	_ = s.Ingest(Observation{
		ServerID:   "src-a",
		ObservedAt: t0,
		Slots:      []SlotFact{{VolumeID: "v1", ReplicaID: "r2", DataAddr: "OLD:1"}},
	})
	_ = s.Ingest(Observation{
		ServerID:   "src-b",
		ObservedAt: t0.Add(5 * time.Second),
		Slots:      []SlotFact{{VolumeID: "v1", ReplicaID: "r2", DataAddr: "NEW:1"}},
	})
	got, ok := s.SlotFact("v1", "r2")
	if !ok {
		t.Fatal("SlotFact: ok=false")
	}
	if got.DataAddr != "NEW:1" {
		t.Errorf("DataAddr: got %q want NEW:1 (latest ObservedAt wins)", got.DataAddr)
	}
}

// TestObservationStore_SlotFact_ExpiredObservation_FailsClosed pins
// architect round 54 finding-1: expired heartbeat addrs MUST NOT be
// used for live replication peer construction. Observation stays
// visible in the store (other diagnostic surfaces use it), but
// SlotFact's narrow contract is "give me an addr safe to dial NOW".
func TestObservationStore_SlotFact_ExpiredObservation_FailsClosed(t *testing.T) {
	t0 := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	now := t0
	clock := func() time.Time { return now }
	s := NewObservationStore(FreshnessConfig{FreshnessWindow: 10 * time.Second}, clock)

	// Ingest at t0 (ExpiresAt = t0+10s).
	if err := s.Ingest(Observation{
		ServerID:   "src",
		ObservedAt: t0,
		Slots:      []SlotFact{{VolumeID: "v1", ReplicaID: "r2", DataAddr: "192.168.1.184:9221"}},
	}); err != nil {
		t.Fatal(err)
	}

	// Within freshness window: ok=true.
	now = t0.Add(5 * time.Second)
	if _, ok := s.SlotFact("v1", "r2"); !ok {
		t.Fatal("within freshness window: ok=false; want true")
	}

	// Past freshness window: must fail closed.
	now = t0.Add(11 * time.Second)
	if got, ok := s.SlotFact("v1", "r2"); ok {
		t.Fatalf("after expiry: ok=true got %+v; want false (fail-closed on expired heartbeat)", got)
	}
}

func TestObservationStore_SlotFact_VolumeOrReplicaMismatch_ReturnsFalse(t *testing.T) {
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	s := NewObservationStore(FreshnessConfig{FreshnessWindow: 30 * time.Second}, func() time.Time { return now })
	_ = s.Ingest(Observation{
		ServerID:   "src",
		ObservedAt: now,
		Slots:      []SlotFact{{VolumeID: "v1", ReplicaID: "r2", DataAddr: "ADDR:1"}},
	})
	if _, ok := s.SlotFact("vOTHER", "r2"); ok {
		t.Errorf("expected false for volume mismatch")
	}
	if _, ok := s.SlotFact("v1", "rOTHER"); ok {
		t.Errorf("expected false for replica mismatch")
	}
	if _, ok := s.SlotFact("", "r2"); ok {
		t.Errorf("expected false for empty volume")
	}
	if _, ok := s.SlotFact("v1", ""); ok {
		t.Errorf("expected false for empty replica")
	}
}
