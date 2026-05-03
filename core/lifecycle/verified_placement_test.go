package lifecycle

import (
	"testing"
	"time"
)

func TestG9F_PlacementIntentWithoutObservation_DoesNotVerify(t *testing.T) {
	intent := PlacementIntent{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Slots: []PlacementSlotIntent{{
			ServerID: "node-a",
			PoolID:   "pool-a",
			Source:   PlacementSourceBlankPool,
		}},
	}
	got := VerifyPlacementIntent(intent, nil, VerificationConfig{Now: time.Now(), FreshnessWindow: time.Second})
	if got.Verified {
		t.Fatalf("placement intent without observation verified: %+v", got)
	}
	if got.Reason != VerifyReasonMissingObservation {
		t.Fatalf("reason=%q want %q", got.Reason, VerifyReasonMissingObservation)
	}
}

func TestG9F_PlacementIntentWithStaleObservation_DoesNotVerify(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	intent := PlacementIntent{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Slots: []PlacementSlotIntent{{
			ServerID: "node-a",
			PoolID:   "pool-a",
			Source:   PlacementSourceBlankPool,
		}},
	}
	got := VerifyPlacementIntent(intent, []NodeRegistration{{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		SeenAt:   now.Add(-2 * time.Minute),
	}}, VerificationConfig{Now: now, FreshnessWindow: time.Minute})
	if got.Verified {
		t.Fatalf("stale observation verified: %+v", got)
	}
	if got.Reason != VerifyReasonStaleObservation {
		t.Fatalf("reason=%q want %q", got.Reason, VerifyReasonStaleObservation)
	}
}

func TestG9F_VerifiedPlacement_IsNotAuthorityShaped(t *testing.T) {
	for _, name := range []string{"VerifiedPlacement", "VerifiedPlacementSlot"} {
		typ := mustParseStruct(t, "verified_placement.go", name)
		for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy", "Primary"} {
			if _, ok := typ.Fields[forbidden]; ok {
				t.Fatalf("%s must not carry %s", name, forbidden)
			}
		}
	}
}

func TestG9F_BlankPoolIntentWithFreshObservation_VerifiesCreateNeededSlot(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	intent := PlacementIntent{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Slots: []PlacementSlotIntent{{
			ServerID: "node-a",
			PoolID:   "pool-a",
			Source:   PlacementSourceBlankPool,
		}},
	}
	got := VerifyPlacementIntent(intent, []NodeRegistration{{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		SeenAt:   now,
	}}, VerificationConfig{Now: now, FreshnessWindow: time.Minute})
	if !got.Verified {
		t.Fatalf("fresh blank-pool intent did not verify: %+v", got)
	}
	if len(got.Slots) != 1 {
		t.Fatalf("slot count=%d want 1", len(got.Slots))
	}
	slot := got.Slots[0]
	if slot.Source != PlacementSourceBlankPool || slot.PoolID != "pool-a" || slot.ReplicaID != "" {
		t.Fatalf("verified blank slot mismatch: %+v", slot)
	}
	if slot.DataAddr == "" || slot.CtrlAddr == "" {
		t.Fatalf("verified slot missing addresses: %+v", slot)
	}
}

func TestG9F_ExistingReplicaIntentWithWrongInventory_DoesNotVerify(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	intent := PlacementIntent{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Slots: []PlacementSlotIntent{{
			ServerID:  "node-a",
			ReplicaID: "r2",
			Source:    PlacementSourceExistingReplica,
		}},
	}
	got := VerifyPlacementIntent(intent, []NodeRegistration{{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		SeenAt:   now,
		Replicas: []ReplicaInventory{{
			VolumeID:  "vol-a",
			ReplicaID: "r3",
			StoreUUID: "store-1",
			SizeBytes: 1 << 20,
			State:     "existing",
		}},
	}}, VerificationConfig{Now: now, FreshnessWindow: time.Minute})
	if got.Verified {
		t.Fatalf("wrong replica inventory verified: %+v", got)
	}
	if got.Reason != VerifyReasonReplicaMismatch {
		t.Fatalf("reason=%q want %q", got.Reason, VerifyReasonReplicaMismatch)
	}
}
