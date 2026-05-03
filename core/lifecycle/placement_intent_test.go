package lifecycle

import (
	"errors"
	"reflect"
	"testing"
)

func TestPlacementIntentStore_ApplyPlanPersistsSufficientPlan(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenPlacementIntentStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	plan := PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 2,
		Candidates: []PlacementCandidate{
			{VolumeID: "vol-a", ServerID: "node-a", PoolID: "pool-a", Source: PlacementSourceBlankPool},
			{VolumeID: "vol-a", ServerID: "node-b", ReplicaID: "r2", Source: PlacementSourceExistingReplica},
		},
	}
	intent, err := s.ApplyPlan(plan)
	if err != nil {
		t.Fatalf("apply plan: %v", err)
	}
	if intent.VolumeID != "vol-a" || intent.DesiredRF != 2 || len(intent.Slots) != 2 {
		t.Fatalf("intent mismatch: %+v", intent)
	}

	reopened, err := OpenPlacementIntentStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok := reopened.GetPlacement("vol-a")
	if !ok {
		t.Fatal("reopened store missing placement intent")
	}
	if !reflect.DeepEqual(got, intent) {
		t.Fatalf("reopened intent mismatch:\ngot  %+v\nwant %+v", got, intent)
	}
}

func TestPlacementIntentStore_RejectsInsufficientPlan(t *testing.T) {
	s, err := OpenPlacementIntentStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = s.ApplyPlan(PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 2,
		Candidates: []PlacementCandidate{
			{VolumeID: "vol-a", ServerID: "node-a", PoolID: "pool-a", Source: PlacementSourceBlankPool},
		},
	})
	if !errors.Is(err, ErrInsufficientPlacementCandidates) {
		t.Fatalf("err=%v want ErrInsufficientPlacementCandidates", err)
	}
	if _, ok := s.GetPlacement("vol-a"); ok {
		t.Fatal("insufficient plan must not persist placement intent")
	}
}

func TestPlacementIntentStore_UsesOnlyDesiredRFSlots(t *testing.T) {
	s, err := OpenPlacementIntentStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	intent, err := s.ApplyPlan(PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Candidates: []PlacementCandidate{
			{VolumeID: "vol-a", ServerID: "node-a", PoolID: "pool-a", Source: PlacementSourceBlankPool},
			{VolumeID: "vol-a", ServerID: "node-b", PoolID: "pool-b", Source: PlacementSourceBlankPool},
		},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(intent.Slots) != 1 || intent.Slots[0].ServerID != "node-a" {
		t.Fatalf("intent slots=%+v want only node-a", intent.Slots)
	}
}

func TestPlacementIntentStore_RejectsAuthorityShapedIntent(t *testing.T) {
	for _, name := range []string{"PlacementIntent", "PlacementSlotIntent"} {
		typ := mustParseStruct(t, "placement_intent.go", name)
		for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy", "Primary"} {
			if _, ok := typ.Fields[forbidden]; ok {
				t.Fatalf("%s must not carry %s", name, forbidden)
			}
		}
	}
}

func TestPlacementIntentStore_DeletePlacementIsIdempotent(t *testing.T) {
	s, err := OpenPlacementIntentStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.ApplyPlan(PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Candidates: []PlacementCandidate{
			{VolumeID: "vol-a", ServerID: "node-a", PoolID: "pool-a", Source: PlacementSourceBlankPool},
		},
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := s.DeletePlacement("vol-a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.DeletePlacement("vol-a"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	if _, ok := s.GetPlacement("vol-a"); ok {
		t.Fatal("deleted placement still present")
	}
}
