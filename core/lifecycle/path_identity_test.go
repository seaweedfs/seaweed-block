package lifecycle

import "testing"

func TestPhase175StorageIdentityComponentsRejectPathTraversal(t *testing.T) {
	for _, value := range []string{"", ".", "..", "../r1", "r1/../r2", `r1\..\master`} {
		if IsSafeStorageIdentityComponent(value) {
			t.Fatalf("unsafe component accepted: %q", value)
		}
	}
	for _, value := range []string{"pvc-a", "r1", "replica_2", "vol.3"} {
		if !IsSafeStorageIdentityComponent(value) {
			t.Fatalf("safe component rejected: %q", value)
		}
	}
}

func TestPhase175PlacementAndInventoryRejectUnsafeReplicaID(t *testing.T) {
	intent := PlacementIntent{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Slots: []PlacementSlotIntent{{
			ServerID: "m02", ReplicaID: "../../master", Source: PlacementSourceExistingReplica,
		}},
	}
	if err := validatePlacementIntent(intent); err == nil {
		t.Fatal("placement accepted unsafe replica id")
	}
	if err := validateReplicaInventory(ReplicaInventory{VolumeID: "vol-a", ReplicaID: "..", StoreUUID: "store-a", SizeBytes: 4096, State: "ready"}); err == nil {
		t.Fatal("inventory accepted unsafe replica id")
	}
	if err := validateVolumeID(".."); err == nil {
		t.Fatal("volume identity accepted parent traversal")
	}
}
