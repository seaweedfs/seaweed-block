package lifecycle

import (
	"errors"
	"testing"
)

func TestReconcilePlacement_HappyPathWritesPlacementIntent(t *testing.T) {
	volumes := []VolumeRecord{{Spec: VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}}}
	nodes := []NodeRegistration{
		nodeWithPool("node-a", "pool-a", 1<<30),
		nodeWithPool("node-b", "pool-b", 1<<30),
	}
	store, err := OpenPlacementIntentStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	results := ReconcilePlacement(volumes, nodes, store)
	if len(results) != 1 {
		t.Fatalf("result count=%d want 1", len(results))
	}
	if !results[0].Applied || results[0].Err != nil {
		t.Fatalf("result not applied: %+v", results[0])
	}
	intent, ok := store.GetPlacement("vol-a")
	if !ok {
		t.Fatal("placement intent not written")
	}
	if intent.DesiredRF != 2 || len(intent.Slots) != 2 {
		t.Fatalf("intent=%+v want RF=2 slots=2", intent)
	}
}

func TestReconcilePlacement_InsufficientInventoryReportsButDoesNotPersist(t *testing.T) {
	volumes := []VolumeRecord{{Spec: VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}}}
	nodes := []NodeRegistration{nodeWithPool("node-a", "pool-a", 1<<30)}
	store, err := OpenPlacementIntentStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	results := ReconcilePlacement(volumes, nodes, store)
	if len(results) != 1 {
		t.Fatalf("result count=%d want 1", len(results))
	}
	if results[0].Applied {
		t.Fatalf("insufficient plan must not apply: %+v", results[0])
	}
	if !errors.Is(results[0].Err, ErrInsufficientPlacementCandidates) {
		t.Fatalf("err=%v want ErrInsufficientPlacementCandidates", results[0].Err)
	}
	if _, ok := store.GetPlacement("vol-a"); ok {
		t.Fatal("insufficient reconcile must not persist placement")
	}
}

func TestReconcilePlacement_ExistingReplicaConflictIsReportOnly(t *testing.T) {
	volumes := []VolumeRecord{{Spec: VolumeSpec{
		VolumeID:          "vol-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}}}
	nodes := []NodeRegistration{{
		ServerID: "node-a",
		Addr:     "127.0.0.1:9101",
		Pools: []StoragePool{{
			PoolID:     "pool-a",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
		Replicas: []ReplicaInventory{{
			VolumeID:  "vol-a",
			ReplicaID: "r2",
			StoreUUID: "store-1",
			SizeBytes: 2 << 20,
			State:     "existing",
		}},
	}}
	store, err := OpenPlacementIntentStore(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	results := ReconcilePlacement(volumes, nodes, store)
	if len(results) != 1 {
		t.Fatalf("result count=%d want 1", len(results))
	}
	if results[0].Applied {
		t.Fatalf("conflict must not apply: %+v", results[0])
	}
	if len(results[0].Plan.Conflicts) != 1 {
		t.Fatalf("conflict count=%d want 1: %+v", len(results[0].Plan.Conflicts), results[0].Plan)
	}
	if _, ok := store.GetPlacement("vol-a"); ok {
		t.Fatal("conflicting existing replica must not persist placement")
	}
}

func TestReconcilePlacement_ResultIsNotAuthorityShaped(t *testing.T) {
	typ := mustParseStruct(t, "reconciler.go", "ReconcileResult")
	for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy", "Primary"} {
		if _, ok := typ.Fields[forbidden]; ok {
			t.Fatalf("ReconcileResult must not carry %s", forbidden)
		}
	}
}

func nodeWithPool(serverID, poolID string, freeBytes uint64) NodeRegistration {
	return NodeRegistration{
		ServerID: serverID,
		Addr:     "127.0.0.1:9100",
		Pools: []StoragePool{{
			PoolID:     poolID,
			TotalBytes: 1 << 30,
			FreeBytes:  freeBytes,
			BlockSize:  4096,
		}},
	}
}
