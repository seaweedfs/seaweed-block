package master

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestG15d_WorkloadPlanTickMaterializesBlankPoolPlacement(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m02",
		DataAddr: "10.0.0.2:9201",
		CtrlAddr: "10.0.0.2:9101",
		Pools: []lifecycle.StoragePool{{
			PoolID:     "default",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	before, ok := stores.Placements.GetPlacement("pvc-a")
	if !ok {
		t.Fatal("placement not reconciled")
	}
	if before.Slots[0].Source != lifecycle.PlacementSourceBlankPool || before.Slots[0].ReplicaID != "" {
		t.Fatalf("before=%+v want blank pool without replica id", before.Slots[0])
	}

	result, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{ISCSIPortBase: 3260})
	if err != nil {
		t.Fatalf("workload plan tick: %v", err)
	}
	if result.PlannedVolumes != 1 || result.MaterializedPlacements != 1 || len(result.Plans) != 1 {
		t.Fatalf("result=%+v want one planned+materialized volume", result)
	}
	if got := result.Plans[0].Replicas[0].ReplicaID; got != "r1" {
		t.Fatalf("planned replica=%q want r1", got)
	}
	after, ok := stores.Placements.GetPlacement("pvc-a")
	if !ok {
		t.Fatal("placement disappeared")
	}
	if after.Slots[0].Source != lifecycle.PlacementSourceExistingReplica || after.Slots[0].ReplicaID != "r1" {
		t.Fatalf("after=%+v want materialized existing replica r1", after.Slots[0])
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("pvc-a"); ok {
		t.Fatal("workload planning/materialization must not mint authority")
	}
}

func TestG15d_ReplicaSlotsForFallsBackToLifecyclePlacement(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	if _, err := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "pvc-a",
		DesiredRF: 2,
		Candidates: []lifecycle.PlacementCandidate{
			{VolumeID: "pvc-a", ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica},
			{VolumeID: "pvc-a", ServerID: "node-b", ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica},
		},
	}); err != nil {
		t.Fatalf("apply placement: %v", err)
	}
	got := h.replicaSlotsFor("pvc-a")
	if len(got) != 2 || got[0] != "r1" || got[1] != "r2" {
		t.Fatalf("slots=%v want r1,r2 from lifecycle placement", got)
	}
}
