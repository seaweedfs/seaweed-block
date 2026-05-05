package lifecycle

import "testing"

func TestG15d_WorkloadPlan_BlankPoolRF2CreatesReplicaWorkloads(t *testing.T) {
	volume := VolumeRecord{Spec: VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
		PVCName:           "demo-pvc",
		PVCNamespace:      "default",
		PVCUID:            "uid-123",
	}}
	placement := PlacementIntent{
		VolumeID:  "pvc-a",
		DesiredRF: 2,
		Slots: []PlacementSlotIntent{
			{ServerID: "node-a", PoolID: "pool-a", Source: PlacementSourceBlankPool},
			{ServerID: "node-b", PoolID: "pool-b", Source: PlacementSourceBlankPool},
		},
	}
	plan, err := PlanBlockVolumeWorkloads(volume, placement, []NodeRegistration{
		nodeForWorkload("node-a", "10.0.0.1:9201", "10.0.0.1:9101"),
		nodeForWorkload("node-b", "10.0.0.2:9201", "10.0.0.2:9101"),
	}, WorkloadPlanConfig{ISCSIPortBase: 3260, IQNPrefix: "iqn.test"})
	if err != nil {
		t.Fatalf("PlanBlockVolumeWorkloads: %v", err)
	}
	if plan.VolumeID != "pvc-a" || plan.SizeBytes != 1<<20 || len(plan.Replicas) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	if plan.PVCName != "demo-pvc" || plan.PVCNamespace != "default" || plan.PVCUID != "uid-123" {
		t.Fatalf("pvc metadata=%+v", plan)
	}
	if plan.Replicas[0].ReplicaID != "r1" || plan.Replicas[1].ReplicaID != "r2" {
		t.Fatalf("replica ids=%q/%q want r1/r2", plan.Replicas[0].ReplicaID, plan.Replicas[1].ReplicaID)
	}
	if plan.Replicas[0].ISCSIListenPort != 3260 || plan.Replicas[1].ISCSIListenPort != 3261 {
		t.Fatalf("iscsi ports=%d/%d", plan.Replicas[0].ISCSIListenPort, plan.Replicas[1].ISCSIListenPort)
	}
	if plan.Replicas[0].ISCSIQualifiedName != "iqn.test:pvc-a" {
		t.Fatalf("iqn=%q", plan.Replicas[0].ISCSIQualifiedName)
	}
}

func TestG15d_WorkloadPlan_ExistingReplicaKeepsReplicaID(t *testing.T) {
	volume := VolumeRecord{Spec: VolumeSpec{VolumeID: "pvc-a", SizeBytes: 1 << 20, ReplicationFactor: 1}}
	placement := PlacementIntent{
		VolumeID:  "pvc-a",
		DesiredRF: 1,
		Slots: []PlacementSlotIntent{{
			ServerID:  "node-a",
			ReplicaID: "r9",
			Source:    PlacementSourceExistingReplica,
		}},
	}
	plan, err := PlanBlockVolumeWorkloads(volume, placement, []NodeRegistration{
		nodeForWorkload("node-a", "10.0.0.1:9201", "10.0.0.1:9101"),
	}, WorkloadPlanConfig{})
	if err != nil {
		t.Fatalf("PlanBlockVolumeWorkloads: %v", err)
	}
	if got := plan.Replicas[0].ReplicaID; got != "r9" {
		t.Fatalf("replica id=%q want r9", got)
	}
	if got := plan.Replicas[0].ISCSIListenPort; got != 3260 {
		t.Fatalf("default iscsi port=%d want 3260", got)
	}
}

func TestG15d_WorkloadPlan_IsIdempotentForSameInputs(t *testing.T) {
	volume := VolumeRecord{Spec: VolumeSpec{VolumeID: "pvc-a", SizeBytes: 1 << 20, ReplicationFactor: 1}}
	placement := PlacementIntent{
		VolumeID:  "pvc-a",
		DesiredRF: 1,
		Slots: []PlacementSlotIntent{{
			ServerID: "node-a",
			PoolID:   "pool-a",
			Source:   PlacementSourceBlankPool,
		}},
	}
	nodes := []NodeRegistration{nodeForWorkload("node-a", "10.0.0.1:9201", "10.0.0.1:9101")}
	first, err := PlanBlockVolumeWorkloads(volume, placement, nodes, WorkloadPlanConfig{})
	if err != nil {
		t.Fatalf("first plan: %v", err)
	}
	second, err := PlanBlockVolumeWorkloads(volume, placement, nodes, WorkloadPlanConfig{})
	if err != nil {
		t.Fatalf("second plan: %v", err)
	}
	if first.Replicas[0] != second.Replicas[0] {
		t.Fatalf("non-idempotent first=%+v second=%+v", first, second)
	}
}

func TestG15d_MaterializePlacementFromWorkloadPlan_BlankPoolBecomesReplicaSlots(t *testing.T) {
	placement := PlacementIntent{
		VolumeID:  "pvc-a",
		DesiredRF: 2,
		Slots: []PlacementSlotIntent{
			{ServerID: "node-a", PoolID: "pool-a", Source: PlacementSourceBlankPool},
			{ServerID: "node-b", PoolID: "pool-b", Source: PlacementSourceBlankPool},
		},
	}
	plan := BlockVolumeWorkloadPlan{
		VolumeID: "pvc-a",
		Replicas: []BlockVolumeReplicaWorkload{
			{ServerID: "node-a", ReplicaID: "r1"},
			{ServerID: "node-b", ReplicaID: "r2"},
		},
	}
	got, err := MaterializePlacementFromWorkloadPlan(placement, plan)
	if err != nil {
		t.Fatalf("MaterializePlacementFromWorkloadPlan: %v", err)
	}
	if got.Slots[0].ReplicaID != "r1" || got.Slots[1].ReplicaID != "r2" {
		t.Fatalf("replica ids=%q/%q want r1/r2", got.Slots[0].ReplicaID, got.Slots[1].ReplicaID)
	}
	for _, slot := range got.Slots {
		if slot.Source != PlacementSourceExistingReplica {
			t.Fatalf("slot=%+v want existing_replica after materialization", slot)
		}
	}
	if placement.Slots[0].ReplicaID != "" || placement.Slots[0].Source != PlacementSourceBlankPool {
		t.Fatalf("input mutated: %+v", placement.Slots[0])
	}
}

func TestG15d_MaterializePlacementFromWorkloadPlan_FailsOnMismatchedServer(t *testing.T) {
	_, err := MaterializePlacementFromWorkloadPlan(
		PlacementIntent{
			VolumeID:  "pvc-a",
			DesiredRF: 1,
			Slots:     []PlacementSlotIntent{{ServerID: "node-a", Source: PlacementSourceBlankPool}},
		},
		BlockVolumeWorkloadPlan{
			VolumeID: "pvc-a",
			Replicas: []BlockVolumeReplicaWorkload{{
				ServerID:  "node-b",
				ReplicaID: "r1",
			}},
		},
	)
	if err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestG15d_WorkloadPlan_IsNotAuthorityShaped(t *testing.T) {
	for _, name := range []string{"BlockVolumeWorkloadPlan", "BlockVolumeReplicaWorkload"} {
		typ := mustParseStruct(t, "workload_plan.go", name)
		for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy", "Primary"} {
			if _, ok := typ.Fields[forbidden]; ok {
				t.Fatalf("%s must not carry %s", name, forbidden)
			}
		}
	}
}

func nodeForWorkload(serverID, dataAddr, ctrlAddr string) NodeRegistration {
	return NodeRegistration{
		ServerID: serverID,
		DataAddr: dataAddr,
		CtrlAddr: ctrlAddr,
	}
}
