package master

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
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

func TestG15d_WorkloadPlanTickAllocatesDistinctNodeLocalPortsAcrossVolumes(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	for _, volumeID := range []string{"pvc-a", "pvc-b"} {
		if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
			VolumeID:          volumeID,
			SizeBytes:         1 << 20,
			ReplicationFactor: 1,
		}); err != nil {
			t.Fatalf("create volume %s: %v", volumeID, err)
		}
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m02",
		DataAddr: "127.0.0.1:19101",
		CtrlAddr: "127.0.0.1:19102",
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

	result, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{
		ISCSIPortBase: 3260,
		NVMePortBase:  4420,
	})
	if err != nil {
		t.Fatalf("workload plan tick: %v", err)
	}
	if len(result.Plans) != 2 {
		t.Fatalf("plans=%d want 2", len(result.Plans))
	}
	first := result.Plans[0].Replicas[0]
	second := result.Plans[1].Replicas[0]
	if first.ISCSIListenPort != 3260 || second.ISCSIListenPort != 3261 {
		t.Fatalf("iscsi ports=%d/%d want 3260/3261", first.ISCSIListenPort, second.ISCSIListenPort)
	}
	if first.NVMeListenPort != 4420 || second.NVMeListenPort != 4421 {
		t.Fatalf("nvme ports=%d/%d want 4420/4421", first.NVMeListenPort, second.NVMeListenPort)
	}
	if first.DataAddr != "127.0.0.1:19101" || first.CtrlAddr != "127.0.0.1:19102" {
		t.Fatalf("first addrs=%s/%s want original node addrs", first.DataAddr, first.CtrlAddr)
	}
	if second.DataAddr != "127.0.0.1:19103" || second.CtrlAddr != "127.0.0.1:19104" {
		t.Fatalf("second addrs=%s/%s want shifted node addrs", second.DataAddr, second.CtrlAddr)
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("pvc-a"); ok {
		t.Fatal("workload planning must not mint authority for pvc-a")
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("pvc-b"); ok {
		t.Fatal("workload planning must not mint authority for pvc-b")
	}
}

func TestG15d_WorkloadPlanTickPreservesMaterializedPortsWhenVolumeIDsSortEarlier(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-c",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create first volume: %v", err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m02",
		DataAddr: "127.0.0.1:19101",
		CtrlAddr: "127.0.0.1:19102",
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
	firstResult, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{
		ISCSIPortBase: 3260,
		NVMePortBase:  4420,
	})
	if err != nil {
		t.Fatalf("first workload tick: %v", err)
	}
	if got := firstResult.Plans[0].Replicas[0].ISCSIListenPort; got != 3260 {
		t.Fatalf("first volume port=%d want 3260", got)
	}

	for _, volumeID := range []string{"pvc-a", "pvc-b"} {
		if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
			VolumeID:          volumeID,
			SizeBytes:         1 << 20,
			ReplicationFactor: 1,
		}); err != nil {
			t.Fatalf("create later volume %s: %v", volumeID, err)
		}
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("second product tick: %v", err)
	}
	secondResult, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{
		ISCSIPortBase: 3260,
		NVMePortBase:  4420,
	})
	if err != nil {
		t.Fatalf("second workload tick: %v", err)
	}
	portsByVolume := map[string]int{}
	dataByVolume := map[string]string{}
	for _, plan := range secondResult.Plans {
		portsByVolume[plan.VolumeID] = plan.Replicas[0].ISCSIListenPort
		dataByVolume[plan.VolumeID] = plan.Replicas[0].DataAddr
	}
	if portsByVolume["pvc-c"] != 3260 {
		t.Fatalf("materialized pvc-c port moved to %d; want preserved 3260", portsByVolume["pvc-c"])
	}
	seenPorts := map[int]string{}
	seenData := map[string]string{}
	for volumeID, port := range portsByVolume {
		if prior := seenPorts[port]; prior != "" {
			t.Fatalf("duplicate port %d for %s and %s: %+v", port, prior, volumeID, portsByVolume)
		}
		seenPorts[port] = volumeID
	}
	for volumeID, data := range dataByVolume {
		if prior := seenData[data]; prior != "" {
			t.Fatalf("duplicate data addr %s for %s and %s: %+v", data, prior, volumeID, dataByVolume)
		}
		seenData[data] = volumeID
	}
}

func TestMountedFailover_WorkloadPlanSupportsLogicalServersOnOneKubernetesNode(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-rf2",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	for _, reg := range []lifecycle.NodeRegistration{
		{
			ServerID: "m02-r1",
			DataAddr: "127.0.0.1:19101",
			CtrlAddr: "127.0.0.1:19102",
			Labels: map[string]string{
				lifecycle.KubernetesNodeNameLabel: "m02",
			},
			Pools: []lifecycle.StoragePool{{
				PoolID:     "default-r1",
				TotalBytes: 1 << 30,
				FreeBytes:  1 << 30,
				BlockSize:  4096,
			}},
		},
		{
			ServerID: "m02-r2",
			DataAddr: "127.0.0.1:19103",
			CtrlAddr: "127.0.0.1:19104",
			Labels: map[string]string{
				lifecycle.KubernetesNodeNameLabel: "m02",
			},
			Pools: []lifecycle.StoragePool{{
				PoolID:     "default-r2",
				TotalBytes: 1 << 30,
				FreeBytes:  1 << 30,
				BlockSize:  4096,
			}},
		},
	} {
		if _, err := stores.Nodes.RegisterNode(reg); err != nil {
			t.Fatalf("register node %s: %v", reg.ServerID, err)
		}
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	result, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{
		ISCSIPortBase: 3260,
		NVMePortBase:  4420,
	})
	if err != nil {
		t.Fatalf("workload plan tick: %v", err)
	}
	if len(result.Plans) != 1 || len(result.Plans[0].Replicas) != 2 {
		t.Fatalf("plans=%+v want one RF2 workload plan", result.Plans)
	}
	for _, replica := range result.Plans[0].Replicas {
		if replica.KubernetesNodeName != "m02" {
			t.Fatalf("replica %s kube node=%q want m02", replica.ReplicaID, replica.KubernetesNodeName)
		}
		if replica.ServerID == replica.KubernetesNodeName {
			t.Fatalf("replica %s collapsed server identity into k8s node name", replica.ReplicaID)
		}
	}
}

func TestMountedFailover_WorkloadPlanAllocatesPortsByKubernetesNode(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	for _, volumeID := range []string{"pvc-rf2-a", "pvc-rf2-b"} {
		if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
			VolumeID:          volumeID,
			SizeBytes:         1 << 20,
			ReplicationFactor: 2,
		}); err != nil {
			t.Fatalf("create volume %s: %v", volumeID, err)
		}
	}
	for _, reg := range []lifecycle.NodeRegistration{
		{
			ServerID: "m02-r1",
			DataAddr: "127.0.0.1:19101",
			CtrlAddr: "127.0.0.1:19102",
			Labels:   map[string]string{lifecycle.KubernetesNodeNameLabel: "m02"},
			Pools: []lifecycle.StoragePool{{
				PoolID: "default-r1", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096,
			}},
		},
		{
			ServerID: "m02-r2",
			DataAddr: "127.0.0.1:19103",
			CtrlAddr: "127.0.0.1:19104",
			Labels:   map[string]string{lifecycle.KubernetesNodeNameLabel: "m02"},
			Pools: []lifecycle.StoragePool{{
				PoolID: "default-r2", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096,
			}},
		},
	} {
		if _, err := stores.Nodes.RegisterNode(reg); err != nil {
			t.Fatalf("register node %s: %v", reg.ServerID, err)
		}
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	result, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{
		ISCSIPortBase: 3260,
		NVMePortBase:  4420,
	})
	if err != nil {
		t.Fatalf("workload plan tick: %v", err)
	}
	var ports []int
	for _, plan := range result.Plans {
		for _, replica := range plan.Replicas {
			ports = append(ports, replica.ISCSIListenPort)
			if replica.KubernetesNodeName != "m02" {
				t.Fatalf("replica %s kube node=%q want m02", replica.ReplicaID, replica.KubernetesNodeName)
			}
		}
	}
	if len(ports) != 4 {
		t.Fatalf("ports=%v want four RF2 replicas across two volumes", ports)
	}
	want := map[int]bool{3260: true, 3261: true, 3262: true, 3263: true}
	for _, port := range ports {
		if !want[port] {
			t.Fatalf("unexpected or duplicate host-network port %d in %v", port, ports)
		}
		delete(want, port)
	}
	if len(want) != 0 {
		t.Fatalf("missing expected ports: %v from got %v", want, ports)
	}
}

func TestMountedFailover_WorkloadPlanAllocatesPortsByPhysicalNodeAcrossRF2Volumes(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	for _, volumeID := range []string{"pvc-rf2-a", "pvc-rf2-b"} {
		if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
			VolumeID:          volumeID,
			SizeBytes:         1 << 20,
			ReplicationFactor: 2,
		}); err != nil {
			t.Fatalf("create volume %s: %v", volumeID, err)
		}
	}
	for _, reg := range []lifecycle.NodeRegistration{
		{
			ServerID: "node-a",
			DataAddr: "10.0.0.1:19101",
			CtrlAddr: "10.0.0.1:19102",
			Labels:   map[string]string{lifecycle.KubernetesNodeNameLabel: "k8s-a"},
			Pools: []lifecycle.StoragePool{{
				PoolID: "pool-a", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096,
			}},
		},
		{
			ServerID: "node-b",
			DataAddr: "10.0.0.2:19101",
			CtrlAddr: "10.0.0.2:19102",
			Labels:   map[string]string{lifecycle.KubernetesNodeNameLabel: "k8s-b"},
			Pools: []lifecycle.StoragePool{{
				PoolID: "pool-b", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096,
			}},
		},
	} {
		if _, err := stores.Nodes.RegisterNode(reg); err != nil {
			t.Fatalf("register node %s: %v", reg.ServerID, err)
		}
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	result, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{
		ISCSIPortBase: 3260,
		NVMePortBase:  4420,
	})
	if err != nil {
		t.Fatalf("workload plan tick: %v", err)
	}
	portsByNode := map[string]map[int]bool{}
	dataByNode := map[string]map[string]bool{}
	for _, plan := range result.Plans {
		for _, replica := range plan.Replicas {
			node := replica.KubernetesNodeName
			if portsByNode[node] == nil {
				portsByNode[node] = map[int]bool{}
				dataByNode[node] = map[string]bool{}
			}
			if portsByNode[node][replica.ISCSIListenPort] {
				t.Fatalf("duplicate iscsi port %d on node %s", replica.ISCSIListenPort, node)
			}
			portsByNode[node][replica.ISCSIListenPort] = true
			if dataByNode[node][replica.DataAddr] {
				t.Fatalf("duplicate data addr %s on node %s", replica.DataAddr, node)
			}
			dataByNode[node][replica.DataAddr] = true
		}
	}
	for _, node := range []string{"k8s-a", "k8s-b"} {
		for _, want := range []int{3260, 3261} {
			if !portsByNode[node][want] {
				t.Fatalf("node %s ports=%v missing %d", node, portsByNode[node], want)
			}
		}
	}
}

func TestMountedFailover_ProductTickPreservesMaterializedRF2Placement(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 2,
	})
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-rf2",
		SizeBytes:         1 << 20,
		ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	for _, reg := range []lifecycle.NodeRegistration{
		{
			ServerID: "m02-r1",
			DataAddr: "127.0.0.1:19101",
			CtrlAddr: "127.0.0.1:19102",
			Labels:   map[string]string{lifecycle.KubernetesNodeNameLabel: "m02"},
			Pools: []lifecycle.StoragePool{{
				PoolID: "default-r1", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096,
			}},
		},
		{
			ServerID: "m02-r2",
			DataAddr: "127.0.0.1:19103",
			CtrlAddr: "127.0.0.1:19104",
			Labels:   map[string]string{lifecycle.KubernetesNodeNameLabel: "m02"},
			Pools: []lifecycle.StoragePool{{
				PoolID: "default-r2", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096,
			}},
		},
	} {
		if _, err := stores.Nodes.RegisterNode(reg); err != nil {
			t.Fatalf("register node %s: %v", reg.ServerID, err)
		}
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if _, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{}); err != nil {
		t.Fatalf("workload plan tick: %v", err)
	}
	before, ok := stores.Placements.GetPlacement("pvc-rf2")
	if !ok {
		t.Fatal("missing materialized placement")
	}
	for _, slot := range before.Slots {
		if slot.Source != lifecycle.PlacementSourceExistingReplica || slot.ReplicaID == "" {
			t.Fatalf("placement not materialized before product tick: %+v", before)
		}
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("second product tick: %v", err)
	}
	after, ok := stores.Placements.GetPlacement("pvc-rf2")
	if !ok {
		t.Fatal("placement disappeared")
	}
	for _, slot := range after.Slots {
		if slot.Source != lifecycle.PlacementSourceExistingReplica || slot.ReplicaID == "" {
			t.Fatalf("product tick overwrote materialized placement: before=%+v after=%+v", before, after)
		}
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
