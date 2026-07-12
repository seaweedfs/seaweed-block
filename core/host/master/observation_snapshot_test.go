package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

func TestMasterObservationSnapshot_RF3HealthyReadOnly(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	lineBefore := waitAuthorityLine(t, h.Publisher(), "pvc-a")

	snapshot := h.ObservationSnapshot(time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC))
	lineAfter, ok := h.Publisher().VolumeAuthorityLine("pvc-a")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if lineAfter != lineBefore {
		t.Fatalf("observation snapshot mutated authority before=%+v after=%+v", lineBefore, lineAfter)
	}

	if snapshot.SchemaVersion != ops.ObservationSchemaVersion {
		t.Fatalf("schema=%q want %q", snapshot.SchemaVersion, ops.ObservationSchemaVersion)
	}
	if snapshot.Status != ops.ObservationStatusOK {
		t.Fatalf("cluster status=%q want ok: %+v", snapshot.Status, snapshot)
	}
	if len(snapshot.Nodes) != 3 {
		t.Fatalf("nodes=%d want 3", len(snapshot.Nodes))
	}
	if len(snapshot.Volumes) != 1 {
		t.Fatalf("volumes=%d want 1", len(snapshot.Volumes))
	}
	volume := snapshot.Volumes[0]
	if volume.VolumeID != "pvc-a" || volume.PVCName != "demo-pvc" || volume.Namespace != "default" {
		t.Fatalf("volume identity=%+v", volume)
	}
	if volume.ReplicationFactor != 3 || volume.DesiredReplicas != 3 || volume.ObservedReplicas != 3 {
		t.Fatalf("replica counts=%+v", volume)
	}
	if volume.PrimaryReplica != lineBefore.ReplicaID || volume.PrimaryNode == "" || volume.PublishTarget == "" {
		t.Fatalf("primary evidence=%+v line=%+v", volume, lineBefore)
	}
	if volume.Status != ops.ObservationStatusOK {
		t.Fatalf("volume status=%q want ok: %+v", volume.Status, volume)
	}
	if !hasReplicaWithRole(volume.Replicas, volume.PrimaryReplica, "primary") {
		t.Fatalf("replicas=%+v missing primary %s", volume.Replicas, volume.PrimaryReplica)
	}
}

func TestMasterObservationSnapshot_SeparatesManagementAndFrontendNodeAddress(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{
		ServerID: "m01",
		DataAddr: "10.0.0.181:19101",
		CtrlAddr: "192.168.1.181:19102",
		Labels: map[string]string{
			lifecycle.KubernetesNodeNameLabel:   "m01",
			lifecycle.ManagementIPLabel:         "192.168.1.181",
			lifecycle.FrontendIPLabel:           "10.0.0.181",
			lifecycle.FrontendNetworkClassLabel: "100gbe_tcp",
		},
		Pools: []lifecycle.StoragePool{{
			PoolID:     "pool-m01",
			TotalBytes: 1 << 30,
			FreeBytes:  1 << 30,
			BlockSize:  4096,
		}},
		SeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	snapshot := h.ObservationSnapshot(time.Now().UTC())
	if len(snapshot.Nodes) != 1 {
		t.Fatalf("nodes=%d want 1: %+v", len(snapshot.Nodes), snapshot.Nodes)
	}
	node := snapshot.Nodes[0]
	if node.InternalIP != "192.168.1.181" ||
		node.FrontendIP != "10.0.0.181" ||
		node.FrontendNetworkClass != "100gbe_tcp" {
		t.Fatalf("node address evidence=%+v", node)
	}
}

func TestMasterObservationSnapshot_MissingReplicaIsDegraded(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, false)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	snapshot := h.ObservationSnapshot(time.Now().UTC())
	if snapshot.Status != ops.ObservationStatusDegraded {
		t.Fatalf("cluster status=%q want degraded", snapshot.Status)
	}
	if len(snapshot.Volumes) != 1 {
		t.Fatalf("volumes=%d want 1", len(snapshot.Volumes))
	}
	volume := snapshot.Volumes[0]
	if volume.Status != ops.ObservationStatusDegraded {
		t.Fatalf("volume status=%q want degraded: %+v", volume.Status, volume)
	}
	if volume.Reason != ops.ReasonObservedReplicasBelowDesired {
		t.Fatalf("volume reason=%q want %q", volume.Reason, ops.ReasonObservedReplicasBelowDesired)
	}
	if volume.ObservedReplicas != 2 || volume.DesiredReplicas != 3 {
		t.Fatalf("replica counts observed=%d desired=%d", volume.ObservedReplicas, volume.DesiredReplicas)
	}
	if !hasReplicaCondition(volume.Replicas, "r3", ops.ReasonStatusEndpointUnreachable) {
		t.Fatalf("replicas=%+v missing r3 unreachable condition", volume.Replicas)
	}
}

func TestMasterObservationSnapshot_PrimaryMustConfirmReadiness(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "pvc-a")
	ingestObservationSnapshotReplica(t, h, line.ReplicaID, false)

	snapshot := h.ObservationSnapshot(time.Now().UTC())
	if snapshot.Status != ops.ObservationStatusDegraded {
		t.Fatalf("cluster status=%q want degraded", snapshot.Status)
	}
	if len(snapshot.Volumes) != 1 {
		t.Fatalf("volumes=%d want 1", len(snapshot.Volumes))
	}
	volume := snapshot.Volumes[0]
	if volume.Status == ops.ObservationStatusOK {
		t.Fatalf("volume incorrectly reported ok after primary readiness block: %+v", volume)
	}
	if volume.Reason != ops.ReasonNoPromotionReadyCandidate {
		t.Fatalf("volume reason=%q want %q: %+v", volume.Reason, ops.ReasonNoPromotionReadyCandidate, volume)
	}
	if !hasVolumeCondition(volume, "PrimaryReady", ops.ReasonNoPromotionReadyCandidate) {
		t.Fatalf("volume conditions=%+v missing primary readiness block", volume.Conditions)
	}
	if !hasReplicaNotReady(volume.Replicas, line.ReplicaID) {
		t.Fatalf("replicas=%+v missing not-ready primary %s", volume.Replicas, line.ReplicaID)
	}
}

func TestMasterObservationSnapshot_NoLifecycleStoreReturnsEmptyOK(t *testing.T) {
	h := newTestMaster(t, "")
	defer closeTestMaster(t, h)

	snapshot := h.ObservationSnapshot(time.Time{})
	if snapshot.Status != ops.ObservationStatusOK {
		t.Fatalf("status=%q want ok", snapshot.Status)
	}
	if len(snapshot.Volumes) != 0 || len(snapshot.Nodes) != 0 {
		t.Fatalf("snapshot=%+v want empty observation", snapshot)
	}
}

func TestMasterObservationSnapshot_PreservesNVMeFrontendIdentityInVolumeEvidence(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 1,
	})
	defer closeTestMaster(t, h)
	if _, err := h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-nvme",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
		Protocol:          "nvme",
		PVCName:           "nvme-pvc",
		PVCNamespace:      "default",
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, err := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "pvc-nvme",
		DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{
			VolumeID:  "pvc-nvme",
			ServerID:  "m01",
			ReplicaID: "r1",
			Source:    lifecycle.PlacementSourceExistingReplica,
		}},
	}); err != nil {
		t.Fatalf("apply placement: %v", err)
	}
	if err := h.ObservationHost().Ingest(authority.Observation{
		ServerID:   "m01",
		ObservedAt: time.Now().UTC(),
		Server:     authority.ServerFact{Reachable: true, Eligible: true},
		Slots: []authority.SlotFact{{
			VolumeID:        "pvc-nvme",
			ReplicaID:       "r1",
			DataAddr:        "192.168.1.181:19101",
			CtrlAddr:        "192.168.1.181:19102",
			Reachable:       true,
			ReadyForPrimary: true,
			Eligible:        true,
			Frontends: []authority.FrontendTargetFact{{
				Protocol: "nvme",
				Addr:     "127.0.0.1:4420",
				NQN:      "nqn.2026-05.io.seaweedfs:pvc-nvme",
				NSID:     1,
			}},
		}},
	}); err != nil {
		t.Fatalf("ingest nvme observation: %v", err)
	}

	snapshot := h.ObservationSnapshot(time.Now().UTC())
	if len(snapshot.Volumes) != 1 || len(snapshot.Volumes[0].Replicas) != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	replica := snapshot.Volumes[0].Replicas[0]
	if replica.FrontendProtocol != "nvme" ||
		replica.FrontendAddr != "127.0.0.1:4420" ||
		replica.FrontendNQN != "nqn.2026-05.io.seaweedfs:pvc-nvme" ||
		replica.FrontendNSID != 1 {
		t.Fatalf("replica frontend=%+v", replica)
	}
}

func seedObservationSnapshotVolume(t *testing.T, h *Host) {
	t.Helper()
	if _, err := h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 3,
		Protocol:          "iscsi",
		PVCName:           "demo-pvc",
		PVCNamespace:      "default",
		PVName:            "pv-a",
	}); err != nil {
		t.Fatalf("create volume: %v", err)
	}
	now := time.Now().UTC()
	for _, node := range []struct {
		serverID string
		ip       string
	}{
		{serverID: "m01", ip: "192.168.1.181"},
		{serverID: "m02", ip: "192.168.1.184"},
		{serverID: "tp01", ip: "192.168.1.188"},
	} {
		if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{
			ServerID: node.serverID,
			DataAddr: node.ip + ":19101",
			CtrlAddr: node.ip + ":19102",
			Labels: map[string]string{
				lifecycle.KubernetesNodeNameLabel: node.serverID,
			},
			Pools: []lifecycle.StoragePool{{
				PoolID:     "pool-" + node.serverID,
				TotalBytes: 1 << 30,
				FreeBytes:  1 << 30,
				BlockSize:  4096,
			}},
			SeenAt: now,
		}); err != nil {
			t.Fatalf("register node %s: %v", node.serverID, err)
		}
	}
}

func ingestObservationSnapshotRF3(t *testing.T, h *Host, r1Ready, r2Ready, r3Ready bool) {
	t.Helper()
	for _, item := range []struct {
		replica string
		ready   bool
	}{
		{replica: "r1", ready: r1Ready},
		{replica: "r2", ready: r2Ready},
		{replica: "r3", ready: r3Ready},
	} {
		if !item.ready {
			continue
		}
		ingestObservationSnapshotReplica(t, h, item.replica, true)
	}
}

func ingestObservationSnapshotReplica(t *testing.T, h *Host, replica string, ready bool) {
	t.Helper()
	item, ok := map[string]struct {
		serverID string
		ip       string
		score    uint64
	}{
		"r1": {serverID: "m01", ip: "192.168.1.181", score: 30},
		"r2": {serverID: "m02", ip: "192.168.1.184", score: 29},
		"r3": {serverID: "tp01", ip: "192.168.1.188", score: 28},
	}[replica]
	if !ok {
		t.Fatalf("unknown replica %s", replica)
	}
	if err := h.ObservationHost().Ingest(authority.Observation{
		ServerID:   item.serverID,
		ObservedAt: time.Now().UTC(),
		Server:     authority.ServerFact{Reachable: true, Eligible: true},
		Slots: []authority.SlotFact{{
			VolumeID:        "pvc-a",
			ReplicaID:       replica,
			DataAddr:        item.ip + ":19101",
			CtrlAddr:        item.ip + ":19102",
			Reachable:       true,
			ReadyForPrimary: ready,
			Eligible:        true,
			EvidenceScore:   item.score,
			Frontends: []authority.FrontendTargetFact{{
				Protocol: "iscsi",
				Addr:     item.ip + ":3260",
				IQN:      "iqn.2026-05.io.seaweedfs:pvc-a",
				LUN:      1,
			}},
		}},
	}); err != nil {
		t.Fatalf("ingest %s: %v", replica, err)
	}
}

func hasReplicaWithRole(replicas []ops.ReplicaEvidence, replicaID, role string) bool {
	for _, replica := range replicas {
		if replica.ReplicaID == replicaID && replica.Role == role {
			return true
		}
	}
	return false
}

func hasReplicaCondition(replicas []ops.ReplicaEvidence, replicaID, reason string) bool {
	for _, replica := range replicas {
		if replica.ReplicaID != replicaID {
			continue
		}
		for _, condition := range replica.Conditions {
			if condition.Reason == reason {
				return true
			}
		}
	}
	return false
}

func hasVolumeCondition(volume ops.VolumeEvidence, conditionType, reason string) bool {
	for _, condition := range volume.Conditions {
		if condition.Type == conditionType && condition.Reason == reason {
			return true
		}
	}
	return false
}

func hasReplicaNotReady(replicas []ops.ReplicaEvidence, replicaID string) bool {
	for _, replica := range replicas {
		if replica.ReplicaID == replicaID && replica.Observed && replica.Role == "primary" && !replica.CandidateReady {
			return true
		}
	}
	return false
}
