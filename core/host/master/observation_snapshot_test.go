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
	now := time.Now().UTC()
	for i, item := range []struct {
		serverID string
		replica  string
		ip       string
		ready    bool
	}{
		{serverID: "m01", replica: "r1", ip: "192.168.1.181", ready: r1Ready},
		{serverID: "m02", replica: "r2", ip: "192.168.1.184", ready: r2Ready},
		{serverID: "tp01", replica: "r3", ip: "192.168.1.188", ready: r3Ready},
	} {
		if !item.ready {
			continue
		}
		if err := h.ObservationHost().Ingest(authority.Observation{
			ServerID:   item.serverID,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: true, Eligible: true},
			Slots: []authority.SlotFact{{
				VolumeID:        "pvc-a",
				ReplicaID:       item.replica,
				DataAddr:        item.ip + ":19101",
				CtrlAddr:        item.ip + ":19102",
				Reachable:       true,
				ReadyForPrimary: true,
				Eligible:        true,
				EvidenceScore:   uint64(30 - i),
				Frontends: []authority.FrontendTargetFact{{
					Protocol: "iscsi",
					Addr:     item.ip + ":3260",
					IQN:      "iqn.2026-05.io.seaweedfs:pvc-a",
					LUN:      1,
				}},
			}},
		}); err != nil {
			t.Fatalf("ingest %s: %v", item.replica, err)
		}
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
