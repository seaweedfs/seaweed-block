package ops

import (
	"testing"
	"time"
)

func TestOperatorFoundationSnapshot_ReadOnlyBoundary(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 23, 20, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{healthyObservationVolume()}
	cluster.Cleanup = &CleanupEvidence{
		Status:                 "ok",
		KubernetesResidueCount: 0,
		MultipathResidueCount:  0,
		FailureCount:           0,
		EvidenceRef:            "cleanup-summary.txt",
	}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if !snapshot.ReadOnly || snapshot.Mutation.MutationAllowed {
		t.Fatalf("snapshot must be read-only: %+v", snapshot.Mutation)
	}
	if snapshot.APIVersion != SwBlockVolumeAPIVersion || snapshot.Kind != "ReadOnlyOperatorFoundationSnapshot" {
		t.Fatalf("snapshot identity=%s/%s", snapshot.APIVersion, snapshot.Kind)
	}
	if snapshot.CRDContract.Group != "block.seaweedfs.com" || len(snapshot.CRDContract.Resources) == 0 {
		t.Fatalf("missing CRD contract: %+v", snapshot.CRDContract)
	}
	if snapshot.Cluster.VolumeCount != 1 || snapshot.Cluster.ReadyVolumeCount != 1 || snapshot.Cluster.BlockedVolumeCount != 0 {
		t.Fatalf("cluster status=%+v", snapshot.Cluster)
	}
	if snapshot.Cluster.Cleanup == nil || snapshot.Cluster.Cleanup.Status != "ok" || snapshot.Cluster.Cleanup.EvidenceRef != "cleanup-summary.txt" {
		t.Fatalf("missing cleanup evidence: %+v", snapshot.Cluster.Cleanup)
	}
	if len(snapshot.Volumes) != 1 {
		t.Fatalf("volumes=%+v", snapshot.Volumes)
	}
	for _, action := range snapshot.Volumes[0].AllowedActions {
		if action.MutationAllowed {
			t.Fatalf("operator snapshot exposed mutating action: %+v", action)
		}
		if action.Mode != ManagedVolumeActionModeReadOnly && action.Mode != ManagedVolumeActionModeDryRun {
			t.Fatalf("operator snapshot exposed unsupported action mode: %+v", action)
		}
	}
}

func TestOperatorFoundationSnapshot_BlockedVolumeCarriesWarningEvent(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 23, 20, 0, 0, 0, time.UTC))
	cluster.Status = ObservationStatusBlocked
	cluster.Volumes = []VolumeEvidence{{
		VolumeID:       "pvc-loopback",
		Namespace:      "default",
		PVCName:        "demo-pvc",
		Status:         ObservationStatusBlocked,
		Reason:         ReasonPublishTargetLoopbackCrossNode,
		PrimaryReplica: "r1",
		PrimaryNode:    "m01",
		PublishTarget:  "127.0.0.1:3260",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
			FrontendAddr:   "127.0.0.1:3260",
		}},
	}}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.VolumeCount != 1 || snapshot.Cluster.BlockedVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", snapshot.Cluster)
	}
	if len(snapshot.Volumes) != 1 {
		t.Fatalf("volumes=%+v", snapshot.Volumes)
	}
	volume := snapshot.Volumes[0]
	if volume.Status.Status != ManagedVolumeStatusBlocked || volume.Status.ReasonCode != ReasonPublishTargetLoopbackCrossNode {
		t.Fatalf("volume status=%+v", volume.Status)
	}
	warning := false
	for _, event := range volume.Events {
		if event.Type == "Warning" && event.Reason == ReasonPublishTargetLoopbackCrossNode {
			warning = true
		}
	}
	if !warning {
		t.Fatalf("missing warning event: %+v", volume.Events)
	}
}

func TestOperatorFoundationSnapshot_CountsStaleEvidenceVolumes(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 25, 20, 0, 0, 0, time.UTC))
	cluster.ManagedVolumes = []ManagedVolumeProjection{ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:            "pvc-stale",
		EvidenceStale:       true,
		EvidenceStaleReason: ReasonEvidenceStale,
		EvidenceRefs:        []string{"product/unreachable.txt"},
	})}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.VolumeCount != 1 || snapshot.Cluster.StaleVolumeCount != 1 {
		t.Fatalf("cluster status=%+v", snapshot.Cluster)
	}
	found := false
	for _, condition := range snapshot.Cluster.Conditions {
		if condition.Type == ConditionEvidenceStale && condition.Status == "True" && condition.Reason == ReasonEvidenceStale {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing cluster EvidenceStale condition: %+v", snapshot.Cluster.Conditions)
	}
}

func TestOperatorFoundationSnapshot_IncludesNodeReadiness(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 6, 5, 16, 30, 0, 0, time.UTC))
	cluster.Nodes = []NodeEvidence{{
		NodeName:       "m02",
		KubernetesNode: "m02",
		InternalIP:     "192.168.1.184",
		Schedulable:    true,
		Ready:          true,
		ReplicaCount:   1,
		RequiredImages: []string{"sw-block:local"},
	}, {
		NodeName:       "tp01",
		KubernetesNode: "tp01",
		InternalIP:     "192.168.1.188",
		Schedulable:    true,
		Ready:          true,
		MissingImages:  []string{"sw-block-csi:local"},
	}}

	snapshot := BuildOperatorFoundationSnapshot(cluster)
	if snapshot.Cluster.NodeCount != 2 || len(snapshot.Cluster.Nodes) != 2 {
		t.Fatalf("node status=%+v", snapshot.Cluster)
	}
	ready := snapshot.Cluster.Nodes[0]
	if ready.Status != ManagedVolumeStatusReady || ready.ReasonCode != ReasonNodeReady {
		t.Fatalf("ready node=%+v", ready)
	}
	blocked := snapshot.Cluster.Nodes[1]
	if blocked.Status != ManagedVolumeStatusBlocked || blocked.ReasonCode != ReasonImageMissingOnNode {
		t.Fatalf("blocked node=%+v", blocked)
	}
	if len(blocked.MissingImages) != 1 {
		t.Fatalf("blocked missing images=%+v", blocked)
	}
}
