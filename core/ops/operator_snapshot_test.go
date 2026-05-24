package ops

import (
	"testing"
	"time"
)

func TestOperatorFoundationSnapshot_ReadOnlyBoundary(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 23, 20, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{healthyObservationVolume()}

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
