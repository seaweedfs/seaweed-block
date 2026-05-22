package ops

import "testing"

func TestProjectManagedVolumeFromEvidence_PreservesReadyVolume(t *testing.T) {
	projection := ProjectManagedVolumeFromEvidence(VolumeEvidence{
		VolumeID:          "pvc-a",
		Namespace:         "default",
		PVCName:           "demo-pvc",
		PVName:            "pvc-a",
		ReplicationFactor: 1,
		AckProfile:        "best-effort",
		Status:            ObservationStatusOK,
		PrimaryReplica:    "r1",
		PublishTarget:     "127.0.0.1:3260",
		Epoch:             1,
		EndpointVersion:   1,
		Replicas: []ReplicaEvidence{{
			ReplicaID:            "r1",
			KubernetesNode:       "m02",
			Observed:             true,
			Role:                 "primary",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   7,
			FrontendAddr:         "127.0.0.1:3260",
		}},
	})

	if projection.VolumeID != "pvc-a" || projection.PVCName != "demo-pvc" {
		t.Fatalf("projection identity=%+v", projection)
	}
	if projection.States.Authority != ManagedVolumeAuthorityPrimaryAvailable {
		t.Fatalf("authority=%s", projection.States.Authority)
	}
	if projection.Status != ManagedVolumeStatusReady {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonFirstVolumeVerified {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
}

func TestProjectManagedVolumeFromEvidence_DualPrimaryInvalid(t *testing.T) {
	projection := ProjectManagedVolumeFromEvidence(VolumeEvidence{
		VolumeID:       "pvc-a",
		Status:         ObservationStatusDegraded,
		PrimaryReplica: "r1",
		Replicas: []ReplicaEvidence{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Observed:       true,
			Role:           "primary",
		}, {
			ReplicaID:      "r2",
			KubernetesNode: "m02",
			Observed:       true,
			Role:           "primary",
		}},
	})

	if projection.Status != ManagedVolumeStatusInvalid {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonMultiplePrimariesObserved {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
}

func TestProjectManagedVolumeFromEvidence_ImagePullBlocked(t *testing.T) {
	projection := ProjectManagedVolumeFromEvidence(VolumeEvidence{
		VolumeID: "pvc-blocked",
		Status:   ObservationStatusBlocked,
		Reason:   ReasonCSINodeImagePullFailed,
	})

	if projection.Status != ManagedVolumeStatusBlocked {
		t.Fatalf("status=%s reason=%s", projection.Status, projection.ReasonCode)
	}
	if projection.ReasonCode != ReasonCSINodeImagePullFailed {
		t.Fatalf("reason=%s", projection.ReasonCode)
	}
	if !hasManagedVolumeAction(projection.Actions, ManagedVolumeActionImportCSIImage) {
		t.Fatalf("missing import action: %+v", projection.Actions)
	}
}
