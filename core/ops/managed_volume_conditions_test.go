package ops

import "testing"

func TestManagedVolumeProjection_ReadyConditionForFirstVolume(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-ready",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "127.0.0.1:3260",
		},
		CSIStages: []CSIStageFact{{
			NodeName: "m02",
			Target:   "127.0.0.1:3260",
		}},
		Workload: &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
	})

	ready := findObservationCondition(projection.Conditions, "Ready")
	if ready == nil {
		t.Fatalf("conditions=%+v", projection.Conditions)
	}
	if ready.Status != "True" || ready.Reason != ReasonFirstVolumeVerified || ready.Severity != "info" {
		t.Fatalf("ready=%+v", ready)
	}
}

func TestManagedVolumeProjection_BlockedConditionForLoopbackCrossNode(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-blocked",
		PVCName:  "demo-pvc",
		EvidenceRefs: []string{
			"diagnostics/writer/writer-describe.txt",
		},
		PVC: &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "127.0.0.1:3260",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:      "r1",
			KubernetesNode: "m01",
			Role:           "primary",
			Observed:       true,
		}},
		CSIStages: []CSIStageFact{{
			NodeName: "m02",
			Target:   "127.0.0.1:3260",
		}},
	})

	ready := findObservationCondition(projection.Conditions, "Ready")
	blocked := findObservationCondition(projection.Conditions, "Blocked")
	if ready == nil || blocked == nil {
		t.Fatalf("conditions=%+v", projection.Conditions)
	}
	if ready.Status != "False" || ready.Reason != ReasonPublishTargetLoopbackCrossNode {
		t.Fatalf("ready=%+v", ready)
	}
	if blocked.Status != "True" || blocked.Severity != "warning" {
		t.Fatalf("blocked=%+v", blocked)
	}
	if len(blocked.EvidenceRefs) != 1 || blocked.EvidenceRefs[0] != "diagnostics/writer/writer-describe.txt" {
		t.Fatalf("blocked evidence=%+v", blocked.EvidenceRefs)
	}
}

func TestManagedVolumeProjection_RecoveredConditionForTransparentFailover(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-recovered",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:  "r2",
			PreviousPrimary: "r1",
			PublishTarget:   "192.168.1.184:3261",
		},
		HostPaths: []HostPathFact{{
			Protocol:       "iscsi",
			State:          HostPathStateActiveOptimized,
			MultipathReady: true,
			StaleFenced:    true,
		}},
		Workload: &WorkloadCheckFact{
			WriterVerified: true,
			ReaderVerified: true,
			SamePodUID:     true,
		},
	})

	recovered := findObservationCondition(projection.Conditions, "Recovered")
	if recovered == nil {
		t.Fatalf("conditions=%+v", projection.Conditions)
	}
	if recovered.Status != "True" || recovered.Reason != ReasonTransparentHostPathRecovered {
		t.Fatalf("recovered=%+v", recovered)
	}
}

func TestManagedVolumeProjection_EvidenceStaleIsNotReady(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:            "pvc-stale",
		EvidenceStale:       true,
		EvidenceStaleReason: ReasonEvidenceStale,
		EvidenceRefs:        []string{"product/unreachable.txt"},
	})

	ready := findObservationCondition(projection.Conditions, "Ready")
	stale := findObservationCondition(projection.Conditions, "EvidenceStale")
	if ready == nil || stale == nil {
		t.Fatalf("conditions=%+v", projection.Conditions)
	}
	if ready.Status != "Unknown" || ready.Reason != ReasonEvidenceStale {
		t.Fatalf("ready=%+v", ready)
	}
	if stale.Status != "True" || stale.Severity != "warning" || stale.Reason != ReasonEvidenceStale {
		t.Fatalf("stale=%+v", stale)
	}
	if projection.Status != ManagedVolumeStatusUnknown || projection.ReasonCode != ReasonEvidenceStale {
		t.Fatalf("projection=%+v", projection)
	}
}

func findObservationCondition(conditions []ObservationCondition, conditionType string) *ObservationCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}
