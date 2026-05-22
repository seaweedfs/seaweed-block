package ops

import "testing"

func TestManagedVolumeOperatorContract_ReadinessConditionAndEvents(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-operator",
		PVCName:  "demo-pvc",
		PVC:      &PVCFact{Phase: "Bound"},
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
		EvidenceRefs: []string{"diagnostics/writer/writer-describe.txt"},
	})

	contract := ManagedVolumeOperatorContractFromProjection(projection)
	if contract.APIVersion != "block.seaweedfs.com/v1alpha1" {
		t.Fatalf("contract=%+v", contract)
	}
	if contract.Kind != "ManagedVolumeStatusContract" {
		t.Fatalf("kind=%s", contract.Kind)
	}
	if len(contract.Status.Conditions) < 2 {
		t.Fatalf("conditions=%+v", contract.Status.Conditions)
	}
	if len(contract.Events) == 0 || contract.Events[0].Type != "Warning" {
		t.Fatalf("events=%+v", contract.Events)
	}
	if len(contract.AllowedActions) == 0 {
		t.Fatalf("allowed actions missing: %+v", contract)
	}
	for _, action := range contract.AllowedActions {
		if action.MutationAllowed {
			t.Fatalf("mutation allowed in Phase 23 contract: %+v", action)
		}
		if action.Mode != ManagedVolumeActionModeReadOnly && action.Mode != ManagedVolumeActionModeDryRun {
			t.Fatalf("unexpected action mode: %+v", action)
		}
	}
}

func TestManagedVolumeOperatorContract_RecoveredConditionEventIsNormal(t *testing.T) {
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
		EvidenceRefs: []string{"primary-failure-recovery.txt"},
	})

	contract := ManagedVolumeOperatorContractFromProjection(projection)
	recovered := false
	for _, event := range contract.Events {
		if event.Reason == ReasonTransparentHostPathRecovered && event.Type == "Normal" {
			recovered = true
		}
	}
	if !recovered {
		t.Fatalf("events=%+v", contract.Events)
	}
}
