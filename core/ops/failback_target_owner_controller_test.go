package ops

import (
	"context"
	"testing"
)

func TestFailbackTargetOwnerCreatesTargetFromReadyContract(t *testing.T) {
	client := &fakeFailbackTargetOwnerClient{volumes: []SwBlockVolumeObject{failbackTargetOwnerTestVolume()}}

	result, err := (FailbackTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.VolumeCount != 1 ||
		result.ContractCount != 1 ||
		result.TargetPlannedCount != 1 ||
		result.TargetCreateCount != 1 ||
		result.TerminalEvidenceReady != 1 ||
		result.FailbackAttempts != 0 ||
		result.StorageMutationAllowed ||
		result.FrontendPublicationAllowed {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 1 {
		t.Fatalf("creates=%+v", client.creates)
	}
	created := client.creates[0]
	if created.Ref.Kind != SwBlockReplicaFailbackKind ||
		created.Ref.Name != "demo-pvc-r1-failback" ||
		created.Spec.VolumeName != "demo-pvc" ||
		created.Spec.VolumeID != "pvc-demo" ||
		created.Spec.PVCName != "demo-pvc" ||
		created.Spec.ReplicaID != "r1" ||
		created.Spec.TargetDataAddr != "data-r1" ||
		created.Spec.TargetCtrlAddr != "ctrl-r1" ||
		created.Spec.ExpectedCurrentReplicaID != "r2" ||
		created.Spec.ExpectedCurrentEpoch != 7 ||
		!created.Spec.AckEligible ||
		!created.Spec.FrontendFencedBeforeFailback ||
		!created.Spec.DurableFrontierCovered ||
		!created.Spec.NoCrossVolumeIdentityChange ||
		created.Spec.FailbackDecision != AuthorityExecutorFailbackDecisionDisabled ||
		created.Spec.FailbackReason != AuthorityExecutorFailbackReasonDisabled ||
		created.Spec.FailbackMutationAllowed {
		t.Fatalf("created=%+v", created)
	}
}

func TestFailbackTargetOwnerRequiresCurrentAuthorityFacts(t *testing.T) {
	volume := failbackTargetOwnerTestVolume()
	volume.Status.PrimaryReplicaID = ""
	volume.Status.AuthorityEpoch = 0
	client := &fakeFailbackTargetOwnerClient{volumes: []SwBlockVolumeObject{volume}}

	result, err := (FailbackTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.AuthorityFactsMissing != 1 ||
		result.TargetPlannedCount != 0 ||
		result.TerminalEvidenceReady != 0 ||
		result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("missing authority facts created target=%+v", client.creates)
	}
}

func TestFailbackTargetOwnerDryRunDoesNotCreateTarget(t *testing.T) {
	client := &fakeFailbackTargetOwnerClient{volumes: []SwBlockVolumeObject{failbackTargetOwnerTestVolume()}}

	result, err := (FailbackTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
		DryRun:    true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetPlannedCount != 1 || result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("dry-run creates=%+v", client.creates)
	}
}

func TestFailbackTargetOwnerRejectsNonFailbackContract(t *testing.T) {
	volume := failbackTargetOwnerTestVolume()
	volume.Status.ExecutorContracts[0].AllowedMutationClass = []string{AuthorityExecutorAllowedMutationAckEligibility}
	client := &fakeFailbackTargetOwnerClient{volumes: []SwBlockVolumeObject{volume}}

	result, err := (FailbackTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.ContractCount != 1 || result.InvalidContractCount != 1 || result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestFailbackTargetOwnerRequiresTerminalEvidence(t *testing.T) {
	volume := failbackTargetOwnerTestVolume()
	volume.Status.ReplicaReintegrations[0].AckEligible = false
	client := &fakeFailbackTargetOwnerClient{volumes: []SwBlockVolumeObject{volume}}

	result, err := (FailbackTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TerminalEvidenceMissing != 1 || result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestFailbackTargetOwnerSkipsExistingTarget(t *testing.T) {
	volume := failbackTargetOwnerTestVolume()
	client := &fakeFailbackTargetOwnerClient{
		volumes: []SwBlockVolumeObject{volume},
		targets: []SwBlockReplicaFailbackObject{{
			Spec: SwBlockReplicaFailbackSpec{
				VolumeName: "demo-pvc",
				ReplicaID:  "r1",
			},
		}},
	}

	result, err := (FailbackTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetExistingCount != 1 || result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func failbackTargetOwnerTestVolume() SwBlockVolumeObject {
	return SwBlockVolumeObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockVolumeKind,
			Namespace:  "kube-system",
			Name:       "demo-pvc",
		},
		Status: SwBlockVolumeCRDStatus{
			VolumeID:         "pvc-demo",
			PVCName:          "demo-pvc",
			PrimaryReplicaID: "r2",
			AuthorityEpoch:   7,
			ReplicaReintegrations: []SwBlockVolumeCRDReturnedReplica{{
				ReplicaID:             "r1",
				State:                 ReturnedReplicaStateFenced,
				FrontendFenced:        true,
				FrontendPrimaryReady:  false,
				AckEligibilityKnown:   true,
				AckEligible:           true,
				DurableFrontierKnown:  true,
				DurableFrontierLSN:    52,
				RequiredFrontierKnown: true,
				RequiredFrontierLSN:   52,
				TargetDataAddr:        "data-r1",
				TargetCtrlAddr:        "ctrl-r1",
			}},
			ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
				ActionType:           ManagedVolumeActionFailbackReturned,
				ReplicaID:            "r1",
				Decision:             ReturnedReplicaExecutorContractDisabled,
				Reason:               ReturnedReplicaExecutorContractReasonExecutorDisabled,
				PreflightDecision:    ReturnedReplicaExecutorPreflightReady,
				PreflightReason:      ReturnedReplicaExecutorPreflightReasonSatisfied,
				AllowedMutationClass: []string{"failback"},
			}},
		},
	}
}

type fakeFailbackTargetOwnerClient struct {
	volumes []SwBlockVolumeObject
	targets []SwBlockReplicaFailbackObject
	creates []SwBlockReplicaFailbackObject
}

func (f *fakeFailbackTargetOwnerClient) ListSwBlockVolumes(context.Context, string) ([]SwBlockVolumeObject, error) {
	return append([]SwBlockVolumeObject(nil), f.volumes...), nil
}

func (f *fakeFailbackTargetOwnerClient) ListSwBlockReplicaFailbacks(context.Context, string) ([]SwBlockReplicaFailbackObject, error) {
	return append([]SwBlockReplicaFailbackObject(nil), f.targets...), nil
}

func (f *fakeFailbackTargetOwnerClient) CreateSwBlockReplicaFailback(_ context.Context, _ string, obj SwBlockReplicaFailbackObject) error {
	f.creates = append(f.creates, obj)
	return nil
}
