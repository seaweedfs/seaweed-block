package ops

import (
	"context"
	"testing"
)

func TestFrontendPublicationTargetOwnerDryRunPlansTargetWithoutCreate(t *testing.T) {
	client := &fakeFrontendPublicationTargetOwnerClient{
		eligibilities: []SwBlockReplicaEligibilityObject{frontendPublicationTargetOwnerTestEligibility()},
	}
	result, err := (FrontendPublicationTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
		DryRun:    true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.EligibilityCount != 1 ||
		result.ReadyEligibilityCount != 1 ||
		result.TargetPlannedCount != 1 ||
		result.TargetCreateCount != 0 ||
		result.InvalidEligibilityCount != 0 ||
		result.FrontendPublicationAttempts != 0 ||
		result.FailbackAttempts != 0 ||
		result.StorageMutationAllowed {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("dry-run created targets: %+v", client.creates)
	}
}

func TestFrontendPublicationTargetOwnerCreatesMissingTarget(t *testing.T) {
	client := &fakeFrontendPublicationTargetOwnerClient{
		eligibilities: []SwBlockReplicaEligibilityObject{frontendPublicationTargetOwnerTestEligibility()},
	}
	result, err := (FrontendPublicationTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetCreateCount != 1 || result.TargetExistingCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 1 {
		t.Fatalf("creates=%+v", client.creates)
	}
	created := client.creates[0]
	if created.Ref.Name != "demo-pvc-r2-frontend-publication" ||
		created.Ref.Namespace != "kube-system" ||
		created.Spec.VolumeName != "demo-pvc" ||
		created.Spec.VolumeID != "pvc-demo" ||
		created.Spec.PVCName != "demo-pvc" ||
		created.Spec.ReplicaID != "r2" ||
		created.Spec.SourceEligibilityName != "demo-pvc-r2-ack" ||
		!created.Spec.AckEligibilityKnown ||
		!created.Spec.AckEligible ||
		!created.Spec.FrontendFencedAfterExecution ||
		!created.Spec.PrimaryUnchanged ||
		!created.Spec.DurableFrontierCovered ||
		!created.Spec.NoCrossVolumeIdentityChange ||
		created.Spec.FrontendPublicationDecision != AuthorityExecutorPublicationDecisionDisabled ||
		created.Spec.FrontendPublicationReason != AuthorityExecutorFrontendPublicationReasonDisabled ||
		created.Spec.FrontendPublicationMutationAllowed {
		t.Fatalf("created=%+v", created)
	}
	if created.Status.State != "" {
		t.Fatalf("target owner must not pre-populate status: %+v", created.Status)
	}
}

func TestFrontendPublicationTargetOwnerSkipsExistingTarget(t *testing.T) {
	client := &fakeFrontendPublicationTargetOwnerClient{
		eligibilities: []SwBlockReplicaEligibilityObject{frontendPublicationTargetOwnerTestEligibility()},
		targets: []SwBlockFrontendPublicationObject{{
			Ref: OperatorObjectRef{
				Namespace: "kube-system",
				Name:      "existing",
			},
			Spec: SwBlockFrontendPublicationSpec{
				VolumeName: "demo-pvc",
				ReplicaID:  "r2",
			},
		}},
	}
	result, err := (FrontendPublicationTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetExistingCount != 1 || result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("unexpected creates=%+v", client.creates)
	}
}

func TestFrontendPublicationTargetOwnerRejectsEnabledPublication(t *testing.T) {
	eligibility := frontendPublicationTargetOwnerTestEligibility()
	eligibility.Status.FrontendPublicationMutationAllowed = true
	client := &fakeFrontendPublicationTargetOwnerClient{
		eligibilities: []SwBlockReplicaEligibilityObject{eligibility},
	}
	result, err := (FrontendPublicationTargetOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.InvalidEligibilityCount != 1 ||
		result.TargetPlannedCount != 0 ||
		result.TargetCreateCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.creates) != 0 {
		t.Fatalf("unexpected creates=%+v", client.creates)
	}
}

func frontendPublicationTargetOwnerTestEligibility() SwBlockReplicaEligibilityObject {
	return SwBlockReplicaEligibilityObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaEligibilityKind,
			Namespace:  "kube-system",
			Name:       "demo-pvc-r2-ack",
		},
		Spec: SwBlockReplicaEligibilitySpec{
			VolumeName: "demo-pvc",
			VolumeID:   "pvc-demo",
			PVCName:    "demo-pvc",
			ReplicaID:  "r2",
		},
		Status: SwBlockReplicaEligibilityCRDStatus{
			Executor:                           "authority-recovery-executor",
			ReasonCode:                         ReturnedReplicaExecutorPreflightReasonAckEligible,
			AckEligibilityKnown:                true,
			AckEligible:                        true,
			FrontendFencedAfterExecution:       true,
			PrimaryUnchanged:                   true,
			DurableFrontierCovered:             true,
			NoCrossVolumeIdentityChange:        true,
			FrontendPublicationDecision:        AuthorityExecutorPublicationDecisionDisabled,
			FrontendPublicationReason:          AuthorityExecutorFrontendPublicationReasonDisabled,
			FrontendPublicationMutationAllowed: false,
		},
	}
}

type fakeFrontendPublicationTargetOwnerClient struct {
	eligibilities []SwBlockReplicaEligibilityObject
	targets       []SwBlockFrontendPublicationObject
	creates       []SwBlockFrontendPublicationObject
}

func (f *fakeFrontendPublicationTargetOwnerClient) ListSwBlockReplicaEligibilities(context.Context, string) ([]SwBlockReplicaEligibilityObject, error) {
	return append([]SwBlockReplicaEligibilityObject(nil), f.eligibilities...), nil
}

func (f *fakeFrontendPublicationTargetOwnerClient) ListSwBlockFrontendPublications(context.Context, string) ([]SwBlockFrontendPublicationObject, error) {
	return append([]SwBlockFrontendPublicationObject(nil), f.targets...), nil
}

func (f *fakeFrontendPublicationTargetOwnerClient) CreateSwBlockFrontendPublication(_ context.Context, _ string, obj SwBlockFrontendPublicationObject) error {
	f.creates = append(f.creates, obj)
	return nil
}
