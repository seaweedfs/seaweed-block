package ops

import (
	"context"
	"testing"
	"time"
)

func TestFrontendPublicationExecutorWritesDisabledStatus(t *testing.T) {
	client := &fakeFrontendPublicationExecutorClient{
		targets: []SwBlockFrontendPublicationObject{frontendPublicationExecutorTestTarget()},
	}
	result, err := (FrontendPublicationExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
		Now:       func() time.Time { return time.Date(2026, 6, 25, 11, 0, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetCount != 1 ||
		result.StatusWriteCount != 1 ||
		result.InvalidTargetCount != 0 ||
		result.FrontendPublicationAttempts != 0 ||
		result.FailbackAttempts != 0 ||
		result.StorageMutationAllowed {
		t.Fatalf("result=%+v", result)
	}
	if len(client.writes) != 1 {
		t.Fatalf("writes=%+v", client.writes)
	}
	write := client.writes[0]
	if write.status.State != FrontendPublicationStateBlocked ||
		write.status.ReasonCode != AuthorityExecutorFrontendPublicationReasonDisabled ||
		write.status.PublicationMutationAllowed ||
		write.status.FrontendPublished ||
		write.status.FailbackStarted ||
		!write.status.NoStorageMutation ||
		!write.status.NoCrossVolumeIdentityChange {
		t.Fatalf("status=%+v", write.status)
	}
	if len(write.status.Conditions) != 1 ||
		write.status.Conditions[0].Type != ConditionBlocked ||
		write.status.Conditions[0].Status != "True" {
		t.Fatalf("conditions=%+v", write.status.Conditions)
	}
	for _, want := range []string{"no_frontend_publication", "no_failback", "no_storage_mutation"} {
		if !frontendPublicationExecutorStringSliceContains(write.status.NonClaims, want) {
			t.Fatalf("nonClaims=%+v missing %s", write.status.NonClaims, want)
		}
	}
}

func TestFrontendPublicationExecutorDryRunDoesNotWriteStatus(t *testing.T) {
	client := &fakeFrontendPublicationExecutorClient{
		targets: []SwBlockFrontendPublicationObject{frontendPublicationExecutorTestTarget()},
	}
	result, err := (FrontendPublicationExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
		DryRun:    true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetCount != 1 || result.StatusWriteCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(client.writes) != 0 {
		t.Fatalf("dry-run wrote status: %+v", client.writes)
	}
}

func TestFrontendPublicationExecutorMarksInvalidTargets(t *testing.T) {
	target := frontendPublicationExecutorTestTarget()
	target.Spec.FrontendPublicationMutationAllowed = true
	client := &fakeFrontendPublicationExecutorClient{
		targets: []SwBlockFrontendPublicationObject{target},
	}
	result, err := (FrontendPublicationExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.InvalidTargetCount != 1 || result.StatusWriteCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	if got := client.writes[0].status.ReasonCode; got != "missing_required_facts" {
		t.Fatalf("reason=%s", got)
	}
}

func frontendPublicationExecutorTestTarget() SwBlockFrontendPublicationObject {
	return SwBlockFrontendPublicationObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockFrontendPublicationKind,
			Namespace:  "kube-system",
			Name:       "demo-pvc-r2-frontend-publication",
		},
		Spec: SwBlockFrontendPublicationSpec{
			VolumeName:                         "demo-pvc",
			VolumeID:                           "pvc-demo",
			PVCName:                            "demo-pvc",
			ReplicaID:                          "r2",
			SourceEligibilityName:              "demo-pvc-r2-ack",
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

type fakeFrontendPublicationExecutorClient struct {
	targets []SwBlockFrontendPublicationObject
	writes  []fakeFrontendPublicationStatusWrite
}

type fakeFrontendPublicationStatusWrite struct {
	ref    OperatorObjectRef
	status SwBlockFrontendPublicationCRDStatus
}

func (f *fakeFrontendPublicationExecutorClient) ListSwBlockFrontendPublications(context.Context, string) ([]SwBlockFrontendPublicationObject, error) {
	return append([]SwBlockFrontendPublicationObject(nil), f.targets...), nil
}

func (f *fakeFrontendPublicationExecutorClient) WriteFrontendPublicationStatus(_ context.Context, ref OperatorObjectRef, status SwBlockFrontendPublicationCRDStatus) error {
	f.writes = append(f.writes, fakeFrontendPublicationStatusWrite{ref: ref, status: status})
	return nil
}

func frontendPublicationExecutorStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
