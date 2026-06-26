package ops

import (
	"context"
	"errors"
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

func TestFrontendPublicationExecutorExecutionPolicyBlocks(t *testing.T) {
	client := &fakeFrontendPublicationExecutorClient{
		targets: []SwBlockFrontendPublicationObject{frontendPublicationExecutorExecutableTargetFixture()},
	}
	_, err := (FrontendPublicationExecutorReconciler{
		Namespace:          "kube-system",
		Client:             client,
		ExecutionRequested: true,
	}).Reconcile(context.Background())
	if err == nil || err.Error() != "frontend publication executor execution is disabled by product policy" {
		t.Fatalf("err=%v", err)
	}
	if len(client.writes) != 0 {
		t.Fatalf("policy-disabled execution wrote status: %+v", client.writes)
	}
}

func TestFrontendPublicationExecutorBlocksReturnedReplicaRuntimeWithoutAuthorityOwner(t *testing.T) {
	target := frontendPublicationExecutorExecutableTargetFixture()
	client := &fakeFrontendPublicationExecutorClient{targets: []SwBlockFrontendPublicationObject{target}}
	runtime := &fakeFrontendPublicationRuntime{result: FrontendPublicationRuntimeResult{
		FrontendPublished:           true,
		FailbackStarted:             false,
		NoStorageMutation:           true,
		NoCrossVolumeIdentityChange: true,
	}}
	result, err := (FrontendPublicationExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		Now:                    func() time.Time { return time.Date(2026, 6, 25, 12, 30, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FrontendPublicationAttempts != 0 ||
		result.FailbackAttempts != 0 ||
		result.StatusWriteCount != 1 ||
		result.InvalidTargetCount != 1 ||
		result.StorageMutationAllowed {
		t.Fatalf("result=%+v", result)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("returned-replica frontend publication must not invoke runtime: %+v", runtime.requests)
	}
	status := client.writes[0].status
	if status.State != FrontendPublicationStateBlocked ||
		status.ReasonCode != AuthorityExecutorFrontendPublicationReasonAuthorityOwnerRequired ||
		status.FrontendPublished ||
		status.FailbackStarted ||
		!status.NoStorageMutation {
		t.Fatalf("status=%+v", status)
	}
}

func TestFrontendPublicationExecutorInvokesRuntimeWhenExplicitlyEnabled(t *testing.T) {
	target := frontendPublicationExecutorGenericExecutableTargetFixture()
	client := &fakeFrontendPublicationExecutorClient{targets: []SwBlockFrontendPublicationObject{target}}
	runtime := &fakeFrontendPublicationRuntime{result: FrontendPublicationRuntimeResult{
		FrontendPublished:           true,
		FailbackStarted:             false,
		NoStorageMutation:           true,
		NoCrossVolumeIdentityChange: true,
		EvidenceRefs:                []string{"frontend-runtime.txt"},
	}}
	result, err := (FrontendPublicationExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		Now:                    func() time.Time { return time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FrontendPublicationAttempts != 1 ||
		result.FailbackAttempts != 0 ||
		result.StatusWriteCount != 1 ||
		result.StorageMutationAllowed {
		t.Fatalf("result=%+v", result)
	}
	if len(runtime.requests) != 1 {
		t.Fatalf("runtime requests=%+v", runtime.requests)
	}
	req := runtime.requests[0]
	if req.VolumeName != target.Spec.VolumeName ||
		req.ReplicaID != target.Spec.ReplicaID ||
		!req.AckEligible ||
		!req.NoCrossVolumeIdentityChange {
		t.Fatalf("runtime request=%+v", req)
	}
	status := client.writes[0].status
	if status.State != FrontendPublicationStatePublished ||
		status.ReasonCode != AuthorityExecutorFrontendPublicationReasonPublished ||
		status.PublicationMutationAllowed ||
		!status.FrontendPublished ||
		status.FailbackStarted ||
		!status.NoStorageMutation ||
		!status.NoCrossVolumeIdentityChange {
		t.Fatalf("status=%+v", status)
	}
}

func TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus(t *testing.T) {
	client := &fakeFrontendPublicationExecutorClient{targets: []SwBlockFrontendPublicationObject{frontendPublicationExecutorGenericExecutableTargetFixture()}}
	runtime := &fakeFrontendPublicationRuntime{err: errors.New("runtime refused")}
	result, err := (FrontendPublicationExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected runtime error")
	}
	if result.FrontendPublicationAttempts != 1 || result.StatusWriteCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	if got := client.writes[0].status.ReasonCode; got != "frontend_publication_runtime_failed" {
		t.Fatalf("reason=%s", got)
	}
	if client.writes[0].status.FrontendPublished {
		t.Fatalf("failed runtime must not claim published: %+v", client.writes[0].status)
	}
}

func TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence(t *testing.T) {
	client := &fakeFrontendPublicationExecutorClient{targets: []SwBlockFrontendPublicationObject{frontendPublicationExecutorGenericExecutableTargetFixture()}}
	runtime := &fakeFrontendPublicationRuntime{result: FrontendPublicationRuntimeResult{
		FrontendPublished:           true,
		FailbackStarted:             true,
		NoStorageMutation:           true,
		NoCrossVolumeIdentityChange: true,
	}}
	result, err := (FrontendPublicationExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FrontendPublicationAttempts != 1 || result.FailbackAttempts != 1 || result.StatusWriteCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	status := client.writes[0].status
	if status.State != FrontendPublicationStateBlocked ||
		status.ReasonCode != "frontend_publication_runtime_invalid_terminal_evidence" ||
		status.FrontendPublished {
		t.Fatalf("status=%+v", status)
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

func frontendPublicationExecutorExecutableTargetFixture() SwBlockFrontendPublicationObject {
	target := frontendPublicationExecutorTestTarget()
	target.Spec.FrontendPublicationDecision = AuthorityExecutorPublicationDecisionEnabled
	target.Spec.FrontendPublicationReason = "frontend_publication_requested"
	target.Spec.FrontendPublicationMutationAllowed = true
	target.Spec.RuntimeEndpoint = "http://127.0.0.1:23260/runtime/frontend-publication"
	return target
}

func frontendPublicationExecutorGenericExecutableTargetFixture() SwBlockFrontendPublicationObject {
	target := frontendPublicationExecutorExecutableTargetFixture()
	target.Spec.SourceEligibilityName = ""
	return target
}

type fakeFrontendPublicationExecutorClient struct {
	targets []SwBlockFrontendPublicationObject
	writes  []fakeFrontendPublicationStatusWrite
}

type fakeFrontendPublicationRuntime struct {
	requests []FrontendPublicationRuntimeRequest
	result   FrontendPublicationRuntimeResult
	err      error
}

func (f *fakeFrontendPublicationRuntime) ExecuteFrontendPublication(_ context.Context, req FrontendPublicationRuntimeRequest) (FrontendPublicationRuntimeResult, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return FrontendPublicationRuntimeResult{}, f.err
	}
	return f.result, nil
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
