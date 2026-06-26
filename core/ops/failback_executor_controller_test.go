package ops

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFailbackExecutorWritesDisabledStatus(t *testing.T) {
	client := &fakeFailbackExecutorClient{
		targets: []SwBlockReplicaFailbackObject{failbackExecutorTestTarget()},
	}
	result, err := (FailbackExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
		Now:       func() time.Time { return time.Date(2026, 6, 25, 13, 0, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.TargetCount != 1 ||
		result.StatusWriteCount != 1 ||
		result.InvalidTargetCount != 0 ||
		result.FailbackAttempts != 0 ||
		result.AuthorityMutationAllowed ||
		result.FrontendPublicationAllowed ||
		result.StorageMutationAllowed {
		t.Fatalf("result=%+v", result)
	}
	if len(client.writes) != 1 {
		t.Fatalf("writes=%+v", client.writes)
	}
	status := client.writes[0].status
	if status.State != FailbackStateBlocked ||
		status.ReasonCode != AuthorityExecutorFailbackReasonDisabled ||
		status.FailbackMutationAllowed ||
		status.FailbackStarted ||
		status.AuthorityEpochAdvanced ||
		status.SinglePrimaryAfterFailback ||
		status.PublishTargetSwappedAfterFailback ||
		!status.NoCrossVolumeIdentityChange {
		t.Fatalf("status=%+v", status)
	}
	if len(status.Conditions) != 1 ||
		status.Conditions[0].Type != ConditionBlocked ||
		status.Conditions[0].Status != "True" ||
		status.Conditions[0].Reason != AuthorityExecutorFailbackReasonDisabled {
		t.Fatalf("conditions=%+v", status.Conditions)
	}
	for _, want := range []string{
		"no_failback",
		"no_authority_epoch_advance",
		"no_primary_reassignment",
		"no_publish_target_swap",
		"no_frontend_publication",
		"no_storage_mutation",
	} {
		if !failbackExecutorStringSliceContains(status.NonClaims, want) {
			t.Fatalf("nonClaims=%+v missing %s", status.NonClaims, want)
		}
	}
}

func TestFailbackExecutorDryRunDoesNotWriteStatus(t *testing.T) {
	client := &fakeFailbackExecutorClient{
		targets: []SwBlockReplicaFailbackObject{failbackExecutorTestTarget()},
	}
	result, err := (FailbackExecutorReconciler{
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

func TestFailbackExecutorMarksInvalidTargets(t *testing.T) {
	target := failbackExecutorTestTarget()
	target.Spec.DurableFrontierCovered = false
	client := &fakeFailbackExecutorClient{
		targets: []SwBlockReplicaFailbackObject{target},
	}
	result, err := (FailbackExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.InvalidTargetCount != 1 || result.StatusWriteCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	status := client.writes[0].status
	if status.State != FailbackStateBlocked ||
		status.ReasonCode != AuthorityExecutorFailbackReasonMissingFacts ||
		status.FailbackMutationAllowed ||
		status.FailbackStarted {
		t.Fatalf("status=%+v", status)
	}
}

func TestFailbackExecutorExecutionPolicyBlocks(t *testing.T) {
	client := &fakeFailbackExecutorClient{
		targets: []SwBlockReplicaFailbackObject{failbackExecutorExecutableTargetFixture()},
	}
	_, err := (FailbackExecutorReconciler{
		Namespace:          "kube-system",
		Client:             client,
		ExecutionRequested: true,
	}).Reconcile(context.Background())
	if err == nil || err.Error() != "failback executor execution is disabled by product policy" {
		t.Fatalf("err=%v", err)
	}
	if len(client.writes) != 0 {
		t.Fatalf("policy-disabled execution wrote status: %+v", client.writes)
	}
}

func TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled(t *testing.T) {
	target := failbackExecutorExecutableTargetFixture()
	client := &fakeFailbackExecutorClient{targets: []SwBlockReplicaFailbackObject{target}}
	runtime := &fakeFailbackRuntime{result: FailbackRuntimeResult{
		FailbackStarted:                   true,
		AuthorityEpochAdvanced:            true,
		SinglePrimaryAfterFailback:        true,
		PublishTargetSwappedAfterFailback: true,
		NoStorageMutation:                 true,
		NoCrossVolumeIdentityChange:       true,
		EvidenceRefs:                      []string{"failback-runtime.txt"},
	}}
	result, err := (FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		Now:                    func() time.Time { return time.Date(2026, 6, 26, 9, 0, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FailbackAttempts != 1 ||
		result.StatusWriteCount != 1 ||
		result.AuthorityMutationAllowed ||
		result.FrontendPublicationAllowed ||
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
		!req.FrontendFencedBeforeFailback ||
		!req.NoCrossVolumeIdentityChange {
		t.Fatalf("runtime request=%+v", req)
	}
	status := client.writes[0].status
	if status.State != FailbackStateFailedBack ||
		status.ReasonCode != AuthorityExecutorFailbackReasonCompleted ||
		status.FailbackMutationAllowed ||
		!status.FailbackStarted ||
		!status.AuthorityEpochAdvanced ||
		!status.SinglePrimaryAfterFailback ||
		!status.PublishTargetSwappedAfterFailback ||
		!status.NoCrossVolumeIdentityChange {
		t.Fatalf("status=%+v", status)
	}
}

func TestFailbackExecutorRuntimeFailureWritesBlockedStatus(t *testing.T) {
	client := &fakeFailbackExecutorClient{targets: []SwBlockReplicaFailbackObject{failbackExecutorExecutableTargetFixture()}}
	runtime := &fakeFailbackRuntime{err: errors.New("runtime refused")}
	result, err := (FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected runtime error")
	}
	if result.FailbackAttempts != 1 || result.StatusWriteCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	if got := client.writes[0].status.ReasonCode; got != AuthorityExecutorFailbackReasonRuntimeFailed {
		t.Fatalf("reason=%s", got)
	}
	if client.writes[0].status.FailbackStarted {
		t.Fatalf("failed runtime must not claim failback: %+v", client.writes[0].status)
	}
}

func TestFailbackExecutorRejectsInvalidRuntimeTerminalEvidence(t *testing.T) {
	client := &fakeFailbackExecutorClient{targets: []SwBlockReplicaFailbackObject{failbackExecutorExecutableTargetFixture()}}
	runtime := &fakeFailbackRuntime{result: FailbackRuntimeResult{
		FailbackStarted:                   true,
		AuthorityEpochAdvanced:            true,
		SinglePrimaryAfterFailback:        false,
		PublishTargetSwappedAfterFailback: true,
		NoStorageMutation:                 true,
		NoCrossVolumeIdentityChange:       true,
	}}
	result, err := (FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FailbackAttempts != 1 || result.StatusWriteCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	status := client.writes[0].status
	if status.State != FailbackStateBlocked ||
		status.ReasonCode != AuthorityExecutorFailbackReasonInvalidTerminalEvidence ||
		status.FailbackStarted ||
		status.SinglePrimaryAfterFailback {
		t.Fatalf("status=%+v", status)
	}
}

func failbackExecutorTestTarget() SwBlockReplicaFailbackObject {
	return SwBlockReplicaFailbackObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaFailbackKind,
			Namespace:  "kube-system",
			Name:       "demo-pvc-r2-failback",
		},
		Spec: SwBlockReplicaFailbackSpec{
			VolumeName:                   "demo-pvc",
			VolumeID:                     "pvc-demo",
			PVCName:                      "demo-pvc",
			ReplicaID:                    "r2",
			AckEligible:                  true,
			FrontendFencedBeforeFailback: true,
			DurableFrontierCovered:       true,
			NoCrossVolumeIdentityChange:  true,
			FailbackDecision:             AuthorityExecutorFailbackDecisionDisabled,
			FailbackReason:               AuthorityExecutorFailbackReasonDisabled,
			FailbackMutationAllowed:      false,
		},
	}
}

func failbackExecutorExecutableTargetFixture() SwBlockReplicaFailbackObject {
	target := failbackExecutorTestTarget()
	target.Spec.FailbackDecision = AuthorityExecutorFailbackDecisionEnabled
	target.Spec.FailbackReason = "failback_requested"
	target.Spec.FailbackMutationAllowed = true
	target.Spec.RuntimeEndpoint = "http://127.0.0.1:23260/runtime/failback"
	return target
}

type fakeFailbackExecutorClient struct {
	targets []SwBlockReplicaFailbackObject
	writes  []fakeFailbackStatusWrite
}

type fakeFailbackRuntime struct {
	requests []FailbackRuntimeRequest
	result   FailbackRuntimeResult
	err      error
}

func (f *fakeFailbackRuntime) ExecuteFailback(_ context.Context, req FailbackRuntimeRequest) (FailbackRuntimeResult, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return FailbackRuntimeResult{}, f.err
	}
	return f.result, nil
}

type fakeFailbackStatusWrite struct {
	ref    OperatorObjectRef
	status SwBlockReplicaFailbackCRDStatus
}

func (f *fakeFailbackExecutorClient) ListSwBlockReplicaFailbacks(context.Context, string) ([]SwBlockReplicaFailbackObject, error) {
	return append([]SwBlockReplicaFailbackObject(nil), f.targets...), nil
}

func (f *fakeFailbackExecutorClient) WriteReplicaFailbackStatus(_ context.Context, ref OperatorObjectRef, status SwBlockReplicaFailbackCRDStatus) error {
	f.writes = append(f.writes, fakeFailbackStatusWrite{ref: ref, status: status})
	return nil
}

func failbackExecutorStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
