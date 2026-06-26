package ops

import (
	"context"
	"testing"
	"time"
)

func TestFailbackTargetOwnerExecutorHandoffUsesExpectedCurrentAuthority(t *testing.T) {
	ownerClient := &fakeFailbackTargetOwnerClient{volumes: []SwBlockVolumeObject{failbackTargetOwnerTestVolume()}}
	ownerResult, err := (FailbackTargetOwnerReconciler{
		Namespace:               "kube-system",
		Client:                  ownerClient,
		ActivateTargets:         true,
		ActivationPolicyEnabled: true,
		RuntimeEndpoint:         "blockmaster.kube-system.svc:9333",
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("owner reconcile: %v", err)
	}
	if ownerResult.TargetCreateCount != 1 || ownerResult.FailbackAttempts != 0 || ownerResult.FrontendPublicationAllowed || ownerResult.StorageMutationAllowed {
		t.Fatalf("owner result=%+v", ownerResult)
	}
	if len(ownerClient.creates) != 1 {
		t.Fatalf("owner creates=%+v", ownerClient.creates)
	}

	executorClient := &fakeFailbackExecutorClient{targets: ownerClient.creates}
	runtime := &fakeFailbackRuntime{result: FailbackRuntimeResult{
		FailbackStarted:                   true,
		AuthorityEpochAdvanced:            true,
		SinglePrimaryAfterFailback:        true,
		PublishTargetSwappedAfterFailback: true,
		NoStorageMutation:                 true,
		NoCrossVolumeIdentityChange:       true,
		EvidenceRefs:                      []string{"phase92-failback-runtime.txt"},
	}}
	executorResult, err := (FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 executorClient,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		Now:                    func() time.Time { return time.Date(2026, 6, 26, 16, 0, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("executor reconcile: %v", err)
	}
	if executorResult.FailbackAttempts != 1 ||
		executorResult.StatusWriteCount != 1 ||
		!executorResult.AuthorityMutationAllowed ||
		executorResult.FrontendPublicationAllowed ||
		executorResult.StorageMutationAllowed {
		t.Fatalf("executor result=%+v", executorResult)
	}
	if len(runtime.requests) != 1 {
		t.Fatalf("runtime requests=%+v", runtime.requests)
	}
	req := runtime.requests[0]
	if req.ExpectedCurrentReplicaID != "r2" ||
		req.ExpectedCurrentEpoch != 7 ||
		req.ReplicaID != "r1" ||
		req.TargetDataAddr != "data-r1" ||
		req.TargetCtrlAddr != "ctrl-r1" {
		t.Fatalf("runtime request=%+v", req)
	}
	status := executorClient.writes[0].status
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
	if !failbackExecutorStringSliceContains(status.EvidenceRefs, "phase92-failback-runtime.txt") {
		t.Fatalf("evidenceRefs=%+v", status.EvidenceRefs)
	}
}
