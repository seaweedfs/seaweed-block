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

func TestFailbackTargetOwnerExecutorHandoffIsolatesMultipleVolumes(t *testing.T) {
	ownerClient := &fakeFailbackTargetOwnerClient{volumes: []SwBlockVolumeObject{
		phase93FailbackVolume("pvc-a", "pvc-a-id", "r1", "r2", 7, "data-a-r1", "ctrl-a-r1"),
		phase93FailbackVolume("pvc-b", "pvc-b-id", "r3", "r4", 11, "data-b-r3", "ctrl-b-r3"),
	}}
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
	if ownerResult.TargetCreateCount != 2 ||
		ownerResult.TargetPlannedCount != 2 ||
		ownerResult.TerminalEvidenceReady != 2 ||
		ownerResult.FailbackAttempts != 0 ||
		ownerResult.FrontendPublicationAllowed ||
		ownerResult.StorageMutationAllowed {
		t.Fatalf("owner result=%+v", ownerResult)
	}

	executorClient := &fakeFailbackExecutorClient{targets: ownerClient.creates}
	runtime := &fakeFailbackRuntime{result: FailbackRuntimeResult{
		FailbackStarted:                   true,
		AuthorityEpochAdvanced:            true,
		SinglePrimaryAfterFailback:        true,
		PublishTargetSwappedAfterFailback: true,
		NoStorageMutation:                 true,
		NoCrossVolumeIdentityChange:       true,
		EvidenceRefs:                      []string{"phase93-failback-runtime.txt"},
	}}
	executorResult, err := (FailbackExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 executorClient,
		Runtime:                runtime,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("executor reconcile: %v", err)
	}
	if executorResult.FailbackAttempts != 2 ||
		executorResult.StatusWriteCount != 2 ||
		!executorResult.AuthorityMutationAllowed ||
		executorResult.FrontendPublicationAllowed ||
		executorResult.StorageMutationAllowed {
		t.Fatalf("executor result=%+v", executorResult)
	}
	if len(runtime.requests) != 2 {
		t.Fatalf("runtime requests=%+v", runtime.requests)
	}

	requestByVolume := map[string]FailbackRuntimeRequest{}
	for _, req := range runtime.requests {
		requestByVolume[req.VolumeID] = req
	}
	assertPhase93RuntimeRequest(t, requestByVolume["pvc-a-id"], "pvc-a-id", "r1", "r2", 7, "data-a-r1", "ctrl-a-r1")
	assertPhase93RuntimeRequest(t, requestByVolume["pvc-b-id"], "pvc-b-id", "r3", "r4", 11, "data-b-r3", "ctrl-b-r3")
}

func phase93FailbackVolume(name, volumeID, returnedReplica, currentReplica string, epoch uint64, dataAddr, ctrlAddr string) SwBlockVolumeObject {
	volume := failbackTargetOwnerTestVolume()
	volume.Ref.Name = name
	volume.Status.VolumeID = volumeID
	volume.Status.PVCName = name
	volume.Status.PrimaryReplicaID = currentReplica
	volume.Status.AuthorityEpoch = epoch
	volume.Status.ReplicaReintegrations[0].ReplicaID = returnedReplica
	volume.Status.ReplicaReintegrations[0].TargetDataAddr = dataAddr
	volume.Status.ReplicaReintegrations[0].TargetCtrlAddr = ctrlAddr
	volume.Status.ExecutorContracts[0].ReplicaID = returnedReplica
	return volume
}

func assertPhase93RuntimeRequest(t *testing.T, req FailbackRuntimeRequest, volumeID, returnedReplica, currentReplica string, epoch uint64, dataAddr, ctrlAddr string) {
	t.Helper()
	if req.VolumeID != volumeID ||
		req.ReplicaID != returnedReplica ||
		req.ExpectedCurrentReplicaID != currentReplica ||
		req.ExpectedCurrentEpoch != epoch ||
		req.TargetDataAddr != dataAddr ||
		req.TargetCtrlAddr != ctrlAddr {
		t.Fatalf("runtime request mismatch got=%+v want volume=%s returned=%s current=%s epoch=%d data=%s ctrl=%s",
			req,
			volumeID,
			returnedReplica,
			currentReplica,
			epoch,
			dataAddr,
			ctrlAddr)
	}
}
