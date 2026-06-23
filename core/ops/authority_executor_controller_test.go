package ops

import (
	"context"
	"testing"
	"time"
)

func TestAuthorityExecutorReconcilerObservesDisabledContractsWithoutMutation(t *testing.T) {
	client := &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "returned"},
		Status: SwBlockVolumeCRDStatus{ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
			ActionType:               ManagedVolumeActionReintegrateReturned,
			ReplicaID:                "r1",
			Decision:                 ReturnedReplicaExecutorContractDisabled,
			Reason:                   ReturnedReplicaExecutorContractReasonExecutorDisabled,
			OwnerExecutor:            "authority_recovery_executor",
			ExecutionEnabled:         false,
			MutationAllowed:          false,
			AllowedMutationClass:     []string{"ack_eligibility"},
			ForbiddenMutationClass:   []string{"frontend_publication", "rebuild_traffic", "failback"},
			TerminalEvidenceRequired: []string{"ack_eligibility_known", "frontend_fenced_after_execution"},
		}}},
	}}}

	result, err := (AuthorityExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.VolumeCount != 1 ||
		result.ContractCount != 1 ||
		result.DisabledContractCount != 1 ||
		result.TerminalEvidenceRequiredCount != 1 ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerFailsClosedOnExecutionEnabledContract(t *testing.T) {
	client := &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "unsafe"},
		Status: SwBlockVolumeCRDStatus{ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
			ActionType:       ManagedVolumeActionReintegrateReturned,
			Decision:         ReturnedReplicaExecutorContractDisabled,
			Reason:           ReturnedReplicaExecutorContractReasonExecutorDisabled,
			OwnerExecutor:    "authority_recovery_executor",
			ExecutionEnabled: true,
			MutationAllowed:  false,
		}}},
	}}}

	result, err := (AuthorityExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected execution-enabled contract to fail closed, result=%+v", result)
	}
	if result.UnsafeExecutionContractCount != 1 || result.MutationAttemptCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerRejectsUnsupportedMutationClass(t *testing.T) {
	result, err := (AuthorityExecutorReconciler{
		Client:               &fakeAuthorityExecutorClient{},
		AllowedMutationClass: "rebuild_traffic",
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected unsupported mutation class to fail closed, result=%+v", result)
	}
	if result.BlockedReason != "unsupported_mutation_class" || result.MutationAttemptCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerIgnoresDisabledRebuildContractWithoutMutation(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes: []SwBlockVolumeObject{authorityExecutorRebuildVolume(false, false)},
		eligibilities: []SwBlockReplicaEligibilityObject{{
			Ref: OperatorObjectRef{Namespace: "kube-system", Name: "rebuild-r1"},
			Spec: SwBlockReplicaEligibilitySpec{
				VolumeName: "rebuild",
				VolumeID:   "pvc-rebuild",
				PVCName:    "rebuild-pvc",
				ReplicaID:  "r1",
			},
		}},
	}
	result, err := (AuthorityExecutorReconciler{
		Namespace:              "kube-system",
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.VolumeCount != 1 ||
		result.ContractCount != 0 ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 ||
		len(client.writes) != 0 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
}

func TestAuthorityExecutorReconcilerFailsClosedOnEnabledRebuildContract(t *testing.T) {
	client := &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{authorityExecutorRebuildVolume(true, false)}}
	result, err := (AuthorityExecutorReconciler{
		Namespace: "kube-system",
		Client:    client,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected enabled rebuild contract to fail closed, result=%+v", result)
	}
	if result.UnsafeExecutionContractCount != 1 || result.MutationAttemptCount != 0 || len(client.writes) != 0 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
}

func TestAuthorityExecutorReconcilerRejectsExecutionWhenPolicyDisabled(t *testing.T) {
	result, err := (AuthorityExecutorReconciler{
		Client:             &fakeAuthorityExecutorClient{},
		ExecutionRequested: true,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected disabled policy to fail closed, result=%+v", result)
	}
	if result.BlockedReason != AuthorityExecutorBlockedPolicyDisabled ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerBlocksExecutionWhenAckTargetMissing(t *testing.T) {
	volume := authorityExecutorReadyVolume()
	result, err := (AuthorityExecutorReconciler{
		Client:                 &fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{volume}},
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.BlockedReason != AuthorityExecutorBlockedMutationTargetMissing ||
		result.AckEligibilityTargetMissingCount != 1 ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerHoldsWhenTerminalEvidenceMissing(t *testing.T) {
	volume := authorityExecutorReadyVolume()
	volume.Status.ReplicaReintegrations[0].FrontendFenced = false
	client := &fakeAuthorityExecutorClient{
		volumes:       []SwBlockVolumeObject{volume},
		eligibilities: []SwBlockReplicaEligibilityObject{authorityExecutorTarget()},
	}
	result, err := (AuthorityExecutorReconciler{
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.BlockedReason != AuthorityExecutorBlockedTerminalEvidence ||
		result.TerminalEvidenceMissingCount != 1 ||
		result.MutationAttemptCount != 0 ||
		len(client.writes) != 0 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
}

func TestAuthorityExecutorReconcilerWritesAckEligibilityStatusWhenTerminalEvidenceReady(t *testing.T) {
	client := &fakeAuthorityExecutorClient{
		volumes:       []SwBlockVolumeObject{authorityExecutorReadyVolume()},
		eligibilities: []SwBlockReplicaEligibilityObject{authorityExecutorTarget()},
	}
	now := time.Date(2026, 6, 23, 1, 2, 3, 0, time.UTC)
	result, err := (AuthorityExecutorReconciler{
		Client:                 client,
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
		Now:                    func() time.Time { return now },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.MutationAttemptCount != 1 ||
		result.AckEligibilityMutationAttempts != 1 ||
		result.BlockedReason != "" ||
		len(client.writes) != 1 {
		t.Fatalf("result=%+v writes=%+v", result, client.writes)
	}
	write := client.writes[0]
	if write.ref.Name != "returned-r1" {
		t.Fatalf("write ref=%+v", write.ref)
	}
	status := write.status
	if !status.AckEligibilityKnown ||
		!status.AckEligible ||
		!status.FrontendFencedAfterExecution ||
		!status.PrimaryUnchanged ||
		!status.DurableFrontierCovered ||
		!status.NoCrossVolumeIdentityChange ||
		status.ReasonCode != AuthorityExecutorReasonAckEligibilityRecorded ||
		!status.ObservedAt.Equal(now) {
		t.Fatalf("status=%+v", status)
	}
	if len(status.Conditions) != 1 || status.Conditions[0].Reason != AuthorityExecutorReasonAckEligibilityRecorded {
		t.Fatalf("conditions=%+v", status.Conditions)
	}
}

type fakeAuthorityExecutorClient struct {
	volumes       []SwBlockVolumeObject
	eligibilities []SwBlockReplicaEligibilityObject
	writes        []fakeReplicaEligibilityWrite
}

type fakeReplicaEligibilityWrite struct {
	ref    OperatorObjectRef
	status SwBlockReplicaEligibilityCRDStatus
}

func (f *fakeAuthorityExecutorClient) ListSwBlockVolumes(context.Context, string) ([]SwBlockVolumeObject, error) {
	return append([]SwBlockVolumeObject(nil), f.volumes...), nil
}

func (f *fakeAuthorityExecutorClient) ListSwBlockReplicaEligibilities(context.Context, string) ([]SwBlockReplicaEligibilityObject, error) {
	return append([]SwBlockReplicaEligibilityObject(nil), f.eligibilities...), nil
}

func (f *fakeAuthorityExecutorClient) WriteReplicaEligibilityStatus(_ context.Context, ref OperatorObjectRef, status SwBlockReplicaEligibilityCRDStatus) error {
	f.writes = append(f.writes, fakeReplicaEligibilityWrite{ref: ref, status: status})
	return nil
}

func authorityExecutorReadyVolume() SwBlockVolumeObject {
	return SwBlockVolumeObject{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "returned"},
		Status: SwBlockVolumeCRDStatus{
			VolumeID: "pvc-1",
			PVCName:  "demo",
			ReplicaReintegrations: []SwBlockVolumeCRDReturnedReplica{{
				ReplicaID:             "r1",
				State:                 ReturnedReplicaStateFenced,
				ReasonCode:            ReasonReturnedReplicaFrontendFenced,
				FrontendFenced:        true,
				FrontendPrimaryReady:  false,
				AckEligibilityKnown:   true,
				AckEligible:           false,
				DurableFrontierKnown:  true,
				DurableFrontierLSN:    52,
				RequiredFrontierKnown: true,
				RequiredFrontierLSN:   52,
				EvidenceRefs:          []string{"returned.txt"},
			}},
			ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
				ActionType:               ManagedVolumeActionReintegrateReturned,
				ReplicaID:                "r1",
				Decision:                 ReturnedReplicaExecutorContractDisabled,
				Reason:                   ReturnedReplicaExecutorContractReasonExecutorDisabled,
				OwnerExecutor:            "authority_recovery_executor",
				ExecutionEnabled:         false,
				MutationAllowed:          false,
				PreflightDecision:        ReturnedReplicaExecutorPreflightReady,
				PreflightReason:          ReturnedReplicaExecutorPreflightReasonSatisfied,
				AllowedMutationClass:     []string{AuthorityExecutorAllowedMutationAckEligibility},
				ForbiddenMutationClass:   []string{"frontend_publication", "rebuild_traffic", "failback"},
				TerminalEvidenceRequired: returnedReplicaTerminalEvidenceRequired(ManagedVolumeActionReintegrateReturned),
				EvidenceRefs:             []string{"contract.txt"},
			}},
		},
	}
}

func authorityExecutorRebuildVolume(executionEnabled, mutationAllowed bool) SwBlockVolumeObject {
	return SwBlockVolumeObject{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "rebuild"},
		Status: SwBlockVolumeCRDStatus{
			VolumeID: "pvc-rebuild",
			PVCName:  "rebuild-pvc",
			ReplicaReintegrations: []SwBlockVolumeCRDReturnedReplica{{
				ReplicaID:             "r1",
				State:                 ReturnedReplicaStateRecovering,
				ReasonCode:            ReasonCandidateFrontierBehind,
				FrontendFenced:        true,
				FrontendPrimaryReady:  false,
				AckEligibilityKnown:   true,
				AckEligible:           false,
				DurableFrontierKnown:  true,
				DurableFrontierLSN:    51,
				RequiredFrontierKnown: true,
				RequiredFrontierLSN:   52,
				EvidenceRefs:          []string{"rebuild.txt"},
			}},
			ExecutorContracts: []SwBlockVolumeCRDExecutorContract{{
				ActionType:               ManagedVolumeActionRebuildReturned,
				ReplicaID:                "r1",
				Decision:                 ReturnedReplicaExecutorContractDisabled,
				Reason:                   ReturnedReplicaExecutorContractReasonExecutorDisabled,
				OwnerExecutor:            "authority_recovery_executor",
				ExecutionEnabled:         executionEnabled,
				MutationAllowed:          mutationAllowed,
				PreflightDecision:        ReturnedReplicaExecutorPreflightReady,
				PreflightReason:          ReturnedReplicaExecutorPreflightReasonSatisfied,
				AllowedMutationClass:     []string{"rebuild_traffic"},
				ForbiddenMutationClass:   []string{"ack_eligibility", "frontend_publication", "failback"},
				TerminalEvidenceRequired: returnedReplicaTerminalEvidenceRequired(ManagedVolumeActionRebuildReturned),
				EvidenceRefs:             []string{"rebuild-contract.txt"},
			}},
		},
	}
}

func authorityExecutorTarget() SwBlockReplicaEligibilityObject {
	return SwBlockReplicaEligibilityObject{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockReplicaEligibilityKind,
			Namespace:  "kube-system",
			Name:       "returned-r1",
		},
		Spec: SwBlockReplicaEligibilitySpec{
			VolumeName: "returned",
			VolumeID:   "pvc-1",
			PVCName:    "demo",
			ReplicaID:  "r1",
		},
	}
}
