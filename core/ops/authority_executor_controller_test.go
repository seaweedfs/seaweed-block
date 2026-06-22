package ops

import (
	"context"
	"testing"
)

func TestAuthorityExecutorReconcilerObservesDisabledContractsWithoutMutation(t *testing.T) {
	client := fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{{
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
	client := fakeAuthorityExecutorClient{volumes: []SwBlockVolumeObject{{
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
		Client:               fakeAuthorityExecutorClient{},
		AllowedMutationClass: "rebuild_traffic",
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected unsupported mutation class to fail closed, result=%+v", result)
	}
	if result.BlockedReason != "unsupported_mutation_class" || result.MutationAttemptCount != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAuthorityExecutorReconcilerRejectsExecutionWhenPolicyDisabled(t *testing.T) {
	result, err := (AuthorityExecutorReconciler{
		Client:             fakeAuthorityExecutorClient{},
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
	result, err := (AuthorityExecutorReconciler{
		Client:                 fakeAuthorityExecutorClient{},
		ExecutionRequested:     true,
		ExecutionPolicyEnabled: true,
		AllowedMutationClass:   AuthorityExecutorAllowedMutationAckEligibility,
	}).Reconcile(context.Background())
	if err == nil {
		t.Fatalf("expected missing ACK target to fail closed, result=%+v", result)
	}
	if result.BlockedReason != AuthorityExecutorBlockedMutationTargetMissing ||
		result.MutationAttemptCount != 0 ||
		result.AckEligibilityMutationAttempts != 0 {
		t.Fatalf("result=%+v", result)
	}
}

type fakeAuthorityExecutorClient struct {
	volumes []SwBlockVolumeObject
}

func (f fakeAuthorityExecutorClient) ListSwBlockVolumes(context.Context, string) ([]SwBlockVolumeObject, error) {
	return append([]SwBlockVolumeObject(nil), f.volumes...), nil
}
