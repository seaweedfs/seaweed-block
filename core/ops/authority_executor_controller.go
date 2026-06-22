package ops

import (
	"context"
	"fmt"
)

type AuthorityExecutorClient interface {
	ListSwBlockVolumes(ctx context.Context, namespace string) ([]SwBlockVolumeObject, error)
}

type AuthorityExecutorReconciler struct {
	Namespace string
	Client    AuthorityExecutorClient
}

type AuthorityExecutorReconcileResult struct {
	VolumeCount                    int `json:"volumeCount"`
	ContractCount                  int `json:"contractCount"`
	DisabledContractCount          int `json:"disabledContractCount"`
	BlockedContractCount           int `json:"blockedContractCount"`
	TerminalEvidenceRequiredCount  int `json:"terminalEvidenceRequiredCount"`
	UnsafeExecutionContractCount   int `json:"unsafeExecutionContractCount"`
	MutationAttemptCount           int `json:"mutationAttemptCount"`
	AckEligibilityMutationAttempts int `json:"ackEligibilityMutationAttempts"`
}

func (r AuthorityExecutorReconciler) Reconcile(ctx context.Context) (AuthorityExecutorReconcileResult, error) {
	if r.Client == nil {
		return AuthorityExecutorReconcileResult{}, fmt.Errorf("authority executor client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	volumes, err := r.Client.ListSwBlockVolumes(ctx, namespace)
	if err != nil {
		return AuthorityExecutorReconcileResult{}, err
	}
	result := AuthorityExecutorReconcileResult{VolumeCount: len(volumes)}
	for _, volume := range volumes {
		for _, contract := range volume.Status.ExecutorContracts {
			if contract.ActionType != ManagedVolumeActionReintegrateReturned {
				continue
			}
			result.ContractCount++
			if contract.ExecutionEnabled || contract.MutationAllowed {
				result.UnsafeExecutionContractCount++
				continue
			}
			switch contract.Decision {
			case ReturnedReplicaExecutorContractDisabled:
				result.DisabledContractCount++
				if len(contract.TerminalEvidenceRequired) > 0 {
					result.TerminalEvidenceRequiredCount++
				}
			case ReturnedReplicaExecutorContractBlocked:
				result.BlockedContractCount++
			}
		}
	}
	if result.UnsafeExecutionContractCount > 0 {
		return result, fmt.Errorf("authority executor found %d execution-enabled or mutating contracts; execution is not supported", result.UnsafeExecutionContractCount)
	}
	return result, nil
}
