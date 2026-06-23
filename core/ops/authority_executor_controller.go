package ops

import (
	"context"
	"fmt"
	"time"
)

const (
	AuthorityExecutorAllowedMutationAckEligibility = "ack_eligibility"
	AuthorityExecutorBlockedPolicyDisabled         = "executor_policy_disabled"
	AuthorityExecutorBlockedMutationTargetMissing  = "ack_eligibility_mutation_target_missing"
	AuthorityExecutorBlockedTerminalEvidence       = "terminal_evidence_missing"
	AuthorityExecutorReasonAckEligibilityRecorded  = "ack_eligibility_recorded"
)

type AuthorityExecutorClient interface {
	ListSwBlockVolumes(ctx context.Context, namespace string) ([]SwBlockVolumeObject, error)
	ListSwBlockReplicaEligibilities(ctx context.Context, namespace string) ([]SwBlockReplicaEligibilityObject, error)
	WriteReplicaEligibilityStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockReplicaEligibilityCRDStatus) error
}

type AuthorityExecutorReconciler struct {
	Namespace              string
	Client                 AuthorityExecutorClient
	ExecutionRequested     bool
	ExecutionPolicyEnabled bool
	AllowedMutationClass   string
	Now                    func() time.Time
}

type AuthorityExecutorReconcileResult struct {
	VolumeCount                      int    `json:"volumeCount"`
	ContractCount                    int    `json:"contractCount"`
	DisabledContractCount            int    `json:"disabledContractCount"`
	BlockedContractCount             int    `json:"blockedContractCount"`
	TerminalEvidenceRequiredCount    int    `json:"terminalEvidenceRequiredCount"`
	TerminalEvidenceMissingCount     int    `json:"terminalEvidenceMissingCount"`
	AckEligibilityTargetMissingCount int    `json:"ackEligibilityTargetMissingCount"`
	UnsafeExecutionContractCount     int    `json:"unsafeExecutionContractCount"`
	MutationAttemptCount             int    `json:"mutationAttemptCount"`
	AckEligibilityMutationAttempts   int    `json:"ackEligibilityMutationAttempts"`
	BlockedReason                    string `json:"blockedReason,omitempty"`
}

func (r AuthorityExecutorReconciler) Reconcile(ctx context.Context) (AuthorityExecutorReconcileResult, error) {
	allowedMutationClass := defaultString(r.AllowedMutationClass, AuthorityExecutorAllowedMutationAckEligibility)
	if allowedMutationClass != AuthorityExecutorAllowedMutationAckEligibility {
		result := AuthorityExecutorReconcileResult{BlockedReason: "unsupported_mutation_class"}
		return result, fmt.Errorf("authority executor unsupported mutation class %q", allowedMutationClass)
	}
	if r.ExecutionRequested && !r.ExecutionPolicyEnabled {
		result := AuthorityExecutorReconcileResult{BlockedReason: AuthorityExecutorBlockedPolicyDisabled}
		return result, fmt.Errorf("authority executor execution is disabled by product policy")
	}
	if r.Client == nil {
		return AuthorityExecutorReconcileResult{}, fmt.Errorf("authority executor client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	volumes, err := r.Client.ListSwBlockVolumes(ctx, namespace)
	if err != nil {
		return AuthorityExecutorReconcileResult{}, err
	}
	var targets []SwBlockReplicaEligibilityObject
	if r.ExecutionRequested {
		targets, err = r.Client.ListSwBlockReplicaEligibilities(ctx, namespace)
		if err != nil {
			return AuthorityExecutorReconcileResult{}, err
		}
	}
	result := AuthorityExecutorReconcileResult{VolumeCount: len(volumes)}
	for _, volume := range volumes {
		for _, contract := range volume.Status.ExecutorContracts {
			if contract.ActionType != ManagedVolumeActionReintegrateReturned {
				if contract.ExecutionEnabled || contract.MutationAllowed {
					result.UnsafeExecutionContractCount++
				}
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
			if r.ExecutionRequested {
				if err := r.evaluateAckEligibility(ctx, &result, volume, contract, targets); err != nil {
					return result, err
				}
			}
		}
	}
	if result.UnsafeExecutionContractCount > 0 {
		return result, fmt.Errorf("authority executor found %d execution-enabled or mutating contracts; execution is not supported", result.UnsafeExecutionContractCount)
	}
	return result, nil
}

func (r AuthorityExecutorReconciler) evaluateAckEligibility(ctx context.Context, result *AuthorityExecutorReconcileResult, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, targets []SwBlockReplicaEligibilityObject) error {
	returned, ok := authorityExecutorTerminalEvidenceReady(volume, contract)
	if !ok {
		result.TerminalEvidenceMissingCount++
		result.BlockedReason = authorityExecutorFirstNonEmpty(result.BlockedReason, AuthorityExecutorBlockedTerminalEvidence)
		return nil
	}
	target, ok := authorityExecutorFindTarget(volume, contract, targets)
	if !ok {
		result.AckEligibilityTargetMissingCount++
		result.BlockedReason = authorityExecutorFirstNonEmpty(result.BlockedReason, AuthorityExecutorBlockedMutationTargetMissing)
		return nil
	}
	result.MutationAttemptCount++
	result.AckEligibilityMutationAttempts++
	if err := r.Client.WriteReplicaEligibilityStatus(ctx, target.Ref, authorityExecutorAckEligibilityStatus(r.now()(), volume, contract, returned)); err != nil {
		result.BlockedReason = "ack_eligibility_status_write_failed"
		return fmt.Errorf("write ACK eligibility status: %w", err)
	}
	return nil
}

func authorityExecutorTerminalEvidenceReady(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract) (SwBlockVolumeCRDReturnedReplica, bool) {
	if contract.Decision != ReturnedReplicaExecutorContractDisabled ||
		contract.Reason != ReturnedReplicaExecutorContractReasonExecutorDisabled ||
		contract.PreflightDecision != ReturnedReplicaExecutorPreflightReady ||
		contract.PreflightReason != ReturnedReplicaExecutorPreflightReasonSatisfied ||
		!authorityExecutorStringSliceContains(contract.AllowedMutationClass, AuthorityExecutorAllowedMutationAckEligibility) ||
		contract.ReplicaID == "" {
		return SwBlockVolumeCRDReturnedReplica{}, false
	}
	for _, returned := range volume.Status.ReplicaReintegrations {
		if returned.ReplicaID != contract.ReplicaID {
			continue
		}
		if !returned.FrontendFenced ||
			returned.FrontendPrimaryReady ||
			!returned.AckEligibilityKnown ||
			returned.AckEligible ||
			!returned.RequiredFrontierKnown ||
			!returned.DurableFrontierKnown ||
			returned.DurableFrontierLSN < returned.RequiredFrontierLSN {
			return SwBlockVolumeCRDReturnedReplica{}, false
		}
		return returned, true
	}
	return SwBlockVolumeCRDReturnedReplica{}, false
}

func authorityExecutorFindTarget(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, targets []SwBlockReplicaEligibilityObject) (SwBlockReplicaEligibilityObject, bool) {
	var matches []SwBlockReplicaEligibilityObject
	for _, target := range targets {
		if target.Spec.VolumeName == "" && target.Spec.VolumeID == "" && target.Spec.PVCName == "" {
			continue
		}
		if target.Spec.ReplicaID != contract.ReplicaID {
			continue
		}
		if target.Spec.VolumeName != "" && target.Spec.VolumeName != volume.Ref.Name {
			continue
		}
		if target.Spec.VolumeID != "" && target.Spec.VolumeID != volume.Status.VolumeID {
			continue
		}
		if target.Spec.PVCName != "" && target.Spec.PVCName != volume.Status.PVCName {
			continue
		}
		matches = append(matches, target)
	}
	if len(matches) != 1 {
		return SwBlockReplicaEligibilityObject{}, false
	}
	return matches[0], true
}

func authorityExecutorAckEligibilityStatus(now time.Time, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica) SwBlockReplicaEligibilityCRDStatus {
	evidenceRefs := appendUniqueStrings(nil, contract.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, returned.EvidenceRefs...)
	return SwBlockReplicaEligibilityCRDStatus{
		ObservedAt:                   now,
		Executor:                     defaultString(contract.OwnerExecutor, "authority_recovery_executor"),
		ReasonCode:                   AuthorityExecutorReasonAckEligibilityRecorded,
		AckEligibilityKnown:          true,
		AckEligible:                  true,
		FrontendFencedAfterExecution: returned.FrontendFenced && !returned.FrontendPrimaryReady,
		PrimaryUnchanged:             returned.FrontendFenced && !returned.FrontendPrimaryReady,
		DurableFrontierCovered:       returned.DurableFrontierKnown && returned.RequiredFrontierKnown && returned.DurableFrontierLSN >= returned.RequiredFrontierLSN,
		NoCrossVolumeIdentityChange:  true,
		Conditions: []ObservationCondition{{
			Type:     ConditionReady,
			Status:   "True",
			Reason:   AuthorityExecutorReasonAckEligibilityRecorded,
			Severity: "info",
			Message:  "ACK eligibility was recorded only after terminal returned-replica evidence stayed fenced and durable-frontier covered",
		}},
		EvidenceRefs: evidenceRefs,
		NonClaims: []string{
			"no_frontend_publication",
			"no_rebuild_traffic",
			"no_failback",
			"no_primary_authority_change",
			"no_cross_volume_mutation",
		},
	}
}

func (r AuthorityExecutorReconciler) now() func() time.Time {
	if r.Now != nil {
		return r.Now
	}
	return func() time.Time { return time.Now().UTC() }
}

func authorityExecutorStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func authorityExecutorFirstNonEmpty(current, fallback string) string {
	if current != "" {
		return current
	}
	return fallback
}
