package ops

import (
	"context"
	"fmt"
	"time"
)

const (
	AuthorityExecutorAllowedMutationAckEligibility     = "ack_eligibility"
	AuthorityExecutorAllowedMutationRebuildTraffic     = "rebuild_traffic"
	AuthorityExecutorBlockedPolicyDisabled             = "executor_policy_disabled"
	AuthorityExecutorBlockedMutationTargetMissing      = "ack_eligibility_mutation_target_missing"
	AuthorityExecutorBlockedRebuildTargetMissing       = "rebuild_target_missing"
	AuthorityExecutorBlockedTerminalEvidence           = "terminal_evidence_missing"
	AuthorityExecutorReasonAckEligibilityRecorded      = "ack_eligibility_recorded"
	AuthorityExecutorReasonRebuildPlanned              = "rebuild_progress_planned"
	AuthorityExecutorReasonRebuildRunning              = "rebuild_runtime_running"
	AuthorityExecutorReasonRebuildCaughtUp             = "rebuild_runtime_caught_up"
	AuthorityExecutorReasonRebuildRuntimeFailed        = "rebuild_runtime_failed"
	AuthorityExecutorReasonRebuildRuntimeTargetMissing = "rebuild_runtime_target_missing"
	AuthorityExecutorPublicationDecisionBlocked        = "blocked"
	AuthorityExecutorPublicationDecisionDisabled       = "disabled"
	AuthorityExecutorPublicationReasonCaughtUpRequired = "rebuild_caught_up_required"
	AuthorityExecutorPublicationReasonPolicyDisabled   = "publication_policy_disabled"
	AuthorityExecutorFrontendPublicationReasonDisabled = "frontend_publication_policy_disabled"
)

type AuthorityExecutorClient interface {
	ListSwBlockVolumes(ctx context.Context, namespace string) ([]SwBlockVolumeObject, error)
	ListSwBlockReplicaEligibilities(ctx context.Context, namespace string) ([]SwBlockReplicaEligibilityObject, error)
	ListSwBlockReplicaRebuilds(ctx context.Context, namespace string) ([]SwBlockReplicaRebuildObject, error)
	WriteReplicaEligibilityStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockReplicaEligibilityCRDStatus) error
	WriteReplicaRebuildStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockReplicaRebuildCRDStatus) error
}

type AuthorityExecutorReconciler struct {
	Namespace              string
	Client                 AuthorityExecutorClient
	RebuildRuntime         AuthorityRebuildRuntime
	ExecutionRequested     bool
	ExecutionPolicyEnabled bool
	AllowedMutationClass   string
	Now                    func() time.Time
}

type AuthorityRebuildRuntime interface {
	ExecuteRebuild(ctx context.Context, req AuthorityRebuildRuntimeRequest) (AuthorityRebuildRuntimeResult, error)
}

type AuthorityRebuildRuntimeRequest struct {
	VolumeName            string   `json:"volumeName"`
	VolumeID              string   `json:"volumeID"`
	PVCName               string   `json:"pvcName"`
	ReplicaID             string   `json:"replicaID"`
	RuntimeEndpoint       string   `json:"runtimeEndpoint,omitempty"`
	TargetDataAddr        string   `json:"targetDataAddr,omitempty"`
	SessionID             uint64   `json:"sessionID,omitempty"`
	Epoch                 uint64   `json:"epoch,omitempty"`
	EndpointVersion       uint64   `json:"endpointVersion,omitempty"`
	FromLSN               uint64   `json:"fromLsn,omitempty"`
	FrontierHintLSN       uint64   `json:"frontierHintLsn,omitempty"`
	BasePinLSN            uint64   `json:"basePinLsn,omitempty"`
	DurableFrontierKnown  bool     `json:"durableFrontierKnown"`
	DurableFrontierLSN    uint64   `json:"durableFrontierLsn"`
	RequiredFrontierKnown bool     `json:"requiredFrontierKnown"`
	RequiredFrontierLSN   uint64   `json:"requiredFrontierLsn"`
	FrontendFenced        bool     `json:"frontendFenced"`
	FrontendPrimaryReady  bool     `json:"frontendPrimaryReady"`
	NoFrontendPublication bool     `json:"noFrontendPublication"`
	NoCrossVolumeMutation bool     `json:"noCrossVolumeMutation"`
	EvidenceRefs          []string `json:"evidenceRefs,omitempty"`
}

type AuthorityRebuildRuntimeResult struct {
	RuntimeState         string   `json:"runtimeState,omitempty"`
	DurableFrontierKnown bool     `json:"durableFrontierKnown"`
	DurableFrontierLSN   uint64   `json:"durableFrontierLsn"`
	EvidenceRefs         []string `json:"evidenceRefs,omitempty"`
}

type AuthorityExecutorReconcileResult struct {
	VolumeCount                      int    `json:"volumeCount"`
	ContractCount                    int    `json:"contractCount"`
	DisabledContractCount            int    `json:"disabledContractCount"`
	BlockedContractCount             int    `json:"blockedContractCount"`
	TerminalEvidenceRequiredCount    int    `json:"terminalEvidenceRequiredCount"`
	TerminalEvidenceMissingCount     int    `json:"terminalEvidenceMissingCount"`
	AckEligibilityTargetMissingCount int    `json:"ackEligibilityTargetMissingCount"`
	RebuildTargetMissingCount        int    `json:"rebuildTargetMissingCount"`
	RebuildRuntimeTargetMissingCount int    `json:"rebuildRuntimeTargetMissingCount"`
	UnsafeExecutionContractCount     int    `json:"unsafeExecutionContractCount"`
	MutationAttemptCount             int    `json:"mutationAttemptCount"`
	AckEligibilityMutationAttempts   int    `json:"ackEligibilityMutationAttempts"`
	RebuildProgressMutationAttempts  int    `json:"rebuildProgressMutationAttempts"`
	BlockedReason                    string `json:"blockedReason,omitempty"`
}

func (r AuthorityExecutorReconciler) Reconcile(ctx context.Context) (AuthorityExecutorReconcileResult, error) {
	allowedMutationClass := defaultString(r.AllowedMutationClass, AuthorityExecutorAllowedMutationAckEligibility)
	if allowedMutationClass != AuthorityExecutorAllowedMutationAckEligibility && allowedMutationClass != AuthorityExecutorAllowedMutationRebuildTraffic {
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
	var rebuildTargets []SwBlockReplicaRebuildObject
	if r.ExecutionRequested && allowedMutationClass == AuthorityExecutorAllowedMutationAckEligibility {
		targets, err = r.Client.ListSwBlockReplicaEligibilities(ctx, namespace)
		if err != nil {
			return AuthorityExecutorReconcileResult{}, err
		}
		rebuildTargets, err = r.Client.ListSwBlockReplicaRebuilds(ctx, namespace)
		if err != nil {
			return AuthorityExecutorReconcileResult{}, err
		}
	}
	if r.ExecutionRequested && allowedMutationClass == AuthorityExecutorAllowedMutationRebuildTraffic {
		rebuildTargets, err = r.Client.ListSwBlockReplicaRebuilds(ctx, namespace)
		if err != nil {
			return AuthorityExecutorReconcileResult{}, err
		}
	}
	result := AuthorityExecutorReconcileResult{VolumeCount: len(volumes)}
	for _, volume := range volumes {
		for _, contract := range volume.Status.ExecutorContracts {
			if contract.ActionType != ManagedVolumeActionReintegrateReturned && contract.ActionType != ManagedVolumeActionRebuildReturned {
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
			if r.ExecutionRequested && allowedMutationClass == AuthorityExecutorAllowedMutationAckEligibility && contract.ActionType == ManagedVolumeActionReintegrateReturned {
				if err := r.evaluateAckEligibility(ctx, &result, volume, contract, targets); err != nil {
					return result, err
				}
			}
			if r.ExecutionRequested && allowedMutationClass == AuthorityExecutorAllowedMutationAckEligibility && contract.ActionType == ManagedVolumeActionRebuildReturned {
				if err := r.evaluateAckEligibilityAfterRebuild(ctx, &result, volume, contract, targets, rebuildTargets); err != nil {
					return result, err
				}
			}
			if r.ExecutionRequested && allowedMutationClass == AuthorityExecutorAllowedMutationRebuildTraffic && contract.ActionType == ManagedVolumeActionRebuildReturned {
				if err := r.evaluateRebuildPlanning(ctx, &result, volume, contract, rebuildTargets); err != nil {
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

func (r AuthorityExecutorReconciler) evaluateAckEligibilityAfterRebuild(ctx context.Context, result *AuthorityExecutorReconcileResult, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, targets []SwBlockReplicaEligibilityObject, rebuildTargets []SwBlockReplicaRebuildObject) error {
	returned, ok := authorityExecutorRebuildEvidenceReady(volume, contract)
	if !ok {
		result.TerminalEvidenceMissingCount++
		result.BlockedReason = authorityExecutorFirstNonEmpty(result.BlockedReason, AuthorityExecutorBlockedTerminalEvidence)
		return nil
	}
	rebuildTarget, ok := authorityExecutorFindRebuildTarget(volume, contract, rebuildTargets)
	if !ok || !authorityExecutorRebuildCaughtUpPublicationReady(rebuildTarget.Status) {
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
	status := authorityExecutorAckEligibilityStatusFromRebuild(r.now()(), volume, contract, returned, rebuildTarget.Status)
	if err := r.Client.WriteReplicaEligibilityStatus(ctx, target.Ref, status); err != nil {
		result.BlockedReason = "ack_eligibility_status_write_failed"
		return fmt.Errorf("write rebuild ACK eligibility status: %w", err)
	}
	return nil
}

func (r AuthorityExecutorReconciler) evaluateRebuildPlanning(ctx context.Context, result *AuthorityExecutorReconcileResult, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, targets []SwBlockReplicaRebuildObject) error {
	returned, ok := authorityExecutorRebuildEvidenceReady(volume, contract)
	if !ok {
		result.TerminalEvidenceMissingCount++
		result.BlockedReason = authorityExecutorFirstNonEmpty(result.BlockedReason, AuthorityExecutorBlockedTerminalEvidence)
		return nil
	}
	target, ok := authorityExecutorFindRebuildTarget(volume, contract, targets)
	if !ok {
		result.RebuildTargetMissingCount++
		result.BlockedReason = authorityExecutorFirstNonEmpty(result.BlockedReason, AuthorityExecutorBlockedRebuildTargetMissing)
		return nil
	}
	result.MutationAttemptCount++
	result.RebuildProgressMutationAttempts++
	runtime := r.RebuildRuntime
	if runtime == nil && target.Spec.RuntimeEndpoint != "" {
		runtime = NewHTTPAuthorityRebuildRuntime(target.Spec.RuntimeEndpoint, nil)
	}
	if runtime == nil {
		if err := r.Client.WriteReplicaRebuildStatus(ctx, target.Ref, authorityExecutorRebuildPlannedStatus(r.now()(), volume, contract, returned)); err != nil {
			result.BlockedReason = "rebuild_status_write_failed"
			return fmt.Errorf("write rebuild status: %w", err)
		}
		return nil
	}
	if !authorityExecutorRebuildRuntimeTargetReady(target.Spec) {
		result.RebuildRuntimeTargetMissingCount++
		result.BlockedReason = authorityExecutorFirstNonEmpty(result.BlockedReason, AuthorityExecutorReasonRebuildRuntimeTargetMissing)
		if err := r.Client.WriteReplicaRebuildStatus(ctx, target.Ref, authorityExecutorRebuildBlockedStatus(r.now()(), volume, contract, returned, AuthorityRebuildRuntimeResult{}, AuthorityExecutorReasonRebuildRuntimeTargetMissing)); err != nil {
			result.BlockedReason = "rebuild_status_write_failed"
			return fmt.Errorf("write missing runtime target status: %w", err)
		}
		return nil
	}
	if err := r.Client.WriteReplicaRebuildStatus(ctx, target.Ref, authorityExecutorRebuildRunningStatus(r.now()(), volume, contract, returned)); err != nil {
		result.BlockedReason = "rebuild_status_write_failed"
		return fmt.Errorf("write rebuild status: %w", err)
	}
	runtimeResult, err := runtime.ExecuteRebuild(ctx, authorityExecutorRebuildRuntimeRequest(volume, contract, returned, target.Spec))
	if err != nil {
		result.BlockedReason = AuthorityExecutorReasonRebuildRuntimeFailed
		if writeErr := r.Client.WriteReplicaRebuildStatus(ctx, target.Ref, authorityExecutorRebuildBlockedStatus(r.now()(), volume, contract, returned, runtimeResult, AuthorityExecutorReasonRebuildRuntimeFailed)); writeErr != nil {
			return fmt.Errorf("write failed rebuild status after runtime error %v: %w", err, writeErr)
		}
		return fmt.Errorf("execute rebuild runtime: %w", err)
	}
	if runtimeResult.RuntimeState == "started" {
		return nil
	}
	if !runtimeResult.DurableFrontierKnown || !returned.RequiredFrontierKnown || runtimeResult.DurableFrontierLSN < returned.RequiredFrontierLSN {
		result.BlockedReason = AuthorityExecutorBlockedTerminalEvidence
		if err := r.Client.WriteReplicaRebuildStatus(ctx, target.Ref, authorityExecutorRebuildBlockedStatus(r.now()(), volume, contract, returned, runtimeResult, AuthorityExecutorBlockedTerminalEvidence)); err != nil {
			result.BlockedReason = "rebuild_status_write_failed"
			return fmt.Errorf("write incomplete rebuild status: %w", err)
		}
		return nil
	}
	if err := r.Client.WriteReplicaRebuildStatus(ctx, target.Ref, authorityExecutorRebuildCaughtUpStatus(r.now()(), volume, contract, returned, runtimeResult)); err != nil {
		result.BlockedReason = "rebuild_status_write_failed"
		return fmt.Errorf("write caught-up rebuild status: %w", err)
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

func authorityExecutorRebuildEvidenceReady(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract) (SwBlockVolumeCRDReturnedReplica, bool) {
	if contract.Decision != ReturnedReplicaExecutorContractDisabled ||
		contract.Reason != ReturnedReplicaExecutorContractReasonExecutorDisabled ||
		contract.PreflightDecision != ReturnedReplicaExecutorPreflightReady ||
		contract.PreflightReason != ReturnedReplicaExecutorPreflightReasonSatisfied ||
		!authorityExecutorStringSliceContains(contract.AllowedMutationClass, AuthorityExecutorAllowedMutationRebuildTraffic) ||
		contract.ReplicaID == "" {
		return SwBlockVolumeCRDReturnedReplica{}, false
	}
	for _, returned := range volume.Status.ReplicaReintegrations {
		if returned.ReplicaID != contract.ReplicaID {
			continue
		}
		if !returned.FrontendFenced ||
			returned.FrontendPrimaryReady ||
			!returned.RequiredFrontierKnown ||
			!returned.DurableFrontierKnown ||
			returned.DurableFrontierLSN >= returned.RequiredFrontierLSN {
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

func authorityExecutorFindRebuildTarget(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, targets []SwBlockReplicaRebuildObject) (SwBlockReplicaRebuildObject, bool) {
	var matches []SwBlockReplicaRebuildObject
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
		return SwBlockReplicaRebuildObject{}, false
	}
	return matches[0], true
}

func authorityExecutorRebuildCaughtUpPublicationReady(status SwBlockReplicaRebuildCRDStatus) bool {
	return status.State == "caught_up" &&
		status.ReasonCode == AuthorityExecutorReasonRebuildCaughtUp &&
		status.RebuildTrafficStarted &&
		status.DurableFrontierKnown &&
		status.RequiredFrontierKnown &&
		status.DurableFrontierCaughtUp &&
		status.DurableFrontierLSN >= status.RequiredFrontierLSN &&
		status.PublicationDecision == AuthorityExecutorPublicationDecisionDisabled &&
		status.PublicationReason == AuthorityExecutorPublicationReasonPolicyDisabled &&
		!status.PublicationMutationAllowed &&
		status.NoFrontendPublication &&
		status.NoCrossVolumeIdentityChange
}

func authorityExecutorRebuildRuntimeTargetReady(spec SwBlockReplicaRebuildSpec) bool {
	return spec.RuntimeEndpoint != "" &&
		spec.SessionID != 0 &&
		spec.Epoch != 0 &&
		spec.EndpointVersion != 0 &&
		spec.FrontierHintLSN != 0
}

func authorityExecutorAckEligibilityStatus(now time.Time, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica) SwBlockReplicaEligibilityCRDStatus {
	evidenceRefs := appendUniqueStrings(nil, contract.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, returned.EvidenceRefs...)
	return SwBlockReplicaEligibilityCRDStatus{
		ObservedAt:                         now,
		Executor:                           defaultString(contract.OwnerExecutor, "authority_recovery_executor"),
		ReasonCode:                         AuthorityExecutorReasonAckEligibilityRecorded,
		AckEligibilityKnown:                true,
		AckEligible:                        true,
		FrontendFencedAfterExecution:       returned.FrontendFenced && !returned.FrontendPrimaryReady,
		PrimaryUnchanged:                   returned.FrontendFenced && !returned.FrontendPrimaryReady,
		DurableFrontierCovered:             returned.DurableFrontierKnown && returned.RequiredFrontierKnown && returned.DurableFrontierLSN >= returned.RequiredFrontierLSN,
		NoCrossVolumeIdentityChange:        true,
		FrontendPublicationDecision:        AuthorityExecutorPublicationDecisionDisabled,
		FrontendPublicationReason:          AuthorityExecutorFrontendPublicationReasonDisabled,
		FrontendPublicationMutationAllowed: false,
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

func authorityExecutorAckEligibilityStatusFromRebuild(now time.Time, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica, rebuild SwBlockReplicaRebuildCRDStatus) SwBlockReplicaEligibilityCRDStatus {
	evidenceRefs := appendUniqueStrings(nil, contract.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, returned.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, rebuild.EvidenceRefs...)
	return SwBlockReplicaEligibilityCRDStatus{
		ObservedAt:                         now,
		Executor:                           defaultString(contract.OwnerExecutor, "authority_recovery_executor"),
		ReasonCode:                         AuthorityExecutorReasonAckEligibilityRecorded,
		AckEligibilityKnown:                true,
		AckEligible:                        true,
		FrontendFencedAfterExecution:       returned.FrontendFenced && !returned.FrontendPrimaryReady,
		PrimaryUnchanged:                   returned.FrontendFenced && !returned.FrontendPrimaryReady,
		DurableFrontierCovered:             rebuild.DurableFrontierKnown && rebuild.RequiredFrontierKnown && rebuild.DurableFrontierLSN >= rebuild.RequiredFrontierLSN,
		NoCrossVolumeIdentityChange:        rebuild.NoCrossVolumeIdentityChange,
		FrontendPublicationDecision:        AuthorityExecutorPublicationDecisionDisabled,
		FrontendPublicationReason:          AuthorityExecutorFrontendPublicationReasonDisabled,
		FrontendPublicationMutationAllowed: false,
		Conditions: []ObservationCondition{{
			Type:     ConditionReady,
			Status:   "True",
			Reason:   AuthorityExecutorReasonAckEligibilityRecorded,
			Severity: "info",
			Message:  "ACK eligibility was recorded only after returned-replica rebuild reached the required durable frontier",
		}},
		EvidenceRefs: evidenceRefs,
		NonClaims: []string{
			"no_frontend_publication",
			"no_failback",
			"no_primary_authority_change",
			"no_cross_volume_mutation",
		},
	}
}

func authorityExecutorRebuildPlannedStatus(now time.Time, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica) SwBlockReplicaRebuildCRDStatus {
	evidenceRefs := appendUniqueStrings(nil, contract.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, returned.EvidenceRefs...)
	return SwBlockReplicaRebuildCRDStatus{
		ObservedAt:                  now,
		Executor:                    defaultString(contract.OwnerExecutor, "authority_recovery_executor"),
		State:                       "planned",
		ReasonCode:                  AuthorityExecutorReasonRebuildPlanned,
		FrontendFencedBeforeRebuild: returned.FrontendFenced && !returned.FrontendPrimaryReady,
		PrimaryUnchanged:            returned.FrontendFenced && !returned.FrontendPrimaryReady,
		DurableFrontierKnown:        returned.DurableFrontierKnown,
		DurableFrontierLSN:          returned.DurableFrontierLSN,
		RequiredFrontierKnown:       returned.RequiredFrontierKnown,
		RequiredFrontierLSN:         returned.RequiredFrontierLSN,
		DurableFrontierCaughtUp:     returned.DurableFrontierKnown && returned.RequiredFrontierKnown && returned.DurableFrontierLSN >= returned.RequiredFrontierLSN,
		RebuildTrafficStarted:       false,
		PublicationDecision:         AuthorityExecutorPublicationDecisionBlocked,
		PublicationReason:           AuthorityExecutorPublicationReasonCaughtUpRequired,
		PublicationMutationAllowed:  false,
		NoFrontendPublication:       true,
		NoCrossVolumeIdentityChange: true,
		Conditions: []ObservationCondition{{
			Type:     ConditionRecovering,
			Status:   "True",
			Reason:   AuthorityExecutorReasonRebuildPlanned,
			Severity: "info",
			Message:  "returned-replica rebuild/catch-up was planned; no rebuild traffic or frontend publication has started",
		}},
		EvidenceRefs: evidenceRefs,
		NonClaims: []string{
			"no_rebuild_data_movement",
			"no_frontend_publication",
			"no_failback",
			"no_primary_authority_change",
			"no_cross_volume_mutation",
		},
	}
}

func authorityExecutorRebuildRunningStatus(now time.Time, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica) SwBlockReplicaRebuildCRDStatus {
	status := authorityExecutorRebuildPlannedStatus(now, volume, contract, returned)
	status.State = "running"
	status.ReasonCode = AuthorityExecutorReasonRebuildRunning
	status.RebuildTrafficStarted = true
	status.Conditions = []ObservationCondition{{
		Type:     ConditionRecovering,
		Status:   "True",
		Reason:   AuthorityExecutorReasonRebuildRunning,
		Severity: "info",
		Message:  "returned-replica rebuild/catch-up runtime was invoked; frontend publication remains disabled",
	}}
	status.NonClaims = []string{
		"no_frontend_publication",
		"no_failback",
		"no_primary_authority_change",
		"no_cross_volume_mutation",
	}
	return status
}

func authorityExecutorRebuildCaughtUpStatus(now time.Time, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica, runtimeResult AuthorityRebuildRuntimeResult) SwBlockReplicaRebuildCRDStatus {
	evidenceRefs := appendUniqueStrings(nil, contract.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, returned.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, runtimeResult.EvidenceRefs...)
	return SwBlockReplicaRebuildCRDStatus{
		ObservedAt:                  now,
		Executor:                    defaultString(contract.OwnerExecutor, "authority_recovery_executor"),
		State:                       "caught_up",
		ReasonCode:                  AuthorityExecutorReasonRebuildCaughtUp,
		FrontendFencedBeforeRebuild: returned.FrontendFenced && !returned.FrontendPrimaryReady,
		PrimaryUnchanged:            returned.FrontendFenced && !returned.FrontendPrimaryReady,
		DurableFrontierKnown:        runtimeResult.DurableFrontierKnown,
		DurableFrontierLSN:          runtimeResult.DurableFrontierLSN,
		RequiredFrontierKnown:       returned.RequiredFrontierKnown,
		RequiredFrontierLSN:         returned.RequiredFrontierLSN,
		DurableFrontierCaughtUp:     runtimeResult.DurableFrontierKnown && returned.RequiredFrontierKnown && runtimeResult.DurableFrontierLSN >= returned.RequiredFrontierLSN,
		RebuildTrafficStarted:       true,
		PublicationDecision:         AuthorityExecutorPublicationDecisionDisabled,
		PublicationReason:           AuthorityExecutorPublicationReasonPolicyDisabled,
		PublicationMutationAllowed:  false,
		NoFrontendPublication:       true,
		NoCrossVolumeIdentityChange: true,
		Conditions: []ObservationCondition{{
			Type:     ConditionRecovered,
			Status:   "True",
			Reason:   AuthorityExecutorReasonRebuildCaughtUp,
			Severity: "info",
			Message:  "returned-replica rebuild/catch-up reached the required durable frontier; frontend publication remains disabled",
		}},
		EvidenceRefs: evidenceRefs,
		NonClaims: []string{
			"no_frontend_publication",
			"no_failback",
			"no_primary_authority_change",
			"no_cross_volume_mutation",
		},
	}
}

func authorityExecutorRebuildBlockedStatus(now time.Time, volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica, runtimeResult AuthorityRebuildRuntimeResult, reason string) SwBlockReplicaRebuildCRDStatus {
	status := authorityExecutorRebuildRunningStatus(now, volume, contract, returned)
	status.State = "blocked"
	status.ReasonCode = reason
	status.DurableFrontierKnown = runtimeResult.DurableFrontierKnown
	if runtimeResult.DurableFrontierKnown {
		status.DurableFrontierLSN = runtimeResult.DurableFrontierLSN
	}
	status.DurableFrontierCaughtUp = runtimeResult.DurableFrontierKnown && returned.RequiredFrontierKnown && runtimeResult.DurableFrontierLSN >= returned.RequiredFrontierLSN
	status.EvidenceRefs = appendUniqueStrings(status.EvidenceRefs, runtimeResult.EvidenceRefs...)
	status.Conditions = []ObservationCondition{{
		Type:     ConditionBlocked,
		Status:   "True",
		Reason:   reason,
		Severity: "warning",
		Message:  "returned-replica rebuild/catch-up did not produce terminal durable-frontier evidence",
	}}
	return status
}

func authorityExecutorRebuildRuntimeRequest(volume SwBlockVolumeObject, contract SwBlockVolumeCRDExecutorContract, returned SwBlockVolumeCRDReturnedReplica, spec SwBlockReplicaRebuildSpec) AuthorityRebuildRuntimeRequest {
	evidenceRefs := appendUniqueStrings(nil, contract.EvidenceRefs...)
	evidenceRefs = appendUniqueStrings(evidenceRefs, returned.EvidenceRefs...)
	return AuthorityRebuildRuntimeRequest{
		VolumeName:            volume.Ref.Name,
		VolumeID:              volume.Status.VolumeID,
		PVCName:               volume.Status.PVCName,
		ReplicaID:             contract.ReplicaID,
		RuntimeEndpoint:       spec.RuntimeEndpoint,
		TargetDataAddr:        spec.TargetDataAddr,
		SessionID:             spec.SessionID,
		Epoch:                 spec.Epoch,
		EndpointVersion:       spec.EndpointVersion,
		FromLSN:               spec.FromLSN,
		FrontierHintLSN:       spec.FrontierHintLSN,
		BasePinLSN:            spec.BasePinLSN,
		DurableFrontierKnown:  returned.DurableFrontierKnown,
		DurableFrontierLSN:    returned.DurableFrontierLSN,
		RequiredFrontierKnown: returned.RequiredFrontierKnown,
		RequiredFrontierLSN:   returned.RequiredFrontierLSN,
		FrontendFenced:        returned.FrontendFenced,
		FrontendPrimaryReady:  returned.FrontendPrimaryReady,
		NoFrontendPublication: true,
		NoCrossVolumeMutation: true,
		EvidenceRefs:          evidenceRefs,
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
