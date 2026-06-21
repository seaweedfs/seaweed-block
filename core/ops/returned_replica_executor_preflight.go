package ops

const (
	ReturnedReplicaExecutorPreflightReady = "ready"
	ReturnedReplicaExecutorPreflightHold  = "hold"

	ReturnedReplicaExecutorPreflightReasonSatisfied             = "preconditions_satisfied"
	ReturnedReplicaExecutorPreflightReasonActionNotAllowed      = "action_not_allowed"
	ReturnedReplicaExecutorPreflightReasonAmbiguousReplica      = "ambiguous_returned_replica"
	ReturnedReplicaExecutorPreflightReasonNoReturnedReplica     = "no_returned_replica"
	ReturnedReplicaExecutorPreflightReasonFrontendNotFenced     = "returned_replica_frontend_not_fenced"
	ReturnedReplicaExecutorPreflightReasonAckEligibilityUnknown = "returned_replica_ack_eligibility_unknown"
	ReturnedReplicaExecutorPreflightReasonAckEligible           = "returned_replica_ack_eligible"
	ReturnedReplicaExecutorPreflightReasonMissingFrontier       = "required_frontier_missing"
	ReturnedReplicaExecutorPreflightReasonDurableMissing        = "durable_frontier_missing"
	ReturnedReplicaExecutorPreflightReasonFrontierBehind        = "returned_replica_frontier_behind"
	ReturnedReplicaExecutorPreflightReasonWrongExecutor         = "wrong_owner_executor"
	ReturnedReplicaExecutorPreflightReasonUnsupportedMode       = "unsupported_action_mode"
	ReturnedReplicaExecutorPreflightReasonUnsupportedMutation   = "unexpected_mutation_permission"
)

// ReturnedReplicaExecutorPreflight is the non-mutating handoff contract for a
// future authority executor. It deliberately does not perform reintegration; it
// records whether the already-published returned-replica facts are sufficient
// to let a later bounded executor consider the action.
type ReturnedReplicaExecutorPreflight struct {
	ActionType             string   `json:"action_type"`
	VolumeID               string   `json:"volume_id,omitempty"`
	ReplicaID              string   `json:"replica_id,omitempty"`
	Decision               string   `json:"decision"`
	Reason                 string   `json:"reason"`
	Mode                   string   `json:"mode"`
	SideEffectClass        string   `json:"side_effect_class"`
	OwnerExecutor          string   `json:"owner_executor"`
	MutationAllowed        bool     `json:"mutation_allowed"`
	FrontendFenced         bool     `json:"frontend_fenced"`
	AckEligibilityKnown    bool     `json:"ack_eligibility_known"`
	AckEligible            bool     `json:"ack_eligible"`
	DurableFrontierKnown   bool     `json:"durable_frontier_known"`
	DurableFrontierLSN     uint64   `json:"durable_frontier_lsn,omitempty"`
	RequiredFrontierKnown  bool     `json:"required_frontier_known"`
	RequiredFrontierLSN    uint64   `json:"required_frontier_lsn,omitempty"`
	EvidenceRequired       string   `json:"evidence_required,omitempty"`
	EvidenceRefs           []string `json:"evidence_refs,omitempty"`
	ForbiddenMutationClass []string `json:"forbidden_mutation_class,omitempty"`
}

func ReturnedReplicaExecutorPreflights(projection ManagedVolumeProjection) []ReturnedReplicaExecutorPreflight {
	action := returnedReplicaReintegrateAction(projection.Actions)
	if len(projection.ReplicaReintegrations) == 0 && action == nil {
		return nil
	}
	preflight := ReturnedReplicaExecutorPreflight{
		ActionType:             ManagedVolumeActionReintegrateReturned,
		VolumeID:               projection.VolumeID,
		Decision:               ReturnedReplicaExecutorPreflightHold,
		Reason:                 ReturnedReplicaExecutorPreflightReasonActionNotAllowed,
		Mode:                   ManagedVolumeActionModeDryRun,
		SideEffectClass:        ManagedVolumeSideEffectAuthorityMutating,
		OwnerExecutor:          "authority_recovery_executor",
		MutationAllowed:        false,
		EvidenceRequired:       "returned_replica_reintegration_evidence",
		ForbiddenMutationClass: []string{"ack_eligibility", "frontend_publication", "rebuild_traffic", "failback"},
	}
	if action == nil {
		if returned, reason := returnedReplicaForExecutorPreflight(projection.ReplicaReintegrations, ""); reason == "" {
			populateReturnedReplicaPreflight(&preflight, returned)
			preflight.Reason = returnedReplicaPreflightHoldReason(returned)
		}
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	preflight.Mode = action.Mode
	preflight.SideEffectClass = action.SideEffectClass
	preflight.OwnerExecutor = action.OwnerExecutor
	preflight.EvidenceRequired = action.EvidenceRequired
	preflight.EvidenceRefs = append([]string(nil), action.EvidenceRefs...)
	if action.Decision != ManagedVolumeActionDecisionAllowed {
		preflight.Reason = defaultString(action.DecisionReason, ReturnedReplicaExecutorPreflightReasonActionNotAllowed)
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	if action.Mode != ManagedVolumeActionModeDryRun {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonUnsupportedMode
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	if action.SideEffectClass != ManagedVolumeSideEffectAuthorityMutating || action.OwnerExecutor != "authority_recovery_executor" {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonWrongExecutor
		return []ReturnedReplicaExecutorPreflight{preflight}
	}

	returned, reason := returnedReplicaForExecutorPreflight(projection.ReplicaReintegrations, action.Target)
	if reason != "" {
		preflight.Reason = reason
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	populateReturnedReplicaPreflight(&preflight, returned)
	if len(preflight.EvidenceRefs) == 0 {
		preflight.EvidenceRefs = append([]string(nil), returned.EvidenceRefs...)
	}
	if !returned.FrontendFenced || returned.FrontendPrimaryReady {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonFrontendNotFenced
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	if !returned.AckEligibilityKnown {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonAckEligibilityUnknown
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	if returned.AckEligible {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonAckEligible
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	if !returned.RequiredFrontierKnown {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonMissingFrontier
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	if !returned.DurableFrontierKnown {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonDurableMissing
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	if returned.DurableFrontierLSN < returned.RequiredFrontierLSN {
		preflight.Reason = ReturnedReplicaExecutorPreflightReasonFrontierBehind
		return []ReturnedReplicaExecutorPreflight{preflight}
	}
	preflight.Decision = ReturnedReplicaExecutorPreflightReady
	preflight.Reason = ReturnedReplicaExecutorPreflightReasonSatisfied
	return []ReturnedReplicaExecutorPreflight{preflight}
}

func populateReturnedReplicaPreflight(preflight *ReturnedReplicaExecutorPreflight, returned ReturnedReplicaProjection) {
	preflight.ReplicaID = returned.ReplicaID
	preflight.FrontendFenced = returned.FrontendFenced
	preflight.AckEligibilityKnown = returned.AckEligibilityKnown
	preflight.AckEligible = returned.AckEligible
	preflight.DurableFrontierKnown = returned.DurableFrontierKnown
	preflight.DurableFrontierLSN = returned.DurableFrontierLSN
	preflight.RequiredFrontierKnown = returned.RequiredFrontierKnown
	preflight.RequiredFrontierLSN = returned.RequiredFrontierLSN
}

func returnedReplicaPreflightHoldReason(returned ReturnedReplicaProjection) string {
	if !returned.FrontendFenced || returned.FrontendPrimaryReady {
		return ReturnedReplicaExecutorPreflightReasonFrontendNotFenced
	}
	if !returned.AckEligibilityKnown {
		return ReturnedReplicaExecutorPreflightReasonAckEligibilityUnknown
	}
	if returned.AckEligible {
		return ReturnedReplicaExecutorPreflightReasonAckEligible
	}
	if !returned.RequiredFrontierKnown {
		return ReturnedReplicaExecutorPreflightReasonMissingFrontier
	}
	if !returned.DurableFrontierKnown {
		return ReturnedReplicaExecutorPreflightReasonDurableMissing
	}
	if returned.DurableFrontierLSN < returned.RequiredFrontierLSN {
		return ReturnedReplicaExecutorPreflightReasonFrontierBehind
	}
	return ReturnedReplicaExecutorPreflightReasonActionNotAllowed
}

func returnedReplicaReintegrateAction(actions []ManagedVolumeAction) *ManagedVolumeAction {
	for i := range actions {
		if actions[i].Type == ManagedVolumeActionReintegrateReturned {
			return &actions[i]
		}
	}
	return nil
}

func returnedReplicaForExecutorPreflight(returned []ReturnedReplicaProjection, target string) (ReturnedReplicaProjection, string) {
	if len(returned) == 0 {
		return ReturnedReplicaProjection{}, ReturnedReplicaExecutorPreflightReasonNoReturnedReplica
	}
	var matches []ReturnedReplicaProjection
	for _, candidate := range returned {
		if target == "" || candidate.ReplicaID == target {
			matches = append(matches, candidate)
		}
	}
	if len(matches) == 0 {
		return ReturnedReplicaProjection{}, ReturnedReplicaExecutorPreflightReasonNoReturnedReplica
	}
	if len(matches) > 1 {
		return ReturnedReplicaProjection{}, ReturnedReplicaExecutorPreflightReasonAmbiguousReplica
	}
	return matches[0], ""
}
