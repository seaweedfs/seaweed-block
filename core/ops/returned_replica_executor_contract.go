package ops

const (
	ReturnedReplicaExecutorContractBlocked  = "blocked"
	ReturnedReplicaExecutorContractDisabled = "disabled"

	ReturnedReplicaExecutorContractReasonPreflightNotReady = "preflight_not_ready"
	ReturnedReplicaExecutorContractReasonExecutorDisabled  = "executor_policy_disabled"
)

// ReturnedReplicaExecutorContract is the still-non-mutating execution boundary
// for returned-replica reintegration. It names the future mutation envelope and
// terminal evidence while keeping execution disabled in the current product.
type ReturnedReplicaExecutorContract struct {
	ActionType               string   `json:"action_type"`
	VolumeID                 string   `json:"volume_id,omitempty"`
	ReplicaID                string   `json:"replica_id,omitempty"`
	Decision                 string   `json:"decision"`
	Reason                   string   `json:"reason"`
	OwnerExecutor            string   `json:"owner_executor"`
	ExecutionEnabled         bool     `json:"execution_enabled"`
	MutationAllowed          bool     `json:"mutation_allowed"`
	PreflightDecision        string   `json:"preflight_decision,omitempty"`
	PreflightReason          string   `json:"preflight_reason,omitempty"`
	AllowedMutationClass     []string `json:"allowed_mutation_class,omitempty"`
	ForbiddenMutationClass   []string `json:"forbidden_mutation_class,omitempty"`
	TerminalEvidenceRequired []string `json:"terminal_evidence_required,omitempty"`
	EvidenceRefs             []string `json:"evidence_refs,omitempty"`
}

func ReturnedReplicaExecutorContracts(projection ManagedVolumeProjection) []ReturnedReplicaExecutorContract {
	preflights := ReturnedReplicaExecutorPreflights(projection)
	if len(preflights) == 0 {
		return nil
	}
	out := make([]ReturnedReplicaExecutorContract, 0, len(preflights))
	for _, preflight := range preflights {
		contract := ReturnedReplicaExecutorContract{
			ActionType:               preflight.ActionType,
			VolumeID:                 preflight.VolumeID,
			ReplicaID:                preflight.ReplicaID,
			Decision:                 ReturnedReplicaExecutorContractBlocked,
			Reason:                   ReturnedReplicaExecutorContractReasonPreflightNotReady,
			OwnerExecutor:            preflight.OwnerExecutor,
			ExecutionEnabled:         false,
			MutationAllowed:          false,
			PreflightDecision:        preflight.Decision,
			PreflightReason:          preflight.Reason,
			ForbiddenMutationClass:   append([]string(nil), preflight.ForbiddenMutationClass...),
			TerminalEvidenceRequired: returnedReplicaTerminalEvidenceRequired(preflight.ActionType),
			EvidenceRefs:             append([]string(nil), preflight.EvidenceRefs...),
		}
		if preflight.Decision == ReturnedReplicaExecutorPreflightReady {
			contract.Decision = ReturnedReplicaExecutorContractDisabled
			contract.Reason = ReturnedReplicaExecutorContractReasonExecutorDisabled
			switch preflight.ActionType {
			case ManagedVolumeActionRebuildReturned:
				contract.AllowedMutationClass = []string{"rebuild_traffic"}
				contract.ForbiddenMutationClass = []string{"ack_eligibility", "frontend_publication", "failback"}
			case ManagedVolumeActionFailbackReturned:
				contract.AllowedMutationClass = []string{"failback"}
				contract.ForbiddenMutationClass = []string{"ack_eligibility", "frontend_publication", "rebuild_traffic"}
			default:
				contract.AllowedMutationClass = []string{"ack_eligibility"}
				contract.ForbiddenMutationClass = []string{"frontend_publication", "rebuild_traffic", "failback"}
			}
		}
		out = append(out, contract)
	}
	return out
}

func returnedReplicaTerminalEvidenceRequired(actionType string) []string {
	if actionType == ManagedVolumeActionRebuildReturned {
		return []string{
			"frontend_fenced_before_rebuild",
			"primary_unchanged",
			"durable_frontier_caught_up",
			"no_frontend_publication",
			"no_cross_volume_identity_change",
		}
	}
	if actionType == ManagedVolumeActionFailbackReturned {
		return []string{
			"ack_eligible_true",
			"frontend_fenced_before_failback",
			"failback_authority_owner",
			"authority_epoch_advanced",
			"single_primary_after_failback",
			"publish_target_swapped_after_failback",
			"no_cross_volume_identity_change",
		}
	}
	return []string{
		"ack_eligibility_known",
		"ack_eligible_true",
		"frontend_fenced_after_execution",
		"primary_unchanged",
		"durable_frontier_covered",
		"no_cross_volume_identity_change",
	}
}
