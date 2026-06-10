package ops

import (
	"fmt"
	"strings"
)

const (
	ManagedVolumeActionDecisionAllowed  = "allowed"
	ManagedVolumeActionDecisionRejected = "rejected"

	ManagedVolumeActionRejectUnknownAction = "unknown_action"
	ManagedVolumeActionRejectDisabled      = "policy_disabled"
	ManagedVolumeActionRejectMissingFacts  = "missing_required_facts"
	ManagedVolumeActionRejectMutation      = "mutation_not_allowed"
)

// ManagedVolumeActionEvaluation is the executable contract for action hints.
// It does not perform the action. It decides whether the latest facts permit a
// read-only/dry-run action to be surfaced as executable by a future bounded
// executor, and it explains why not.
type ManagedVolumeActionEvaluation struct {
	ActionType       string   `json:"action_type"`
	Decision         string   `json:"decision"`
	Mode             string   `json:"mode,omitempty"`
	SideEffectClass  string   `json:"side_effect_class,omitempty"`
	OwnerExecutor    string   `json:"owner_executor,omitempty"`
	MutationAllowed  bool     `json:"mutation_allowed"`
	Reason           string   `json:"reason,omitempty"`
	MissingFacts     []string `json:"missing_facts,omitempty"`
	InvariantRefs    []string `json:"invariant_refs,omitempty"`
	EvidenceRequired string   `json:"evidence_required,omitempty"`
}

// EvaluateManagedVolumeAction evaluates one action against the latest
// ManagedVolume facts. The result is intentionally fail-closed: unknown actions,
// disabled policy gates, mutating actions, and missing required facts are
// rejected before any executor can be considered.
func EvaluateManagedVolumeAction(actionType string, facts ManagedVolumeFacts) ManagedVolumeActionEvaluation {
	contract, ok := managedVolumeActionContractEntry(actionType)
	if !ok {
		return ManagedVolumeActionEvaluation{
			ActionType: actionType,
			Decision:   ManagedVolumeActionDecisionRejected,
			Reason:     ManagedVolumeActionRejectUnknownAction,
		}
	}
	evaluation := ManagedVolumeActionEvaluation{
		ActionType:       actionType,
		Mode:             contract.Mode,
		SideEffectClass:  contract.SideEffectClass,
		OwnerExecutor:    contract.OwnerExecutor,
		MutationAllowed:  contract.MutationAllowed,
		InvariantRefs:    append([]string(nil), contract.InvariantRefs...),
		EvidenceRequired: contract.EvidenceRequired,
	}
	if contract.PolicyGate == ActionPolicyDisabled {
		evaluation.Decision = ManagedVolumeActionDecisionRejected
		evaluation.Reason = ManagedVolumeActionRejectDisabled
		return evaluation
	}
	if contract.MutationAllowed {
		evaluation.Decision = ManagedVolumeActionDecisionRejected
		evaluation.Reason = ManagedVolumeActionRejectMutation
		return evaluation
	}
	missing := missingManagedVolumeActionFacts(contract.RequiredFacts, facts)
	if len(missing) > 0 {
		evaluation.Decision = ManagedVolumeActionDecisionRejected
		evaluation.Reason = ManagedVolumeActionRejectMissingFacts
		evaluation.MissingFacts = missing
		return evaluation
	}
	evaluation.Decision = ManagedVolumeActionDecisionAllowed
	return evaluation
}

func managedVolumeActionContractEntry(actionType string) (ManagedVolumeActionContractEntry, bool) {
	for _, entry := range ManagedVolumeActionContract() {
		if entry.Type == actionType {
			return entry, true
		}
	}
	return ManagedVolumeActionContractEntry{}, false
}

func missingManagedVolumeActionFacts(required []string, facts ManagedVolumeFacts) []string {
	var missing []string
	for _, requiredFact := range required {
		if !managedVolumeActionFactPresent(requiredFact, facts) {
			missing = append(missing, requiredFact)
		}
	}
	return missing
}

func managedVolumeActionFactPresent(requiredFact string, facts ManagedVolumeFacts) bool {
	switch requiredFact {
	case "evidence.reason_code":
		return facts.ProductReason != "" || facts.EvidenceStaleReason != "" || len(facts.EvidenceRefs) > 0
	case "identity.pvc_name":
		return facts.PVCName != ""
	case "kubernetes.pvc_phase":
		return facts.PVC != nil && facts.PVC.Phase != ""
	case "csi.staged_target":
		for _, stage := range facts.CSIStages {
			if stage.Target != "" {
				return true
			}
		}
		return false
	case "host_path.rtpg_aas":
		for _, path := range facts.HostPaths {
			if path.ALUAState != "" || path.ANAState != "" {
				return true
			}
		}
		return false
	case "host_path.stale_path_probe":
		for _, path := range facts.HostPaths {
			if path.StaleFenced {
				return true
			}
		}
		return false
	case "authority.publish_target":
		return facts.Authority != nil && facts.Authority.PublishTarget != ""
	case "placement.replica_node":
		for _, replica := range facts.Replicas {
			if replica.KubernetesNode != "" || replica.ServerID != "" || replica.PhysicalHost != "" {
				return true
			}
		}
		return false
	case "authority.primary_replica":
		return facts.Authority != nil && facts.Authority.PrimaryReplica != ""
	case "replica.durable_frontier_lsn":
		for _, replica := range facts.Replicas {
			if replica.DurableFrontierKnown {
				return true
			}
		}
		return false
	default:
		// Unknown fact names are treated as missing so new contract entries
		// must be deliberately wired into this executable evaluator.
		return false
	}
}

func (e ManagedVolumeActionEvaluation) String() string {
	if e.Decision == ManagedVolumeActionDecisionAllowed {
		return fmt.Sprintf("%s %s mode=%s executor=%s", e.ActionType, e.Decision, e.Mode, e.OwnerExecutor)
	}
	if len(e.MissingFacts) == 0 {
		return fmt.Sprintf("%s %s reason=%s", e.ActionType, e.Decision, e.Reason)
	}
	return fmt.Sprintf("%s %s reason=%s missing=%s", e.ActionType, e.Decision, e.Reason, strings.Join(e.MissingFacts, ","))
}
