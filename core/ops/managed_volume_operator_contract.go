package ops

type ManagedVolumeOperatorContract struct {
	APIVersion     string                        `json:"api_version"`
	Kind           string                        `json:"kind"`
	Status         ManagedVolumeOperatorStatus   `json:"status"`
	Events         []ManagedVolumeOperatorEvent  `json:"events,omitempty"`
	AllowedActions []ManagedVolumeOperatorAction `json:"allowed_actions,omitempty"`
}

type ManagedVolumeOperatorStatus struct {
	VolumeID                 string                             `json:"volume_id,omitempty"`
	PVCName                  string                             `json:"pvc_name,omitempty"`
	PrimaryReplicaID         string                             `json:"primary_replica_id,omitempty"`
	PublishTarget            string                             `json:"publish_target,omitempty"`
	AuthorityEpoch           uint64                             `json:"authority_epoch,omitempty"`
	AuthorityEndpointVersion uint64                             `json:"authority_endpoint_version,omitempty"`
	Status                   string                             `json:"status"`
	ReasonCode               string                             `json:"reason_code,omitempty"`
	Conditions               []ObservationCondition             `json:"conditions,omitempty"`
	DeleteSafety             *SwBlockVolumeDeleteSafetyDecision `json:"delete_safety,omitempty"`
	NVMe                     *ManagedVolumeNVMeStatus           `json:"nvme,omitempty"`
	ReplicaReintegrations    []ReturnedReplicaProjection        `json:"replica_reintegrations,omitempty"`
	ExecutorPreflights       []ReturnedReplicaExecutorPreflight `json:"executor_preflights,omitempty"`
	ExecutorContracts        []ReturnedReplicaExecutorContract  `json:"executor_contracts,omitempty"`
	NonClaims                []string                           `json:"non_claims,omitempty"`
	EvidenceRefs             []string                           `json:"evidence_refs,omitempty"`
}

type ManagedVolumeOperatorEvent struct {
	Type         string   `json:"type"`
	Reason       string   `json:"reason"`
	Message      string   `json:"message"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type ManagedVolumeOperatorAction struct {
	Type             string   `json:"type"`
	Mode             string   `json:"mode"`
	SideEffectClass  string   `json:"side_effect_class"`
	OwnerExecutor    string   `json:"owner_executor,omitempty"`
	Decision         string   `json:"decision,omitempty"`
	DecisionReason   string   `json:"decision_reason,omitempty"`
	MissingFacts     []string `json:"missing_facts,omitempty"`
	MutationAllowed  bool     `json:"mutation_allowed"`
	Preconditions    []string `json:"preconditions,omitempty"`
	InvariantRefs    []string `json:"invariant_refs,omitempty"`
	EvidenceRequired string   `json:"evidence_required,omitempty"`
	EvidenceRefs     []string `json:"evidence_refs,omitempty"`
}

func ManagedVolumeOperatorContractFromProjection(projection ManagedVolumeProjection) ManagedVolumeOperatorContract {
	contract := ManagedVolumeOperatorContract{
		APIVersion: "block.seaweedfs.com/v1alpha1",
		Kind:       "ManagedVolumeStatusContract",
		Status: ManagedVolumeOperatorStatus{
			VolumeID:                 projection.VolumeID,
			PVCName:                  projection.PVCName,
			PrimaryReplicaID:         projection.PrimaryReplicaID,
			PublishTarget:            projection.PublishTarget,
			AuthorityEpoch:           projection.AuthorityEpoch,
			AuthorityEndpointVersion: projection.AuthorityEndpointVersion,
			Status:                   projection.Status,
			ReasonCode:               projection.ReasonCode,
			Conditions:               append([]ObservationCondition(nil), projection.Conditions...),
			DeleteSafety:             cloneSwBlockVolumeDeleteSafetyDecision(projection.DeleteSafety),
			NVMe:                     cloneManagedVolumeNVMeStatus(projection.NVMe),
			ReplicaReintegrations:    cloneReturnedReplicaProjections(projection.ReplicaReintegrations),
			ExecutorPreflights:       cloneReturnedReplicaExecutorPreflights(ReturnedReplicaExecutorPreflights(projection)),
			ExecutorContracts:        cloneReturnedReplicaExecutorContracts(ReturnedReplicaExecutorContracts(projection)),
			NonClaims:                append([]string(nil), projection.NonClaims...),
			EvidenceRefs:             append([]string(nil), projection.EvidenceRefs...),
		},
	}
	for _, condition := range projection.Conditions {
		contract.Events = append(contract.Events, managedVolumeOperatorEventFromCondition(condition))
	}
	for _, action := range projection.Actions {
		contract.AllowedActions = append(contract.AllowedActions, ManagedVolumeOperatorAction{
			Type:             action.Type,
			Mode:             action.Mode,
			SideEffectClass:  action.SideEffectClass,
			OwnerExecutor:    action.OwnerExecutor,
			Decision:         action.Decision,
			DecisionReason:   action.DecisionReason,
			MissingFacts:     append([]string(nil), action.MissingFacts...),
			MutationAllowed:  false,
			Preconditions:    append([]string(nil), action.Preconditions...),
			InvariantRefs:    append([]string(nil), action.InvariantRefs...),
			EvidenceRequired: action.EvidenceRequired,
			EvidenceRefs:     append([]string(nil), action.EvidenceRefs...),
		})
	}
	if projection.DeleteSafety != nil && !hasManagedVolumeOperatorAction(contract.AllowedActions, projection.DeleteSafety.ActionType) {
		contract.AllowedActions = append(contract.AllowedActions, managedVolumeOperatorActionFromDeleteSafety(*projection.DeleteSafety))
	}
	return contract
}

func cloneManagedVolumeNVMeStatus(in *ManagedVolumeNVMeStatus) *ManagedVolumeNVMeStatus {
	if in == nil {
		return nil
	}
	out := *in
	out.NVMeAddrs = append([]string(nil), in.NVMeAddrs...)
	return &out
}

func cloneReturnedReplicaExecutorPreflights(in []ReturnedReplicaExecutorPreflight) []ReturnedReplicaExecutorPreflight {
	if len(in) == 0 {
		return nil
	}
	out := append([]ReturnedReplicaExecutorPreflight(nil), in...)
	for i := range out {
		out[i].EvidenceRefs = append([]string(nil), in[i].EvidenceRefs...)
		out[i].ForbiddenMutationClass = append([]string(nil), in[i].ForbiddenMutationClass...)
	}
	return out
}

func cloneReturnedReplicaExecutorContracts(in []ReturnedReplicaExecutorContract) []ReturnedReplicaExecutorContract {
	if len(in) == 0 {
		return nil
	}
	out := append([]ReturnedReplicaExecutorContract(nil), in...)
	for i := range out {
		out[i].AllowedMutationClass = append([]string(nil), in[i].AllowedMutationClass...)
		out[i].ForbiddenMutationClass = append([]string(nil), in[i].ForbiddenMutationClass...)
		out[i].TerminalEvidenceRequired = append([]string(nil), in[i].TerminalEvidenceRequired...)
		out[i].EvidenceRefs = append([]string(nil), in[i].EvidenceRefs...)
	}
	return out
}

func cloneReturnedReplicaProjections(in []ReturnedReplicaProjection) []ReturnedReplicaProjection {
	if len(in) == 0 {
		return nil
	}
	out := append([]ReturnedReplicaProjection(nil), in...)
	for i := range out {
		out[i].EvidenceRefs = append([]string(nil), in[i].EvidenceRefs...)
	}
	return out
}

func managedVolumeOperatorActionFromDeleteSafety(decision SwBlockVolumeDeleteSafetyDecision) ManagedVolumeOperatorAction {
	return ManagedVolumeOperatorAction{
		Type:             decision.ActionType,
		Mode:             ManagedVolumeActionModeDryRun,
		SideEffectClass:  ManagedVolumeSideEffectSafeK8S,
		OwnerExecutor:    "lifecycle_owner",
		Decision:         decision.Decision,
		DecisionReason:   decision.Reason,
		MissingFacts:     append([]string(nil), decision.MissingFacts...),
		MutationAllowed:  false,
		Preconditions:    []string{"delete_safety_evidence_current", "cleanup_residue_absent"},
		InvariantRefs:    []string{"INV-LIFECYCLE-FINALIZER-001"},
		EvidenceRequired: "cleanup-summary.txt",
		EvidenceRefs:     append([]string(nil), decision.EvidenceRefs...),
	}
}

func hasManagedVolumeOperatorAction(actions []ManagedVolumeOperatorAction, actionType string) bool {
	for _, action := range actions {
		if action.Type == actionType {
			return true
		}
	}
	return false
}

func cloneSwBlockVolumeDeleteSafetyDecision(in *SwBlockVolumeDeleteSafetyDecision) *SwBlockVolumeDeleteSafetyDecision {
	if in == nil {
		return nil
	}
	out := *in
	out.MissingFacts = append([]string(nil), in.MissingFacts...)
	out.EvidenceRefs = append([]string(nil), in.EvidenceRefs...)
	return &out
}

func managedVolumeOperatorEventFromCondition(condition ObservationCondition) ManagedVolumeOperatorEvent {
	eventType := "Normal"
	if condition.Severity == "warning" {
		eventType = "Warning"
	}
	if condition.Severity == "error" {
		eventType = "Warning"
	}
	return ManagedVolumeOperatorEvent{
		Type:         eventType,
		Reason:       condition.Reason,
		Message:      condition.Message,
		EvidenceRefs: append([]string(nil), condition.EvidenceRefs...),
	}
}
