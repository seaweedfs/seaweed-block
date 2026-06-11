package ops

type ManagedVolumeOperatorContract struct {
	APIVersion     string                        `json:"api_version"`
	Kind           string                        `json:"kind"`
	Status         ManagedVolumeOperatorStatus   `json:"status"`
	Events         []ManagedVolumeOperatorEvent  `json:"events,omitempty"`
	AllowedActions []ManagedVolumeOperatorAction `json:"allowed_actions,omitempty"`
}

type ManagedVolumeOperatorStatus struct {
	VolumeID     string                             `json:"volume_id,omitempty"`
	PVCName      string                             `json:"pvc_name,omitempty"`
	Status       string                             `json:"status"`
	ReasonCode   string                             `json:"reason_code,omitempty"`
	Conditions   []ObservationCondition             `json:"conditions,omitempty"`
	DeleteSafety *SwBlockVolumeDeleteSafetyDecision `json:"delete_safety,omitempty"`
	NonClaims    []string                           `json:"non_claims,omitempty"`
	EvidenceRefs []string                           `json:"evidence_refs,omitempty"`
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
			VolumeID:     projection.VolumeID,
			PVCName:      projection.PVCName,
			Status:       projection.Status,
			ReasonCode:   projection.ReasonCode,
			Conditions:   append([]ObservationCondition(nil), projection.Conditions...),
			DeleteSafety: cloneSwBlockVolumeDeleteSafetyDecision(projection.DeleteSafety),
			NonClaims:    append([]string(nil), projection.NonClaims...),
			EvidenceRefs: append([]string(nil), projection.EvidenceRefs...),
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
	return contract
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
