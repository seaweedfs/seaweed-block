package ops

const (
	SwBlockVolumeFinalizerName = "block.seaweedfs.com/swblockvolume-protection"

	SwBlockVolumeDeleteActionReleaseFinalizer = "safe_k8s.release_swblockvolume_finalizer"

	DeleteSafetyStateNotRequested = "not_requested"
	DeleteSafetyStateRequested    = "requested"
	DeleteSafetyStateBlocked      = "blocked"
	DeleteSafetyStateReleasable   = "releasable"
	DeleteSafetyStateReleased     = "released"

	ReasonDeleteNotRequested        = "delete_not_requested"
	ReasonCleanupEvidenceMissing    = "cleanup_evidence_missing"
	ReasonDeleteFinalizerReleasable = "finalizer_releasable"
	ReasonDeleteFinalizerAdded      = "finalizer_added"
	ReasonDeleteFinalizerReleased   = "finalizer_released"
)

type SwBlockVolumeDeleteSafetyContract struct {
	FinalizerName         string   `json:"finalizer_name"`
	OwnedKind             string   `json:"owned_kind"`
	OwnedMutationScope    []string `json:"owned_mutation_scope"`
	RequiredFacts         []string `json:"required_facts"`
	DeleteStates          []string `json:"delete_states"`
	BlockingReasons       []string `json:"blocking_reasons"`
	ReleaseReason         string   `json:"release_reason"`
	NonClaims             []string `json:"non_claims"`
	ReleaseActionType     string   `json:"release_action_type"`
	ReleaseEvidence       string   `json:"release_evidence"`
	BlockedSafeNextAction string   `json:"blocked_safe_next_action"`
}

type SwBlockVolumeDeleteSafetyFacts struct {
	DeleteRequested  bool             `json:"delete_requested"`
	FinalizerPresent bool             `json:"finalizer_present"`
	Cleanup          *CleanupEvidence `json:"cleanup,omitempty"`
}

type SwBlockVolumeDeleteSafetyDecision struct {
	ActionType              string   `json:"action_type"`
	Decision                string   `json:"decision"`
	State                   string   `json:"state"`
	Reason                  string   `json:"reason"`
	FinalizerReleaseAllowed bool     `json:"finalizer_release_allowed"`
	MissingFacts            []string `json:"missing_facts,omitempty"`
	EvidenceRefs            []string `json:"evidence_refs,omitempty"`
	SafeNextAction          string   `json:"safe_next_action,omitempty"`
}

func SwBlockVolumeDeleteSafetyContractDefinition() SwBlockVolumeDeleteSafetyContract {
	return SwBlockVolumeDeleteSafetyContract{
		FinalizerName: SwBlockVolumeFinalizerName,
		OwnedKind:     SwBlockVolumeKind,
		OwnedMutationScope: []string{
			"swblockvolumes.metadata.finalizers",
			"swblockvolumes/status",
			"events",
		},
		RequiredFacts: []string{
			"identity.volume_id",
			"identity.pvc_name",
			"identity.pv_name",
			"kubernetes.swblockvolume.deletion_timestamp",
			"cleanup.status",
			"cleanup.k8s_residue_count",
			"cleanup.iscsi_residue_count",
			"cleanup.multipath_residue_count",
			"cleanup.process_residue_count",
			"cleanup.hostpath_residue_count",
		},
		DeleteStates: []string{
			DeleteSafetyStateNotRequested,
			DeleteSafetyStateRequested,
			DeleteSafetyStateBlocked,
			DeleteSafetyStateReleasable,
			DeleteSafetyStateReleased,
		},
		BlockingReasons: []string{
			ReasonCleanupEvidenceMissing,
			ReasonCleanupRequired,
			"iscsi_node_records_present",
			"iscsi_sessions_present",
			"multipath_maps_present",
			"dmsetup_devices_present",
			"kubernetes_resources_present",
			"hostpath_residue_present",
		},
		ReleaseReason:         ReasonDeleteFinalizerReleasable,
		ReleaseActionType:     SwBlockVolumeDeleteActionReleaseFinalizer,
		ReleaseEvidence:       "cleanup-summary.txt with cleanup_status=ok and all residue counts 0",
		BlockedSafeNextAction: ManagedVolumeActionVerifyCleanup,
		NonClaims: []string{
			"no_pvc_finalizer_ownership",
			"no_automatic_cleanup_execution",
			"no_pv_or_pvc_delete",
			"no_pod_or_deployment_delete",
			"no_iscsi_or_multipath_mutation",
			"no_hostpath_delete",
		},
	}
}

func EvaluateSwBlockVolumeDeleteSafety(facts SwBlockVolumeDeleteSafetyFacts) SwBlockVolumeDeleteSafetyDecision {
	if !facts.DeleteRequested {
		return SwBlockVolumeDeleteSafetyDecision{
			ActionType: SwBlockVolumeDeleteActionReleaseFinalizer,
			Decision:   ManagedVolumeActionDecisionRejected,
			State:      DeleteSafetyStateNotRequested,
			Reason:     ReasonDeleteNotRequested,
		}
	}
	if facts.Cleanup == nil {
		return SwBlockVolumeDeleteSafetyDecision{
			ActionType:     SwBlockVolumeDeleteActionReleaseFinalizer,
			Decision:       ManagedVolumeActionDecisionRejected,
			State:          DeleteSafetyStateBlocked,
			Reason:         ReasonCleanupEvidenceMissing,
			MissingFacts:   []string{"cleanup.status"},
			SafeNextAction: ManagedVolumeActionVerifyCleanup,
		}
	}
	if cleanupRequired(facts.Cleanup) {
		return SwBlockVolumeDeleteSafetyDecision{
			ActionType:     SwBlockVolumeDeleteActionReleaseFinalizer,
			Decision:       ManagedVolumeActionDecisionRejected,
			State:          DeleteSafetyStateBlocked,
			Reason:         cleanupReason(facts.Cleanup),
			EvidenceRefs:   cleanupEvidenceRefs(facts.Cleanup),
			SafeNextAction: ManagedVolumeActionVerifyCleanup,
		}
	}
	return SwBlockVolumeDeleteSafetyDecision{
		ActionType:              SwBlockVolumeDeleteActionReleaseFinalizer,
		Decision:                ManagedVolumeActionDecisionAllowed,
		State:                   DeleteSafetyStateReleasable,
		Reason:                  ReasonDeleteFinalizerReleasable,
		FinalizerReleaseAllowed: true,
		EvidenceRefs:            cleanupEvidenceRefs(facts.Cleanup),
	}
}
