package ops

const (
	ManagedVolumeFieldStable      = "stable"
	ManagedVolumeFieldProvisional = "provisional"
	ManagedVolumeFieldTestOnly    = "test_only"

	FactAggregationPassive = "passive"
	FactAggregationProbe   = "bounded_probe"
	FactAggregationDual    = "passive_plus_bounded_probe"

	FactAuthorityKubernetesObject  = "kubernetes_object_authority"
	FactAuthorityPlacement         = "placement_authority"
	FactAuthorityAuthorityLine     = "authority_line_authority"
	FactAuthorityReplicaDurability = "replica_durability_authority"
	FactAuthorityCSIAttach         = "csi_attach_authority"
	FactAuthorityHostPath          = "host_path_authority"
	FactAuthorityWorkloadEvidence  = "workload_evidence_authority"
	FactAuthorityCleanup           = "cleanup_authority"
	FactAuthorityObservation       = "observation_authority"

	MasterEngine        = "engine_master"
	MasterManagedVolume = "managed_volume_master"

	ActionPolicyReadOnly = "read_only"
	ActionPolicyDryRun   = "dry_run"
	ActionPolicyDisabled = "disabled_until_operator_policy"
)

type ManagedVolumeFactContractEntry struct {
	Path             string `json:"path"`
	Stability        string `json:"stability"`
	Participant      string `json:"participant"`
	FactAuthority    string `json:"fact_authority"`
	Master           string `json:"master"`
	AggregationMode  string `json:"aggregation_mode"`
	ProbeAllowed     bool   `json:"probe_allowed"`
	ProbeTrigger     string `json:"probe_trigger,omitempty"`
	ConditionSurface string `json:"condition_surface,omitempty"`
	EvidenceRequired string `json:"evidence_required"`
}

type ManagedVolumeActionContractEntry struct {
	Type             string   `json:"type"`
	Master           string   `json:"master"`
	Mode             string   `json:"mode"`
	SideEffectClass  string   `json:"side_effect_class"`
	OwnerExecutor    string   `json:"owner_executor"`
	PolicyGate       string   `json:"policy_gate"`
	RequiredFacts    []string `json:"required_facts,omitempty"`
	InvariantRefs    []string `json:"invariant_refs,omitempty"`
	EvidenceRequired string   `json:"evidence_required"`
	MutationAllowed  bool     `json:"mutation_allowed"`
}

func ManagedVolumeFactContract() []ManagedVolumeFactContractEntry {
	return []ManagedVolumeFactContractEntry{
		{
			Path:             "identity.namespace",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "metadata",
			EvidenceRequired: "pvc_or_pv_object",
		},
		{
			Path:             "identity.pvc_name",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "metadata",
			EvidenceRequired: "pvc_object",
		},
		{
			Path:             "identity.volume_id",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_watcher_or_inventory",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "metadata",
			EvidenceRequired: "pv_handle_or_inventory_volume_id",
		},
		{
			Path:             "desired.replication_factor",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "storageclass_or_helm_values_reader",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "metadata/Ready/Blocked",
			EvidenceRequired: "storageclass_parameter_or_helm_values",
		},
		{
			Path:             "desired.ack_profile",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "storageclass_or_helm_values_reader",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "metadata/Ready/Blocked",
			EvidenceRequired: "storageclass_parameter_or_helm_values",
		},
		{
			Path:             "desired.claim_profile",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "storageclass_or_scenario_contract",
			FactAuthority:    FactAuthorityObservation,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "metadata/non_claims",
			EvidenceRequired: "claim_profile_artifact_or_release_contract",
		},
		{
			Path:             "desired.protocol",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "storageclass_or_helm_values_reader",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "metadata/Ready/Blocked",
			EvidenceRequired: "storageclass_parameter_or_helm_values",
		},
		{
			Path:             "kubernetes.pvc_phase",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationDual,
			ProbeAllowed:     true,
			ProbeTrigger:     "pvc_pending_or_first_volume_timeout",
			ConditionSurface: "Ready/Blocked",
			EvidenceRequired: "pvc_status_or_kubectl_describe",
		},
		{
			Path:             "placement.replica_node",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "launcher_or_operator",
			FactAuthority:    FactAuthorityPlacement,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "Ready/Degraded/Blocked",
			EvidenceRequired: "launcher_plan_or_generated_deployment",
		},
		{
			Path:             "authority.primary_replica",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "blockmaster_authority",
			FactAuthority:    FactAuthorityAuthorityLine,
			Master:           MasterEngine,
			AggregationMode:  FactAggregationDual,
			ProbeAllowed:     true,
			ProbeTrigger:     "primary_missing_or_promotion_decision",
			ConditionSurface: "Ready/Recovering/Blocked",
			EvidenceRequired: "authority_event_or_inventory_primary",
		},
		{
			Path:             "authority.epoch",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "blockmaster_authority",
			FactAuthority:    FactAuthorityAuthorityLine,
			Master:           MasterEngine,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "Ready/Recovered",
			EvidenceRequired: "authority_event",
		},
		{
			Path:             "authority.endpoint_version",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "blockmaster_authority",
			FactAuthority:    FactAuthorityAuthorityLine,
			Master:           MasterEngine,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "Ready/Recovered",
			EvidenceRequired: "authority_event",
		},
		{
			Path:             "authority.publish_target",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "blockmaster_authority",
			FactAuthority:    FactAuthorityAuthorityLine,
			Master:           MasterEngine,
			AggregationMode:  FactAggregationDual,
			ProbeAllowed:     true,
			ProbeTrigger:     "publish_target_missing_or_reattach_decision",
			ConditionSurface: "Ready/Recovering/Blocked",
			EvidenceRequired: "authority_event_or_inventory_publish_target",
		},
		{
			Path:             "replica.durable_frontier_lsn",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "blockvolume_status_or_replica_probe",
			FactAuthority:    FactAuthorityReplicaDurability,
			Master:           MasterEngine,
			AggregationMode:  FactAggregationDual,
			ProbeAllowed:     true,
			ProbeTrigger:     "promotion_decision_or_required_frontier_unknown",
			ConditionSurface: "Recovering/Blocked",
			EvidenceRequired: "status_endpoint_or_promotion_evidence",
		},
		{
			Path:             "replica.frontend_primary_ready",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "blockvolume_status_or_replica_probe",
			FactAuthority:    FactAuthorityReplicaDurability,
			Master:           MasterEngine,
			AggregationMode:  FactAggregationDual,
			ProbeAllowed:     true,
			ProbeTrigger:     "returned_replica_observed",
			ConditionSurface: "Recovering/Blocked",
			EvidenceRequired: "status_projection_or_returned_replica_bundle",
		},
		{
			Path:             "csi.staged_target",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "csi_node",
			FactAuthority:    FactAuthorityCSIAttach,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationDual,
			ProbeAllowed:     true,
			ProbeTrigger:     "reattach_timeout_or_target_mismatch",
			ConditionSurface: "Ready/Recovering/Blocked",
			EvidenceRequired: "csi_event_or_node_stage_log",
		},
		{
			Path:             "host_path.rtpg_aas",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "host_path_probe",
			FactAuthority:    FactAuthorityHostPath,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationDual,
			ProbeAllowed:     true,
			ProbeTrigger:     "transparent_failover_claim_or_path_suspect",
			ConditionSurface: "Ready/Recovered/Blocked",
			EvidenceRequired: "sg_rtpg_artifact",
		},
		{
			Path:             "host_path.stale_path_probe",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "host_path_probe",
			FactAuthority:    FactAuthorityHostPath,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "stale_primary_fencing_claim",
			ConditionSurface: "Recovered/Blocked",
			EvidenceRequired: "direct_stale_path_io_probe",
		},
		{
			Path:             "workload.reader_verified",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "workload_probe_or_test_app",
			FactAuthority:    FactAuthorityWorkloadEvidence,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "recovery_or_first_volume_claim",
			ConditionSurface: "Ready/Recovered",
			EvidenceRequired: "reader_log_checksum",
		},
		{
			Path:             "cleanup.status",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "cleanup_verifier",
			FactAuthority:    FactAuthorityCleanup,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "cleanup_close_gate",
			ConditionSurface: "CleanupRequired/Ready",
			EvidenceRequired: "cleanup_summary",
		},
		{
			Path:             "cleanup.k8s_residue_count",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "cleanup_verifier",
			FactAuthority:    FactAuthorityCleanup,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "cleanup_close_gate",
			ConditionSurface: "CleanupRequired",
			EvidenceRequired: "cleanup_summary_and_kubectl_artifact",
		},
		{
			Path:             "cleanup.iscsi_residue_count",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "cleanup_verifier",
			FactAuthority:    FactAuthorityCleanup,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "cleanup_close_gate",
			ConditionSurface: "CleanupRequired",
			EvidenceRequired: "cleanup_summary_and_iscsiadm_artifact",
		},
		{
			Path:             "cleanup.multipath_residue_count",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "cleanup_verifier",
			FactAuthority:    FactAuthorityCleanup,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "cleanup_close_gate",
			ConditionSurface: "CleanupRequired",
			EvidenceRequired: "cleanup_summary_and_multipath_artifact",
		},
		{
			Path:             "cleanup.process_residue_count",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "cleanup_verifier",
			FactAuthority:    FactAuthorityCleanup,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "cleanup_close_gate",
			ConditionSurface: "CleanupRequired",
			EvidenceRequired: "cleanup_summary_and_process_artifact",
		},
		{
			Path:             "cleanup.hostpath_residue_count",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "cleanup_verifier",
			FactAuthority:    FactAuthorityCleanup,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "cleanup_close_gate",
			ConditionSurface: "CleanupRequired",
			EvidenceRequired: "cleanup_summary_and_hostpath_artifact",
		},
		{
			Path:             "cleanup.failure_count",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "cleanup_verifier",
			FactAuthority:    FactAuthorityCleanup,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "cleanup_close_gate",
			ConditionSurface: "CleanupRequired",
			EvidenceRequired: "cleanup_summary",
		},
		{
			Path:             "evidence.reason_code",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "managed_volume_projection",
			FactAuthority:    FactAuthorityObservation,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "all_conditions",
			EvidenceRequired: "projection_inputs",
		},
		{
			Path:             "workload.same_pod_uid",
			Stability:        ManagedVolumeFieldTestOnly,
			Participant:      "testops_or_workload_probe",
			FactAuthority:    FactAuthorityWorkloadEvidence,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationProbe,
			ProbeAllowed:     true,
			ProbeTrigger:     "transparent_failover_claim",
			ConditionSurface: "Recovered",
			EvidenceRequired: "pod_uid_before_after_artifact",
		},
	}
}

func LiveNodeEvidenceFactContract() []ManagedVolumeFactContractEntry {
	return []ManagedVolumeFactContractEntry{
		{
			Path:             "node.kubernetes_ready",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_node_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Ready",
			EvidenceRequired: "kubernetes_node_ready_condition",
		},
		{
			Path:             "node.scheduling_disabled",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_node_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "kubernetes_node_unschedulable_field",
		},
		{
			Path:             "node.csi_node_pod_ready",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_pod_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "csi_node_daemonset_pod_status",
		},
		{
			Path:             "node.csi_driver_exists",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_csi_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "csidriver_object",
		},
		{
			Path:             "node.csi_node_driver_registered",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_csi_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "csinode_driver_entry",
		},
		{
			Path:             "node.required_image_presence",
			Stability:        ManagedVolumeFieldProvisional,
			Participant:      "image_inventory_observer",
			FactAuthority:    FactAuthorityObservation,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "node_image_inventory_or_pod_image_pull_status",
		},
		{
			Path:             "node.image_pull_status",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "kubernetes_pod_watcher",
			FactAuthority:    FactAuthorityKubernetesObject,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "pod_container_waiting_reason",
		},
		{
			Path:             "node.iscsi_prereq",
			Stability:        ManagedVolumeFieldProvisional,
			Participant:      "host_prereq_observer",
			FactAuthority:    FactAuthorityHostPath,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "iscsiadm_or_preflight_artifact",
		},
		{
			Path:             "node.multipath_prereq",
			Stability:        ManagedVolumeFieldProvisional,
			Participant:      "host_prereq_observer",
			FactAuthority:    FactAuthorityHostPath,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "multipath_or_preflight_artifact",
		},
		{
			Path:             "node.loopback_publish_target_cross_node",
			Stability:        ManagedVolumeFieldStable,
			Participant:      "managed_volume_projector",
			FactAuthority:    FactAuthorityObservation,
			Master:           MasterManagedVolume,
			AggregationMode:  FactAggregationPassive,
			ConditionSurface: "SwBlockCluster.status.nodes[].Blocked",
			EvidenceRequired: "publish_target_and_consumer_node_evidence",
		},
	}
}

func LiveNodeEvidenceReasonCodes() []string {
	return []string{
		ReasonNodeReady,
		ReasonNodeNotReady,
		ReasonNodeSchedulingDisabled,
		ReasonCSINodePodNotReady,
		ReasonCSIDriverNotRegistered,
		ReasonImageMissingOnNode,
		ReasonISCSIPrereqMissing,
		ReasonMultipathPrereqMissing,
		ReasonPublishTargetLoopbackCrossNode,
	}
}

func ManagedVolumeActionContract() []ManagedVolumeActionContractEntry {
	return []ManagedVolumeActionContractEntry{
		{
			Type:             ManagedVolumeActionCollectBundle,
			Master:           MasterManagedVolume,
			Mode:             ManagedVolumeActionModeReadOnly,
			SideEffectClass:  ManagedVolumeSideEffectObserve,
			OwnerExecutor:    "ops",
			PolicyGate:       ActionPolicyReadOnly,
			RequiredFacts:    []string{"evidence.reason_code"},
			EvidenceRequired: "projection_inputs_or_bundle",
		},
		{
			Type:             ManagedVolumeActionWaitForPVCBound,
			Master:           MasterManagedVolume,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectObserve,
			OwnerExecutor:    "ops",
			PolicyGate:       ActionPolicyDryRun,
			RequiredFacts:    []string{"identity.pvc_name", "kubernetes.pvc_phase"},
			EvidenceRequired: "pvc_status_or_kubectl_describe",
		},
		{
			Type:             ManagedVolumeActionInspectMountFailure,
			Master:           MasterManagedVolume,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectObserve,
			OwnerExecutor:    "ops",
			PolicyGate:       ActionPolicyDryRun,
			RequiredFacts:    []string{"identity.pvc_name", "kubernetes.pvc_phase", "csi.staged_target"},
			EvidenceRequired: "pod_describe_or_csi_node_log",
		},
		{
			Type:             ManagedVolumeActionInspectHostPath,
			Master:           MasterManagedVolume,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectObserve,
			OwnerExecutor:    "ops",
			PolicyGate:       ActionPolicyDryRun,
			RequiredFacts:    []string{"host_path.rtpg_aas", "host_path.stale_path_probe"},
			InvariantRefs:    []string{"INV-HOSTPATH-FACTS-001", "INV-HOSTPATH-TRANSPARENT-001"},
			EvidenceRequired: "sg_rtpg_artifact_and_stale_path_probe",
		},
		{
			Type:             ManagedVolumeActionReinstallExternalISCSI,
			Master:           MasterManagedVolume,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectSafeK8S,
			OwnerExecutor:    "installer_or_operator",
			PolicyGate:       ActionPolicyDryRun,
			RequiredFacts:    []string{"authority.publish_target", "placement.replica_node"},
			InvariantRefs:    []string{"INV-K8S-NONLOOPBACK-001"},
			EvidenceRequired: "loopback_cross_node_evidence",
		},
		{
			Type:             ManagedVolumeActionInspectTargetTopology,
			Master:           MasterManagedVolume,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectObserve,
			OwnerExecutor:    "ops",
			PolicyGate:       ActionPolicyDryRun,
			RequiredFacts:    []string{"authority.publish_target", "placement.replica_node"},
			InvariantRefs:    []string{"INV-K8S-NONLOOPBACK-001"},
			EvidenceRequired: "loopback_cross_node_evidence",
		},
		{
			Type:             ManagedVolumeActionImportCSIImage,
			Master:           MasterManagedVolume,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectSafeK8S,
			OwnerExecutor:    "installer_or_operator",
			PolicyGate:       ActionPolicyDryRun,
			RequiredFacts:    []string{"kubernetes.pvc_phase", "csi.staged_target"},
			InvariantRefs:    []string{"INV-MANAGED-VOLUME-READMODEL-001"},
			EvidenceRequired: "csi_node_image_pull_evidence",
		},
		{
			Type:             ManagedVolumeActionRequestPromotion,
			Master:           MasterEngine,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectAuthorityMutating,
			OwnerExecutor:    "authority_recovery_executor",
			PolicyGate:       ActionPolicyDisabled,
			RequiredFacts:    []string{"authority.primary_replica", "replica.durable_frontier_lsn"},
			EvidenceRequired: "promotion_readiness_evidence",
			MutationAllowed:  false,
		},
		{
			Type:             ManagedVolumeActionReintegrateReturned,
			Master:           MasterEngine,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectAuthorityMutating,
			OwnerExecutor:    "authority_recovery_executor",
			PolicyGate:       ActionPolicyDryRun,
			RequiredFacts:    []string{"authority.primary_replica", "returned_replica.frontend_fenced", "returned_replica.required_frontier_covered"},
			InvariantRefs:    []string{"INV-RETURNED-REPLICA-FENCING-001", "INV-RETURNED-REPLICA-FRONTIER-001"},
			EvidenceRequired: "returned_replica_reintegration_evidence",
			MutationAllowed:  false,
		},
		{
			Type:             ManagedVolumeActionRebuildReturned,
			Master:           MasterEngine,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectAuthorityMutating,
			OwnerExecutor:    "authority_recovery_executor",
			PolicyGate:       ActionPolicyDisabled,
			RequiredFacts:    []string{"authority.primary_replica", "replica.durable_frontier_lsn", "replica.frontend_primary_ready"},
			InvariantRefs:    []string{"INV-RETURNED-REPLICA-FENCING-001", "INV-RETURNED-REPLICA-FRONTIER-001"},
			EvidenceRequired: "returned_replica_rebuild_evidence",
			MutationAllowed:  false,
		},
		{
			Type:             ManagedVolumeActionFailbackReturned,
			Master:           MasterEngine,
			Mode:             ManagedVolumeActionModeDryRun,
			SideEffectClass:  ManagedVolumeSideEffectAuthorityMutating,
			OwnerExecutor:    "authority_recovery_executor",
			PolicyGate:       ActionPolicyDisabled,
			RequiredFacts:    []string{"authority.primary_replica", "returned_replica.ack_eligible_true", "returned_replica.required_frontier_covered"},
			InvariantRefs:    []string{"INV-RETURNED-REPLICA-FENCING-001", "INV-RETURNED-REPLICA-FRONTIER-001"},
			EvidenceRequired: "returned_replica_failback_evidence",
			MutationAllowed:  false,
		},
	}
}
