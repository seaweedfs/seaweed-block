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
