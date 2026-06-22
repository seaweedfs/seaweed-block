package ops

import (
	"fmt"
	"strings"
	"time"
)

const (
	ManagedVolumeStatusInvalid    = "invalid"
	ManagedVolumeStatusUnsafe     = "unsafe"
	ManagedVolumeStatusBlocked    = "blocked"
	ManagedVolumeStatusRecovering = "recovering"
	ManagedVolumeStatusRecovered  = "recovered"
	ManagedVolumeStatusDegraded   = "degraded"
	ManagedVolumeStatusReady      = "ready"
	ManagedVolumeStatusUnknown    = "unknown"

	ManagedVolumeKubernetesUnknown = "unknown"
	ManagedVolumeKubernetesPending = "pending"
	ManagedVolumeKubernetesBound   = "bound"

	ManagedVolumeAuthorityUnknown          = "unknown"
	ManagedVolumeAuthorityPrimaryAvailable = "primary_available"
	ManagedVolumeAuthorityUnavailable      = "unavailable"
	ManagedVolumeAuthorityInvalid          = "invalid"

	ManagedVolumeCSIUnknown          = "unknown"
	ManagedVolumeCSIStaged           = "staged"
	ManagedVolumeCSIReattachObserved = "reattach_observed"
	ManagedVolumeCSIBlocked          = "blocked"

	ManagedVolumeHostPathUnknown          = "unknown"
	ManagedVolumeHostPathReady            = "ready"
	ManagedVolumeHostPathTransparentReady = "transparent_ready"
	ManagedVolumeHostPathBlocked          = "blocked"

	ManagedVolumeRecoveryNone       = "none"
	ManagedVolumeRecoveryRecovering = "recovering"
	ManagedVolumeRecoveryRecovered  = "recovered"
	ManagedVolumeRecoveryBlocked    = "blocked"

	ManagedVolumeWorkloadUnknown  = "unknown"
	ManagedVolumeWorkloadPending  = "pending"
	ManagedVolumeWorkloadVerified = "verified"

	ManagedVolumeActionModeReadOnly = "read_only"
	ManagedVolumeActionModeDryRun   = "dry_run"
	ManagedVolumeActionModeScripted = "scripted"

	ManagedVolumeSideEffectObserve           = "observe"
	ManagedVolumeSideEffectSafeK8S           = "safe_k8s"
	ManagedVolumeSideEffectDisruptiveK8S     = "disruptive_k8s"
	ManagedVolumeSideEffectAuthorityMutating = "authority_mutating"
	ManagedVolumeSideEffectRepairMutating    = "repair_mutating"
	ManagedVolumeSideEffectDestructive       = "destructive"

	ManagedVolumeActionCollectBundle          = "observe.collect_bundle"
	ManagedVolumeActionVerifyCleanup          = "observe.verify_cleanup"
	ManagedVolumeActionReinstallExternalISCSI = "safe_k8s.reinstall_external_iscsi"
	ManagedVolumeActionWaitForPVCBound        = "observe.wait_for_pvc_bound"
	ManagedVolumeActionInspectMountFailure    = "observe.inspect_mount_failure"
	ManagedVolumeActionImportCSIImage         = "safe_k8s.import_csi_image"
	ManagedVolumeActionInspectHostPath        = "observe.inspect_host_path"
	ManagedVolumeActionRequestPromotion       = "authority.request_promotion"
	ManagedVolumeActionReintegrateReturned    = "authority.reintegrate_returned_replica"
	ManagedVolumeActionRebuildReturned        = "authority.rebuild_returned_replica"

	ReasonMultiplePrimariesObserved      = "multiple_primaries_observed"
	ReasonPublishTargetLoopbackCrossNode = "publish_target_loopback_cross_node"
	ReasonCSIReattachRecovered           = "csi_reattach_recovered"
	ReasonFirstVolumeVerified            = "first_volume_verified"
	ReasonPVCUnbound                     = "pvc_unbound"
	ReasonPrimaryUnavailable             = "primary_unavailable"
	ReasonWriterMountFailed              = "writer_mount_failed"
	ReasonHostPathNotMultipathed         = "host_path_not_multipathed"
	ReasonTransparentHostPathRecovered   = "transparent_host_path_recovered"
	ReasonEvidenceStale                  = "evidence_stale"

	HostPathStateActiveOptimized = "active_optimized"
	HostPathStateSinglePath      = "single_path"
	HostPathStateANAOptimized    = "ana_optimized"

	NonClaimTransparentFailover = "transparent_failover_not_claimed"

	ReturnedReplicaStateFenced     = "fenced"
	ReturnedReplicaStateRecovering = "recovering"
	ReturnedReplicaStateReady      = "ready"
	ReturnedReplicaStateBlocked    = "blocked"
	ReturnedReplicaStateUnknown    = "unknown"
)

type FactMeta struct {
	Source     string    `json:"source,omitempty"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
	Generation uint64    `json:"generation,omitempty"`
	Confidence string    `json:"confidence,omitempty"`
}

type ManagedVolumeFacts struct {
	VolumeID            string               `json:"volume_id,omitempty"`
	Namespace           string               `json:"namespace,omitempty"`
	PVCName             string               `json:"pvc_name,omitempty"`
	PVName              string               `json:"pv_name,omitempty"`
	StorageClass        string               `json:"storage_class,omitempty"`
	ReplicationFactor   int                  `json:"replication_factor,omitempty"`
	AckProfile          string               `json:"ack_profile,omitempty"`
	ClaimProfile        string               `json:"claim_profile,omitempty"`
	Protocol            string               `json:"protocol,omitempty"`
	ProductStatus       string               `json:"product_status,omitempty"`
	ProductReason       string               `json:"product_reason,omitempty"`
	EvidenceStale       bool                 `json:"evidence_stale,omitempty"`
	EvidenceStaleReason string               `json:"evidence_stale_reason,omitempty"`
	KubernetesNodes     []KubernetesNodeFact `json:"kubernetes_nodes,omitempty"`
	PVC                 *PVCFact             `json:"pvc,omitempty"`
	PodMounts           []PodMountFact       `json:"pod_mounts,omitempty"`
	Authority           *AuthorityFact       `json:"authority,omitempty"`
	Replicas            []ReplicaFact        `json:"replicas,omitempty"`
	CSIStages           []CSIStageFact       `json:"csi_stages,omitempty"`
	HostPaths           []HostPathFact       `json:"host_paths,omitempty"`
	Workload            *WorkloadCheckFact   `json:"workload,omitempty"`
	EvidenceRefs        []string             `json:"evidence_refs,omitempty"`
}

type PVCFact struct {
	Meta  FactMeta `json:"meta,omitempty"`
	Phase string   `json:"phase,omitempty"`
}

type KubernetesNodeFact struct {
	Meta         FactMeta `json:"meta,omitempty"`
	NodeName     string   `json:"node_name,omitempty"`
	InternalIP   string   `json:"internal_ip,omitempty"`
	Ready        bool     `json:"ready"`
	Schedulable  bool     `json:"schedulable"`
	CSINodeReady bool     `json:"csi_node_ready"`
	Reason       string   `json:"reason,omitempty"`
	Message      string   `json:"message,omitempty"`
}

type PodMountFact struct {
	Meta     FactMeta `json:"meta,omitempty"`
	PodName  string   `json:"pod_name,omitempty"`
	NodeName string   `json:"node_name,omitempty"`
	Phase    string   `json:"phase,omitempty"`
	Reason   string   `json:"reason,omitempty"`
	Message  string   `json:"message,omitempty"`
}

type AuthorityFact struct {
	Meta                  FactMeta `json:"meta,omitempty"`
	PrimaryReplica        string   `json:"primary_replica,omitempty"`
	PreviousPrimary       string   `json:"previous_primary,omitempty"`
	PublishTarget         string   `json:"publish_target,omitempty"`
	Epoch                 uint64   `json:"epoch,omitempty"`
	EndpointVersion       uint64   `json:"endpoint_version,omitempty"`
	RequiredFrontierKnown bool     `json:"required_frontier_known,omitempty"`
	RequiredFrontierLSN   uint64   `json:"required_frontier_lsn,omitempty"`
}

type ReplicaFact struct {
	Meta                 FactMeta `json:"meta,omitempty"`
	ReplicaID            string   `json:"replica_id,omitempty"`
	ServerID             string   `json:"server_id,omitempty"`
	KubernetesNode       string   `json:"kubernetes_node,omitempty"`
	PhysicalHost         string   `json:"physical_host,omitempty"`
	Observed             bool     `json:"observed"`
	Role                 string   `json:"role,omitempty"`
	ReplicationRole      string   `json:"replication_role,omitempty"`
	DurableLatched       bool     `json:"durable_latched,omitempty"`
	DurableFrontierKnown bool     `json:"durable_frontier_known,omitempty"`
	DurableFrontierLSN   uint64   `json:"durable_frontier_lsn,omitempty"`
	Healthy              bool     `json:"healthy,omitempty"`
	FrontendPrimaryReady bool     `json:"frontend_primary_ready,omitempty"`
	AckEligibilityKnown  bool     `json:"ack_eligibility_known,omitempty"`
	AckEligible          bool     `json:"ack_eligible,omitempty"`
	FrontendProtocol     string   `json:"frontend_protocol,omitempty"`
	FrontendAddr         string   `json:"frontend_addr,omitempty"`
	StatusAddr           string   `json:"status_addr,omitempty"`
	StalePrimaryFenced   bool     `json:"stale_primary_fenced,omitempty"`
}

type CSIStageFact struct {
	Meta            FactMeta `json:"meta,omitempty"`
	NodeName        string   `json:"node_name,omitempty"`
	Target          string   `json:"target,omitempty"`
	Epoch           uint64   `json:"epoch,omitempty"`
	EndpointVersion uint64   `json:"endpoint_version,omitempty"`
	Reattach        bool     `json:"reattach,omitempty"`
}

type HostPathFact struct {
	Meta           FactMeta `json:"meta,omitempty"`
	NodeName       string   `json:"node_name,omitempty"`
	Protocol       string   `json:"protocol,omitempty"`
	Target         string   `json:"target,omitempty"`
	State          string   `json:"state,omitempty"`
	MultipathReady bool     `json:"multipath_ready,omitempty"`
	ALUAState      string   `json:"alua_state,omitempty"`
	ANAState       string   `json:"ana_state,omitempty"`
	StaleFenced    bool     `json:"stale_fenced,omitempty"`
}

type WorkloadCheckFact struct {
	Meta           FactMeta `json:"meta,omitempty"`
	WriterVerified bool     `json:"writer_verified,omitempty"`
	ReaderVerified bool     `json:"reader_verified,omitempty"`
	SamePodUID     bool     `json:"same_pod_uid,omitempty"`
}

type ManagedVolumeProjection struct {
	VolumeID              string                             `json:"volume_id,omitempty"`
	Namespace             string                             `json:"namespace,omitempty"`
	PVCName               string                             `json:"pvc_name,omitempty"`
	PVName                string                             `json:"pv_name,omitempty"`
	StorageClass          string                             `json:"storage_class,omitempty"`
	ReplicationFactor     int                                `json:"replication_factor,omitempty"`
	AckProfile            string                             `json:"ack_profile,omitempty"`
	ClaimProfile          string                             `json:"claim_profile,omitempty"`
	Status                string                             `json:"status"`
	ReasonCode            string                             `json:"reason_code,omitempty"`
	States                ManagedVolumeStates                `json:"states"`
	Actions               []ManagedVolumeAction              `json:"actions,omitempty"`
	Conditions            []ObservationCondition             `json:"conditions,omitempty"`
	DeleteSafety          *SwBlockVolumeDeleteSafetyDecision `json:"delete_safety,omitempty"`
	ReplicaReintegrations []ReturnedReplicaProjection        `json:"replica_reintegrations,omitempty"`
	NonClaims             []string                           `json:"non_claims,omitempty"`
	EvidenceRefs          []string                           `json:"evidence_refs,omitempty"`
}

type ReturnedReplicaProjection struct {
	ReplicaID             string   `json:"replica_id"`
	State                 string   `json:"state"`
	ReasonCode            string   `json:"reason_code,omitempty"`
	FrontendFenced        bool     `json:"frontend_fenced"`
	FrontendPrimaryReady  bool     `json:"frontend_primary_ready"`
	AckEligibilityKnown   bool     `json:"ack_eligibility_known"`
	AckEligible           bool     `json:"ack_eligible"`
	DurableFrontierKnown  bool     `json:"durable_frontier_known"`
	DurableFrontierLSN    uint64   `json:"durable_frontier_lsn,omitempty"`
	RequiredFrontierKnown bool     `json:"required_frontier_known,omitempty"`
	RequiredFrontierLSN   uint64   `json:"required_frontier_lsn,omitempty"`
	EvidenceRefs          []string `json:"evidence_refs,omitempty"`
}

type ManagedVolumeStates struct {
	Kubernetes string `json:"kubernetes"`
	Authority  string `json:"authority"`
	CSI        string `json:"csi"`
	HostPath   string `json:"host_path"`
	Recovery   string `json:"recovery"`
	Workload   string `json:"workload"`
}

type ManagedVolumeAction struct {
	Type             string   `json:"type"`
	Target           string   `json:"target,omitempty"`
	Mode             string   `json:"mode"`
	SideEffectClass  string   `json:"side_effect_class"`
	OwnerExecutor    string   `json:"owner_executor,omitempty"`
	Decision         string   `json:"decision,omitempty"`
	DecisionReason   string   `json:"decision_reason,omitempty"`
	MissingFacts     []string `json:"missing_facts,omitempty"`
	Preconditions    []string `json:"preconditions,omitempty"`
	InvariantRefs    []string `json:"invariant_refs,omitempty"`
	EvidenceRequired string   `json:"evidence_required,omitempty"`
	EvidenceRefs     []string `json:"evidence_refs,omitempty"`
}

type ManagedVolumeArtifactHints struct {
	NodeLoss       map[string]string `json:"node_loss,omitempty"`
	PrimaryFailure map[string]string `json:"primary_failure,omitempty"`
}

func ManagedVolumeFactsFromEvidence(volume VolumeEvidence, hints ManagedVolumeArtifactHints) ManagedVolumeFacts {
	facts := managedVolumeFactsFromVolumeEvidence(volume)
	applyNodeLossHintsToManagedVolumeFacts(&facts, hints.NodeLoss)
	applyPrimaryFailureHintsToManagedVolumeFacts(&facts, hints.PrimaryFailure)
	return facts
}

func RenderManagedVolumeProjectionText(projection ManagedVolumeProjection) string {
	var b strings.Builder
	fmt.Fprintf(&b, "managed_volume %s status=%s", explicitUnavailable(projection.VolumeID), emptyAsDash(projection.Status))
	if projection.ReasonCode != "" {
		fmt.Fprintf(&b, " reason=%s", projection.ReasonCode)
	}
	b.WriteByte('\n')
	fmt.Fprintf(&b, "managed_volume_state kubernetes=%s authority=%s csi=%s host_path=%s recovery=%s workload=%s\n",
		emptyAsDash(projection.States.Kubernetes),
		emptyAsDash(projection.States.Authority),
		emptyAsDash(projection.States.CSI),
		emptyAsDash(projection.States.HostPath),
		emptyAsDash(projection.States.Recovery),
		emptyAsDash(projection.States.Workload))
	if projection.DeleteSafety != nil {
		fmt.Fprintf(&b, "managed_volume_delete_safety state=%s decision=%s reason=%s release_allowed=%t action=%s\n",
			emptyAsDash(projection.DeleteSafety.State),
			emptyAsDash(projection.DeleteSafety.Decision),
			emptyAsDash(projection.DeleteSafety.Reason),
			projection.DeleteSafety.FinalizerReleaseAllowed,
			emptyAsDash(projection.DeleteSafety.ActionType))
		if projection.DeleteSafety.SafeNextAction != "" {
			fmt.Fprintf(&b, "managed_volume_delete_safety_safe_next_action %s\n", projection.DeleteSafety.SafeNextAction)
		}
		if len(projection.DeleteSafety.EvidenceRefs) > 0 {
			fmt.Fprintf(&b, "managed_volume_delete_safety_evidence %s\n", strings.Join(projection.DeleteSafety.EvidenceRefs, ","))
		}
	}
	for _, returned := range projection.ReplicaReintegrations {
		fmt.Fprintf(&b, "managed_volume_returned_replica=%s replica=%s state=%s reason=%s frontend_fenced=%t ack_eligibility_known=%t ack_eligible=%t durable_frontier_known=%t durable_lsn=%d required_frontier_known=%t required_lsn=%d\n",
			explicitUnavailable(projection.VolumeID),
			emptyAsDash(returned.ReplicaID),
			emptyAsDash(returned.State),
			emptyAsDash(returned.ReasonCode),
			returned.FrontendFenced,
			returned.AckEligibilityKnown,
			returned.AckEligible,
			returned.DurableFrontierKnown,
			returned.DurableFrontierLSN,
			returned.RequiredFrontierKnown,
			returned.RequiredFrontierLSN)
	}
	for _, preflight := range ReturnedReplicaExecutorPreflights(projection) {
		fmt.Fprintf(&b, "managed_volume_executor_preflight %s target=%s decision=%s reason=%s mode=%s executor=%s mutation_allowed=%t ack_eligibility_known=%t required_lsn=%d durable_lsn=%d\n",
			emptyAsDash(preflight.ActionType),
			emptyAsDash(preflight.ReplicaID),
			emptyAsDash(preflight.Decision),
			emptyAsDash(preflight.Reason),
			emptyAsDash(preflight.Mode),
			emptyAsDash(preflight.OwnerExecutor),
			preflight.MutationAllowed,
			preflight.AckEligibilityKnown,
			preflight.RequiredFrontierLSN,
			preflight.DurableFrontierLSN)
	}
	for _, contract := range ReturnedReplicaExecutorContracts(projection) {
		fmt.Fprintf(&b, "managed_volume_executor_contract %s target=%s decision=%s reason=%s executor=%s execution_enabled=%t mutation_allowed=%t allowed_mutation=%s terminal_evidence=%s\n",
			emptyAsDash(contract.ActionType),
			emptyAsDash(contract.ReplicaID),
			emptyAsDash(contract.Decision),
			emptyAsDash(contract.Reason),
			emptyAsDash(contract.OwnerExecutor),
			contract.ExecutionEnabled,
			contract.MutationAllowed,
			emptyAsDash(strings.Join(contract.AllowedMutationClass, ",")),
			emptyAsDash(strings.Join(contract.TerminalEvidenceRequired, ",")))
	}
	for _, condition := range projection.Conditions {
		fmt.Fprintf(&b, "managed_volume_condition %s status=%s reason=%s severity=%s",
			emptyAsDash(condition.Type),
			emptyAsDash(condition.Status),
			emptyAsDash(condition.Reason),
			emptyAsDash(condition.Severity))
		if condition.Message != "" {
			fmt.Fprintf(&b, " message=%q", condition.Message)
		}
		b.WriteByte('\n')
		if len(condition.EvidenceRefs) > 0 {
			fmt.Fprintf(&b, "managed_volume_condition_evidence %s %s\n",
				emptyAsDash(condition.Type),
				strings.Join(condition.EvidenceRefs, ","))
		}
	}
	for _, action := range projection.Actions {
		fmt.Fprintf(&b, "managed_volume_action %s mode=%s side_effect=%s executor=%s decision=%s",
			emptyAsDash(action.Type),
			emptyAsDash(action.Mode),
			emptyAsDash(action.SideEffectClass),
			emptyAsDash(action.OwnerExecutor),
			emptyAsDash(action.Decision))
		if action.DecisionReason != "" {
			fmt.Fprintf(&b, " reason=%s", action.DecisionReason)
		}
		b.WriteByte('\n')
		if len(action.MissingFacts) > 0 {
			fmt.Fprintf(&b, "managed_volume_action_missing_facts %s %s\n",
				emptyAsDash(action.Type),
				strings.Join(action.MissingFacts, ","))
		}
		if len(action.Preconditions) > 0 {
			fmt.Fprintf(&b, "managed_volume_action_preconditions %s %s\n",
				emptyAsDash(action.Type),
				strings.Join(action.Preconditions, ","))
		}
		if len(action.InvariantRefs) > 0 {
			fmt.Fprintf(&b, "managed_volume_action_invariants %s %s\n",
				emptyAsDash(action.Type),
				strings.Join(action.InvariantRefs, ","))
		}
		if len(action.EvidenceRefs) > 0 {
			fmt.Fprintf(&b, "managed_volume_action_evidence %s %s\n",
				emptyAsDash(action.Type),
				strings.Join(action.EvidenceRefs, ","))
		}
		if action.EvidenceRequired != "" {
			fmt.Fprintf(&b, "managed_volume_action_evidence_required %s %s\n",
				emptyAsDash(action.Type),
				action.EvidenceRequired)
		}
	}
	for _, nonClaim := range projection.NonClaims {
		fmt.Fprintf(&b, "managed_volume_non_claim %s\n", nonClaim)
	}
	return b.String()
}

func ProjectManagedVolumeFromEvidence(volume VolumeEvidence) ManagedVolumeProjection {
	return ProjectManagedVolume(ManagedVolumeFactsFromEvidence(volume, ManagedVolumeArtifactHints{}))
}

func managedVolumeFactsFromVolumeEvidence(volume VolumeEvidence) ManagedVolumeFacts {
	facts := ManagedVolumeFacts{
		VolumeID:          volume.VolumeID,
		Namespace:         volume.Namespace,
		PVCName:           volume.PVCName,
		PVName:            volume.PVName,
		ReplicationFactor: volume.ReplicationFactor,
		AckProfile:        volume.AckProfile,
		ClaimProfile:      volume.ClaimProfile,
		ProductStatus:     volume.Status,
		ProductReason:     volume.Reason,
		Authority: &AuthorityFact{
			PrimaryReplica:        volume.PrimaryReplica,
			PublishTarget:         volume.PublishTarget,
			Epoch:                 volume.Epoch,
			EndpointVersion:       volume.EndpointVersion,
			RequiredFrontierKnown: volume.RequiredFrontierKnown,
			RequiredFrontierLSN:   volume.RequiredFrontierLSN,
		},
	}
	if volume.PVCName != "" || volume.PVName != "" {
		facts.PVC = &PVCFact{Phase: "Bound"}
	}
	facts.Replicas = make([]ReplicaFact, 0, len(volume.Replicas))
	for _, replica := range volume.Replicas {
		facts.Replicas = append(facts.Replicas, ReplicaFact{
			ReplicaID:            replica.ReplicaID,
			ServerID:             replica.ServerID,
			KubernetesNode:       replica.KubernetesNode,
			PhysicalHost:         replica.PhysicalHost,
			Observed:             replica.Observed,
			Role:                 replica.Role,
			ReplicationRole:      replica.ReplicationRole,
			DurableLatched:       replica.DurableLatched,
			DurableFrontierKnown: replica.DurableFrontierKnown,
			DurableFrontierLSN:   replica.DurableFrontierLSN,
			Healthy:              replica.Healthy,
			FrontendPrimaryReady: replica.FrontendPrimaryReady,
			AckEligibilityKnown:  replica.AckEligibilityKnown,
			AckEligible:          replica.AckEligible,
			FrontendProtocol:     replica.FrontendProtocol,
			FrontendAddr:         replica.FrontendAddr,
			StatusAddr:           replica.StatusAddr,
			StalePrimaryFenced:   replica.StalePrimaryFenced,
		})
	}
	return facts
}

func ProjectManagedVolume(facts ManagedVolumeFacts) ManagedVolumeProjection {
	projection := ManagedVolumeProjection{
		VolumeID:          facts.VolumeID,
		Namespace:         facts.Namespace,
		PVCName:           facts.PVCName,
		PVName:            facts.PVName,
		StorageClass:      facts.StorageClass,
		ReplicationFactor: facts.ReplicationFactor,
		AckProfile:        facts.AckProfile,
		ClaimProfile:      facts.ClaimProfile,
		Status:            ManagedVolumeStatusUnknown,
		States: ManagedVolumeStates{
			Kubernetes: ManagedVolumeKubernetesUnknown,
			Authority:  ManagedVolumeAuthorityUnknown,
			CSI:        ManagedVolumeCSIUnknown,
			HostPath:   ManagedVolumeHostPathUnknown,
			Recovery:   ManagedVolumeRecoveryNone,
			Workload:   ManagedVolumeWorkloadUnknown,
		},
		EvidenceRefs: append([]string(nil), facts.EvidenceRefs...),
	}

	deriveManagedVolumeStates(&projection, facts)
	projection.ReplicaReintegrations = returnedReplicaProjections(facts)
	status, reason := classifyManagedVolume(projection, facts)
	projection.Status = status
	projection.ReasonCode = reason
	projection.NonClaims = managedVolumeNonClaims(projection, facts)
	projection.Actions = managedVolumeActionsForProjection(projection, facts)
	projection.Conditions = managedVolumeConditionsForProjection(projection)
	return projection
}

func applyNodeLossHintsToManagedVolumeFacts(facts *ManagedVolumeFacts, hints map[string]string) {
	if len(hints) == 0 {
		return
	}
	if promoted := hints["promoted"]; promoted != "" {
		replicaID, node := splitReplicaNode(promoted)
		facts.Authority.PrimaryReplica = defaultString(replicaID, facts.Authority.PrimaryReplica)
		facts.Authority.PublishTarget = defaultString(hints["after_frontend"], facts.Authority.PublishTarget)
		facts.Authority.PreviousPrimary = defaultString(splitReplicaOnly(hints["before_primary"]), facts.Authority.PreviousPrimary)
		markManagedVolumeReplicaPrimary(facts, replicaID, node, hints["after_frontend"])
	}
	if hints["reader_verified"] == "true" || hints["data_check_after_node_loss"] == "reader_checksum_passed" {
		if facts.Workload == nil {
			facts.Workload = &WorkloadCheckFact{}
		}
		facts.Workload.ReaderVerified = true
	}
	if hints["pod_recreate_used"] == "true" && facts.Authority != nil && facts.Authority.PublishTarget != "" {
		facts.CSIStages = append(facts.CSIStages, CSIStageFact{
			NodeName: facts.PrimaryNode(),
			Target:   facts.Authority.PublishTarget,
			Reattach: true,
		})
	}
}

func applyPrimaryFailureHintsToManagedVolumeFacts(facts *ManagedVolumeFacts, hints map[string]string) {
	if len(hints) == 0 {
		return
	}
	if promoted := hints["promoted_replica"]; promoted != "" && facts.Authority != nil {
		facts.Authority.PrimaryReplica = promoted
		markManagedVolumeReplicaPrimary(facts, promoted, "", facts.Authority.PublishTarget)
	}
	if hints["data_check_after_failover"] == "mounted_workload_checksum_passed" {
		if facts.Workload == nil {
			facts.Workload = &WorkloadCheckFact{}
		}
		facts.Workload.WriterVerified = true
		facts.Workload.ReaderVerified = true
		facts.Workload.SamePodUID = hints["pod_recreate_used"] == "false"
	}
	if hints["transparent_failover_claimed"] == "true" {
		facts.HostPaths = append(facts.HostPaths, HostPathFact{
			Protocol:       "iscsi",
			Target:         facts.PublishTarget(),
			State:          HostPathStateActiveOptimized,
			MultipathReady: true,
			StaleFenced:    hints["old_primary_stale_io_success_count"] == "0",
		})
	}
}

func (f ManagedVolumeFacts) PrimaryNode() string {
	if f.Authority == nil {
		return ""
	}
	for _, replica := range f.Replicas {
		if replica.ReplicaID == f.Authority.PrimaryReplica {
			return replica.KubernetesNode
		}
	}
	return ""
}

func (f ManagedVolumeFacts) PublishTarget() string {
	if f.Authority == nil {
		return ""
	}
	return f.Authority.PublishTarget
}

func deriveManagedVolumeStates(p *ManagedVolumeProjection, facts ManagedVolumeFacts) {
	if facts.PVC != nil {
		switch strings.ToLower(strings.TrimSpace(facts.PVC.Phase)) {
		case "bound":
			p.States.Kubernetes = ManagedVolumeKubernetesBound
		case "", "unknown":
			p.States.Kubernetes = ManagedVolumeKubernetesUnknown
		default:
			p.States.Kubernetes = ManagedVolumeKubernetesPending
		}
	}
	if facts.Authority != nil {
		if strings.TrimSpace(facts.Authority.PrimaryReplica) != "" {
			p.States.Authority = ManagedVolumeAuthorityPrimaryAvailable
		} else {
			p.States.Authority = ManagedVolumeAuthorityUnavailable
		}
	}
	if hasMultiplePrimaryReplicas(facts.Replicas) {
		p.States.Authority = ManagedVolumeAuthorityInvalid
	}
	if len(facts.CSIStages) > 0 {
		p.States.CSI = ManagedVolumeCSIStaged
		for _, stage := range facts.CSIStages {
			if stage.Reattach {
				p.States.CSI = ManagedVolumeCSIReattachObserved
				break
			}
		}
	}
	if hasBlockedCSINode(facts.KubernetesNodes) {
		p.States.CSI = ManagedVolumeCSIBlocked
	}
	if len(facts.HostPaths) > 0 {
		p.States.HostPath = ManagedVolumeHostPathReady
		for _, path := range facts.HostPaths {
			if strings.EqualFold(path.State, ManagedVolumeStatusBlocked) {
				p.States.HostPath = ManagedVolumeHostPathBlocked
				break
			}
			if path.MultipathReady && path.StaleFenced && strings.EqualFold(path.State, HostPathStateActiveOptimized) {
				p.States.HostPath = ManagedVolumeHostPathTransparentReady
			}
		}
	}
	if facts.Workload != nil {
		if facts.Workload.WriterVerified || facts.Workload.ReaderVerified {
			p.States.Workload = ManagedVolumeWorkloadVerified
		} else {
			p.States.Workload = ManagedVolumeWorkloadPending
		}
	}
	if facts.Authority != nil && strings.TrimSpace(facts.Authority.PreviousPrimary) != "" {
		p.States.Recovery = ManagedVolumeRecoveryRecovering
	}
	if p.States.CSI == ManagedVolumeCSIReattachObserved && facts.Workload != nil && facts.Workload.ReaderVerified {
		p.States.Recovery = ManagedVolumeRecoveryRecovered
	}
}

func classifyManagedVolume(p ManagedVolumeProjection, facts ManagedVolumeFacts) (string, string) {
	if p.States.Authority == ManagedVolumeAuthorityInvalid {
		return ManagedVolumeStatusInvalid, ReasonMultiplePrimariesObserved
	}
	if hasReturnedReplicaReason(p.ReplicaReintegrations, ReasonReturnedReplicaUnsafeFrontend) {
		return ManagedVolumeStatusBlocked, ReasonReturnedReplicaUnsafeFrontend
	}
	if facts.ProductReason == ReasonPublishTargetLoopbackCrossNode {
		return ManagedVolumeStatusBlocked, ReasonPublishTargetLoopbackCrossNode
	}
	if facts.ProductReason == ReasonCSINodeImagePullFailed {
		return ManagedVolumeStatusBlocked, ReasonCSINodeImagePullFailed
	}
	if facts.ProductReason == ReasonWALIntegrityFault {
		return ManagedVolumeStatusBlocked, ReasonWALIntegrityFault
	}
	if hasBlockedCSINode(facts.KubernetesNodes) {
		return ManagedVolumeStatusBlocked, ReasonCSINodeImagePullFailed
	}
	if isLoopbackCrossNode(facts) {
		return ManagedVolumeStatusBlocked, ReasonPublishTargetLoopbackCrossNode
	}
	if hasPodMountReason(facts.PodMounts, ReasonWriterMountFailed) {
		return ManagedVolumeStatusBlocked, ReasonWriterMountFailed
	}
	if facts.Authority != nil && facts.Authority.PreviousPrimary != "" && hasHostPathWithoutMultipath(facts.HostPaths) {
		return ManagedVolumeStatusBlocked, ReasonHostPathNotMultipathed
	}
	if p.States.Kubernetes == ManagedVolumeKubernetesPending {
		return ManagedVolumeStatusBlocked, ReasonPVCUnbound
	}
	if facts.EvidenceStale || facts.ProductReason == ReasonEvidenceStale {
		return ManagedVolumeStatusUnknown, defaultString(facts.EvidenceStaleReason, ReasonEvidenceStale)
	}
	if facts.ProductReason == ReasonStatusEndpointUnreachable {
		return ManagedVolumeStatusUnknown, ReasonStatusEndpointUnreachable
	}
	if p.States.HostPath == ManagedVolumeHostPathTransparentReady &&
		facts.Workload != nil &&
		facts.Workload.WriterVerified &&
		facts.Workload.ReaderVerified &&
		facts.Workload.SamePodUID {
		return ManagedVolumeStatusRecovered, ReasonTransparentHostPathRecovered
	}
	if p.States.Recovery == ManagedVolumeRecoveryRecovered {
		return ManagedVolumeStatusRecovered, ReasonCSIReattachRecovered
	}
	if p.States.Authority == ManagedVolumeAuthorityUnavailable {
		return ManagedVolumeStatusBlocked, ReasonPrimaryUnavailable
	}
	if p.States.Kubernetes == ManagedVolumeKubernetesBound &&
		p.States.Authority == ManagedVolumeAuthorityPrimaryAvailable &&
		p.States.CSI == ManagedVolumeCSIStaged &&
		p.States.Workload == ManagedVolumeWorkloadVerified {
		return ManagedVolumeStatusReady, ReasonFirstVolumeVerified
	}
	if facts.ProductStatus == ObservationStatusOK &&
		p.States.Kubernetes == ManagedVolumeKubernetesBound &&
		p.States.Authority == ManagedVolumeAuthorityPrimaryAvailable {
		return ManagedVolumeStatusReady, ReasonFirstVolumeVerified
	}
	if p.States.Recovery == ManagedVolumeRecoveryRecovering {
		return ManagedVolumeStatusRecovering, ""
	}
	return ManagedVolumeStatusUnknown, ""
}

func managedVolumeActionsForProjection(p ManagedVolumeProjection, facts ManagedVolumeFacts) []ManagedVolumeAction {
	actions := []ManagedVolumeAction{{
		Type:            ManagedVolumeActionCollectBundle,
		Mode:            ManagedVolumeActionModeReadOnly,
		SideEffectClass: ManagedVolumeSideEffectObserve,
		OwnerExecutor:   "ops",
		EvidenceRefs:    append([]string(nil), p.EvidenceRefs...),
	}}
	switch p.ReasonCode {
	case ReasonPublishTargetLoopbackCrossNode:
		actions = append(actions, ManagedVolumeAction{
			Type:            ManagedVolumeActionReinstallExternalISCSI,
			Target:          facts.PVCName,
			Mode:            ManagedVolumeActionModeDryRun,
			SideEffectClass: ManagedVolumeSideEffectSafeK8S,
			OwnerExecutor:   "installer_or_operator",
			Preconditions: []string{
				"multiple_kubernetes_nodes",
				"loopback_publish_target",
				"pod_scheduled_on_different_node",
			},
			InvariantRefs: []string{"INV-K8S-NONLOOPBACK-001"},
			EvidenceRefs:  append([]string(nil), p.EvidenceRefs...),
		})
	case ReasonPVCUnbound:
		actions = append(actions, ManagedVolumeAction{
			Type:            ManagedVolumeActionWaitForPVCBound,
			Target:          facts.PVCName,
			Mode:            ManagedVolumeActionModeDryRun,
			SideEffectClass: ManagedVolumeSideEffectObserve,
			OwnerExecutor:   "ops",
			Preconditions:   []string{"pvc_exists", "pvc_phase_not_bound"},
		})
	case ReasonWriterMountFailed:
		actions = append(actions, ManagedVolumeAction{
			Type:            ManagedVolumeActionInspectMountFailure,
			Target:          facts.PVCName,
			Mode:            ManagedVolumeActionModeDryRun,
			SideEffectClass: ManagedVolumeSideEffectObserve,
			OwnerExecutor:   "ops",
			Preconditions:   []string{"pod_mount_failed", "pvc_bound"},
			EvidenceRefs:    append([]string(nil), p.EvidenceRefs...),
		})
	case ReasonCSINodeImagePullFailed:
		actions = append(actions, ManagedVolumeAction{
			Type:            ManagedVolumeActionImportCSIImage,
			Target:          blockedCSINodeName(facts.KubernetesNodes),
			Mode:            ManagedVolumeActionModeDryRun,
			SideEffectClass: ManagedVolumeSideEffectSafeK8S,
			OwnerExecutor:   "installer_or_operator",
			Preconditions:   []string{"csi_node_not_ready", "image_pull_failed"},
			InvariantRefs:   []string{"INV-MANAGED-VOLUME-READMODEL-001"},
			EvidenceRefs:    append([]string(nil), p.EvidenceRefs...),
		})
	case ReasonHostPathNotMultipathed:
		actions = append(actions, ManagedVolumeAction{
			Type:            ManagedVolumeActionInspectHostPath,
			Target:          facts.PVCName,
			Mode:            ManagedVolumeActionModeDryRun,
			SideEffectClass: ManagedVolumeSideEffectObserve,
			OwnerExecutor:   "ops",
			Preconditions:   []string{"failover_context", "host_path_not_multipathed"},
			InvariantRefs:   []string{"INV-HOSTPATH-FACTS-001", "INV-HOSTPATH-TRANSPARENT-001"},
			EvidenceRefs:    append([]string(nil), p.EvidenceRefs...),
		})
	}
	for _, returned := range p.ReplicaReintegrations {
		if returned.State == ReturnedReplicaStateFenced {
			actions = append(actions, ManagedVolumeAction{
				Type:            ManagedVolumeActionReintegrateReturned,
				Target:          returned.ReplicaID,
				Mode:            ManagedVolumeActionModeDryRun,
				SideEffectClass: ManagedVolumeSideEffectAuthorityMutating,
				OwnerExecutor:   "authority_recovery_executor",
				Preconditions:   []string{"returned_replica_frontend_fenced", "durable_frontier_evidence"},
				InvariantRefs:   []string{"INV-RETURNED-REPLICA-FENCING-001"},
				EvidenceRefs:    append([]string(nil), returned.EvidenceRefs...),
			})
		}
		if returned.State == ReturnedReplicaStateRecovering || returned.ReasonCode == ReasonCandidateFrontierBehind || returned.ReasonCode == ReasonDurableFrontierMissing {
			actions = append(actions, ManagedVolumeAction{
				Type:            ManagedVolumeActionRebuildReturned,
				Target:          returned.ReplicaID,
				Mode:            ManagedVolumeActionModeDryRun,
				SideEffectClass: ManagedVolumeSideEffectAuthorityMutating,
				OwnerExecutor:   "authority_recovery_executor",
				Preconditions:   []string{"returned_replica_frontend_fenced", "required_frontier_known"},
				InvariantRefs:   []string{"INV-RETURNED-REPLICA-FENCING-001", "INV-RETURNED-REPLICA-FRONTIER-001"},
				EvidenceRefs:    append([]string(nil), returned.EvidenceRefs...),
			})
		}
	}
	for i := range actions {
		evaluation := EvaluateManagedVolumeAction(actions[i].Type, facts)
		actions[i].Decision = evaluation.Decision
		actions[i].DecisionReason = evaluation.Reason
		actions[i].MissingFacts = append([]string(nil), evaluation.MissingFacts...)
		actions[i].EvidenceRequired = evaluation.EvidenceRequired
		if len(actions[i].InvariantRefs) == 0 {
			actions[i].InvariantRefs = append([]string(nil), evaluation.InvariantRefs...)
		}
	}
	return actions
}

func managedVolumeNonClaims(p ManagedVolumeProjection, facts ManagedVolumeFacts) []string {
	var out []string
	if hasTransparentHostPathEvidence(facts.HostPaths) {
		if facts.Workload == nil || !facts.Workload.SamePodUID || !facts.Workload.WriterVerified || !facts.Workload.ReaderVerified {
			out = append(out, NonClaimTransparentFailover)
		}
	}
	return out
}

func returnedReplicaProjections(facts ManagedVolumeFacts) []ReturnedReplicaProjection {
	if facts.Authority == nil || facts.Authority.PrimaryReplica == "" {
		return nil
	}
	var out []ReturnedReplicaProjection
	for _, replica := range facts.Replicas {
		if !isReturnedReplicaCandidate(replica, *facts.Authority) {
			continue
		}
		projection := ReturnedReplicaProjection{
			ReplicaID:             replica.ReplicaID,
			FrontendPrimaryReady:  replica.FrontendPrimaryReady,
			FrontendFenced:        !replica.FrontendPrimaryReady,
			AckEligibilityKnown:   replica.AckEligibilityKnown,
			AckEligible:           replica.AckEligible,
			DurableFrontierKnown:  replica.DurableFrontierKnown,
			DurableFrontierLSN:    replica.DurableFrontierLSN,
			RequiredFrontierKnown: facts.Authority.RequiredFrontierKnown,
			RequiredFrontierLSN:   facts.Authority.RequiredFrontierLSN,
			EvidenceRefs:          append([]string(nil), facts.EvidenceRefs...),
		}
		switch {
		case replica.FrontendPrimaryReady:
			projection.State = ReturnedReplicaStateBlocked
			projection.ReasonCode = ReasonReturnedReplicaUnsafeFrontend
		case !replica.DurableFrontierKnown:
			projection.State = ReturnedReplicaStateRecovering
			projection.ReasonCode = ReasonDurableFrontierMissing
		case facts.Authority.RequiredFrontierKnown && replica.DurableFrontierLSN < facts.Authority.RequiredFrontierLSN:
			projection.State = ReturnedReplicaStateRecovering
			projection.ReasonCode = ReasonCandidateFrontierBehind
		default:
			projection.State = ReturnedReplicaStateFenced
			projection.ReasonCode = ReasonReturnedReplicaFrontendFenced
		}
		out = append(out, projection)
	}
	return out
}

func isReturnedReplicaCandidate(replica ReplicaFact, authority AuthorityFact) bool {
	if !replica.Observed || replica.ReplicaID == "" || replica.ReplicaID == authority.PrimaryReplica {
		return false
	}
	if authority.PreviousPrimary != "" && replica.ReplicaID == authority.PreviousPrimary {
		return true
	}
	role := strings.ToLower(strings.TrimSpace(replica.Role))
	if role == "returned" || role == "stale" || role == "previous_primary" {
		return true
	}
	return replica.StalePrimaryFenced
}

func hasReturnedReplicaReason(returned []ReturnedReplicaProjection, reason string) bool {
	for _, replica := range returned {
		if replica.ReasonCode == reason {
			return true
		}
	}
	return false
}

func managedVolumeConditionsForProjection(p ManagedVolumeProjection) []ObservationCondition {
	reason := defaultString(p.ReasonCode, p.Status)
	switch p.Status {
	case ManagedVolumeStatusReady:
		return []ObservationCondition{{
			Type:         "Ready",
			Status:       "True",
			Reason:       reason,
			Severity:     "info",
			Message:      "managed volume is ready for the documented path",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}}
	case ManagedVolumeStatusRecovered:
		return []ObservationCondition{{
			Type:         "Ready",
			Status:       "True",
			Reason:       reason,
			Severity:     "info",
			Message:      "managed volume recovered under the documented recovery path",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}, {
			Type:         "Recovered",
			Status:       "True",
			Reason:       reason,
			Severity:     "info",
			Message:      "recovery evidence is present",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}}
	case ManagedVolumeStatusBlocked:
		return []ObservationCondition{{
			Type:         "Ready",
			Status:       "False",
			Reason:       reason,
			Severity:     "warning",
			Message:      "managed volume is blocked; inspect dry-run actions and evidence refs",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}, {
			Type:         "Blocked",
			Status:       "True",
			Reason:       reason,
			Severity:     "warning",
			Message:      "a documented blocker prevents the expected user path",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}}
	case ManagedVolumeStatusInvalid, ManagedVolumeStatusUnsafe:
		return []ObservationCondition{{
			Type:         "Ready",
			Status:       "False",
			Reason:       reason,
			Severity:     "error",
			Message:      "managed volume state violates a safety invariant",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}, {
			Type:         "Invalid",
			Status:       "True",
			Reason:       reason,
			Severity:     "error",
			Message:      "do not infer recovery or readiness from this state",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}}
	case ManagedVolumeStatusRecovering:
		return []ObservationCondition{{
			Type:         "Ready",
			Status:       "False",
			Reason:       reason,
			Severity:     "info",
			Message:      "managed volume recovery is in progress",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}, {
			Type:         "Recovering",
			Status:       "True",
			Reason:       reason,
			Severity:     "info",
			Message:      "wait for recovery evidence before claiming readiness",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}}
	case ManagedVolumeStatusDegraded:
		return []ObservationCondition{{
			Type:         "Ready",
			Status:       "False",
			Reason:       reason,
			Severity:     "warning",
			Message:      "managed volume is degraded",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}}
	default:
		conditions := []ObservationCondition{{
			Type:         "Ready",
			Status:       "Unknown",
			Reason:       reason,
			Severity:     "info",
			Message:      "insufficient managed volume facts",
			EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
		}}
		if reason == ReasonEvidenceStale || reason == ReasonStatusEndpointUnreachable {
			conditions[0].Severity = "warning"
			conditions[0].Message = "managed volume evidence is stale or unreachable; readiness is not claimed"
			conditions = append(conditions, ObservationCondition{
				Type:         ConditionEvidenceStale,
				Status:       "True",
				Reason:       reason,
				Severity:     "warning",
				Message:      "bounded probe or fresh evidence is required before claiming readiness",
				EvidenceRefs: append([]string(nil), p.EvidenceRefs...),
			})
		}
		return conditions
	}
}

func hasMultiplePrimaryReplicas(replicas []ReplicaFact) bool {
	count := 0
	for _, replica := range replicas {
		if strings.EqualFold(strings.TrimSpace(replica.Role), "primary") {
			count++
		}
	}
	return count > 1
}

func isLoopbackCrossNode(facts ManagedVolumeFacts) bool {
	if facts.Authority == nil || !isLoopbackAddress(facts.Authority.PublishTarget) {
		return false
	}
	primaryNode := ""
	for _, replica := range facts.Replicas {
		if replica.ReplicaID == facts.Authority.PrimaryReplica {
			primaryNode = replica.KubernetesNode
			break
		}
	}
	if primaryNode == "" {
		return false
	}
	for _, stage := range facts.CSIStages {
		if isLoopbackAddress(stage.Target) && stage.NodeName != "" && stage.NodeName != primaryNode {
			return true
		}
	}
	for _, mount := range facts.PodMounts {
		if mount.NodeName != "" && mount.NodeName != primaryNode {
			return true
		}
	}
	return false
}

func isLoopbackAddress(addr string) bool {
	addr = strings.TrimSpace(addr)
	return strings.HasPrefix(addr, "127.") || strings.HasPrefix(addr, "localhost:")
}

func markManagedVolumeReplicaPrimary(facts *ManagedVolumeFacts, replicaID, node, frontend string) {
	if replicaID == "" {
		return
	}
	for i := range facts.Replicas {
		if facts.Replicas[i].ReplicaID != replicaID {
			continue
		}
		facts.Replicas[i].Role = "primary"
		facts.Replicas[i].Observed = true
		if node != "" {
			facts.Replicas[i].KubernetesNode = node
		}
		if frontend != "" {
			facts.Replicas[i].FrontendAddr = frontend
		}
		return
	}
	facts.Replicas = append(facts.Replicas, ReplicaFact{
		ReplicaID:      replicaID,
		KubernetesNode: node,
		Role:           "primary",
		Observed:       true,
		FrontendAddr:   frontend,
	})
}

func splitReplicaOnly(raw string) string {
	replica, _, _ := strings.Cut(raw, "@")
	return replica
}

func hasPodMountReason(mounts []PodMountFact, reason string) bool {
	for _, mount := range mounts {
		if mount.Reason == reason {
			return true
		}
	}
	return false
}

func hasBlockedCSINode(nodes []KubernetesNodeFact) bool {
	for _, node := range nodes {
		if !node.CSINodeReady && node.Reason == ReasonCSINodeImagePullFailed {
			return true
		}
	}
	return false
}

func blockedCSINodeName(nodes []KubernetesNodeFact) string {
	for _, node := range nodes {
		if !node.CSINodeReady && node.Reason == ReasonCSINodeImagePullFailed {
			return node.NodeName
		}
	}
	return ""
}

func hasHostPathWithoutMultipath(paths []HostPathFact) bool {
	for _, path := range paths {
		if strings.EqualFold(path.Protocol, "iscsi") && !path.MultipathReady {
			return true
		}
	}
	return false
}

func hasTransparentHostPathEvidence(paths []HostPathFact) bool {
	for _, path := range paths {
		if path.MultipathReady && path.StaleFenced && strings.EqualFold(path.State, HostPathStateActiveOptimized) {
			return true
		}
	}
	return false
}
