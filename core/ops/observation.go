package ops

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	ObservationSchemaVersion = "1.0"

	ObservationStatusOK          = "ok"
	ObservationStatusDegraded    = "degraded"
	ObservationStatusRecovering  = "recovering"
	ObservationStatusBlocked     = "blocked"
	ObservationStatusInvalid     = "invalid"
	ObservationStatusUnavailable = "unavailable"

	ReasonPrimaryNodeLost                 = "primary_node_lost"
	ReasonCandidateCoversRequiredFrontier = "candidate_covers_required_frontier"
	ReasonNoPromotionReadyCandidate       = "no_promotion_ready_candidate"
	ReasonDurableFrontierMissing          = "durable_frontier_missing"
	ReasonCandidateFrontierBehind         = "candidate_frontier_behind"
	ReasonStatusEndpointUnreachable       = "status_endpoint_unreachable"
	ReasonCSINodeImagePullFailed          = "csi_node_image_pull_failed"
	ReasonCSIDriverNotRegistered          = "csi_driver_not_registered"
	ReasonCSINodePodNotReady              = "csi_node_pod_not_ready"
	ReasonImageMissingOnNode              = "image_missing_on_node"
	ReasonISCSIPrereqMissing              = "iscsi_prereq_missing"
	ReasonMultipathPrereqMissing          = "multipath_prereq_missing"
	ReasonNodeReady                       = "node_ready"
	ReasonNodeNotReady                    = "node_not_ready"
	ReasonNodeSchedulingDisabled          = "node_scheduling_disabled"
	ReasonGeneratedDeploymentMissing      = "generated_deployment_missing"
	ReasonObservedReplicasBelowDesired    = "observed_replicas_below_desired"
	ReasonLoopbackFrontendRejected        = "loopback_frontend_rejected"
	ReasonStalePrimaryFenced              = "stale_primary_fenced"
	ReasonWALIntegrityFault               = "wal_integrity_fault"
	ReasonInstallDriftAligned             = "install_drift_aligned"
	ReasonInstallDriftMismatch            = "install_drift_mismatch"
	ReasonInstallDriftEvidenceMissing     = "install_drift_evidence_missing"

	EventTypeCSIReattachObserved = "csi_reattach_observed"
)

type ObservationCondition struct {
	Type         string   `json:"type"`
	Status       string   `json:"status"`
	Reason       string   `json:"reason"`
	Severity     string   `json:"severity"`
	Message      string   `json:"message,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type ClusterEvidence struct {
	SchemaVersion   string                    `json:"schema_version"`
	CapturedAt      time.Time                 `json:"captured_at"`
	ProductRevision string                    `json:"product_revision,omitempty"`
	Status          string                    `json:"status"`
	Nodes           []NodeEvidence            `json:"nodes"`
	Volumes         []VolumeEvidence          `json:"volumes"`
	ManagedVolumes  []ManagedVolumeProjection `json:"managed_volumes,omitempty"`
	Cleanup         *CleanupEvidence          `json:"cleanup,omitempty"`
	InstallDrift    *InstallDriftEvidence     `json:"install_drift,omitempty"`
	Conditions      []ObservationCondition    `json:"conditions,omitempty"`
	Events          []ClusterEvent            `json:"events,omitempty"`
	NonClaims       []string                  `json:"non_claims,omitempty"`
}

type InstallDriftEvidence struct {
	Status               string `json:"status"`
	ReasonCode           string `json:"reason_code,omitempty"`
	ChartName            string `json:"chart_name,omitempty"`
	CurrentChartVersion  string `json:"current_chart_version,omitempty"`
	DesiredChartVersion  string `json:"desired_chart_version,omitempty"`
	CurrentAppVersion    string `json:"current_app_version,omitempty"`
	DesiredAppVersion    string `json:"desired_app_version,omitempty"`
	CurrentImage         string `json:"current_image,omitempty"`
	DesiredImage         string `json:"desired_image,omitempty"`
	CurrentCSIImage      string `json:"current_csi_image,omitempty"`
	DesiredCSIImage      string `json:"desired_csi_image,omitempty"`
	CurrentOperatorImage string `json:"current_operator_image,omitempty"`
	DesiredOperatorImage string `json:"desired_operator_image,omitempty"`
	EvidenceRef          string `json:"evidence_ref,omitempty"`
}

type CleanupEvidence struct {
	Status                 string    `json:"status"`
	ObservedAt             time.Time `json:"observed_at,omitempty"`
	KubernetesResidueCount int       `json:"k8s_residue_count,omitempty"`
	ISCSIResidueCount      int       `json:"iscsi_residue_count,omitempty"`
	MultipathResidueCount  int       `json:"multipath_residue_count,omitempty"`
	ProcessResidueCount    int       `json:"process_residue_count,omitempty"`
	HostPathResidueCount   int       `json:"hostpath_residue_count,omitempty"`
	FailureCount           int       `json:"failure_count,omitempty"`
	FailedPhase            string    `json:"failed_phase,omitempty"`
	ReasonCodes            []string  `json:"reason_codes,omitempty"`
	EvidenceRef            string    `json:"evidence_ref,omitempty"`
}

type NodeEvidence struct {
	NodeName        string                 `json:"node_name"`
	KubernetesNode  string                 `json:"kubernetes_node,omitempty"`
	PhysicalHost    string                 `json:"physical_host,omitempty"`
	InternalIP      string                 `json:"internal_ip,omitempty"`
	Schedulable     bool                   `json:"schedulable"`
	Ready           bool                   `json:"ready"`
	LastHeartbeatAt time.Time              `json:"last_heartbeat_at,omitempty"`
	ReplicaCount    int                    `json:"replica_count"`
	RequiredImages  []string               `json:"required_images,omitempty"`
	MissingImages   []string               `json:"missing_images,omitempty"`
	Conditions      []ObservationCondition `json:"conditions,omitempty"`
}

type VolumeEvidence struct {
	VolumeID          string                 `json:"volume_id"`
	Namespace         string                 `json:"namespace,omitempty"`
	PVCName           string                 `json:"pvc_name,omitempty"`
	PVName            string                 `json:"pv_name,omitempty"`
	ReplicationFactor int                    `json:"replication_factor"`
	AckProfile        string                 `json:"ack_profile,omitempty"`
	ClaimProfile      string                 `json:"claim_profile,omitempty"`
	DesiredReplicas   int                    `json:"desired_replicas"`
	ObservedReplicas  int                    `json:"observed_replicas"`
	Status            string                 `json:"status"`
	Reason            string                 `json:"reason,omitempty"`
	PrimaryReplica    string                 `json:"primary_replica,omitempty"`
	PrimaryNode       string                 `json:"primary_node,omitempty"`
	PublishTarget     string                 `json:"publish_target,omitempty"`
	Epoch             uint64                 `json:"epoch,omitempty"`
	EndpointVersion   uint64                 `json:"endpoint_version,omitempty"`
	Replicas          []ReplicaEvidence      `json:"replicas"`
	Conditions        []ObservationCondition `json:"conditions,omitempty"`
	NextActions       []string               `json:"next_actions,omitempty"`
	SupportBundleHint string                 `json:"support_bundle_hint,omitempty"`
}

type ReplicaEvidence struct {
	ReplicaID            string                 `json:"replica_id"`
	ServerID             string                 `json:"server_id,omitempty"`
	KubernetesNode       string                 `json:"kubernetes_node,omitempty"`
	PhysicalHost         string                 `json:"physical_host,omitempty"`
	Observed             bool                   `json:"observed"`
	Role                 string                 `json:"role,omitempty"`
	ReplicationRole      string                 `json:"replication_role,omitempty"`
	DurableLatched       bool                   `json:"durable_latched"`
	DurableFrontierKnown bool                   `json:"durable_frontier_known"`
	DurableFrontierLSN   uint64                 `json:"durable_frontier_lsn,omitempty"`
	CandidateReady       bool                   `json:"candidate_ready"`
	CandidateReadyReason string                 `json:"candidate_ready_reason,omitempty"`
	FrontendProtocol     string                 `json:"frontend_protocol,omitempty"`
	FrontendAddr         string                 `json:"frontend_addr,omitempty"`
	StatusAddr           string                 `json:"status_addr,omitempty"`
	StalePrimaryFenced   bool                   `json:"stale_primary_fenced"`
	Conditions           []ObservationCondition `json:"conditions,omitempty"`
	SupportBundlePath    string                 `json:"support_bundle_path,omitempty"`
}

type ClusterEvent struct {
	EventID         string    `json:"event_id"`
	EventTime       time.Time `json:"event_time"`
	VolumeID        string    `json:"volume_id,omitempty"`
	ReplicaID       string    `json:"replica_id,omitempty"`
	NodeName        string    `json:"node_name,omitempty"`
	Type            string    `json:"event_type"`
	Severity        string    `json:"severity"`
	Message         string    `json:"message"`
	Reason          string    `json:"reason_code,omitempty"`
	OldValue        string    `json:"old_value,omitempty"`
	NewValue        string    `json:"new_value,omitempty"`
	Epoch           uint64    `json:"epoch,omitempty"`
	EndpointVersion uint64    `json:"endpoint_version,omitempty"`
	CorrelationID   string    `json:"correlation_id,omitempty"`
	EvidenceRef     string    `json:"evidence_ref,omitempty"`
}

func NewClusterEvidence(capturedAt time.Time) ClusterEvidence {
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	return ClusterEvidence{
		SchemaVersion: ObservationSchemaVersion,
		CapturedAt:    capturedAt.UTC(),
		Status:        ObservationStatusOK,
		NonClaims: []string{
			"read-only-observation: does not mutate product, Kubernetes, iSCSI, or replica state",
			"not-an-admin-action: no promote, repair, rebuild, backup, restore, or cleanup is authorized",
		},
	}
}

func RenderClusterEvidenceText(cluster ClusterEvidence) string {
	status := defaultString(cluster.Status, ObservationStatusUnavailable)
	var b strings.Builder
	fmt.Fprintf(&b, "cluster status=%s volumes=%d nodes=%d\n", status, len(cluster.Volumes), len(cluster.Nodes))
	for _, condition := range cluster.Conditions {
		fmt.Fprintf(&b, "condition %s severity=%s reason=%s %s\n", condition.Type, condition.Severity, condition.Reason, condition.Message)
	}
	for _, volume := range cluster.Volumes {
		b.WriteString(RenderVolumeEvidenceText(volume))
	}
	return b.String()
}

func RenderVolumeEvidenceText(volume VolumeEvidence) string {
	var b strings.Builder
	fmt.Fprintf(&b, "volume %s status=%s", explicitUnavailable(volume.VolumeID), defaultString(volume.Status, ObservationStatusUnavailable))
	if volume.ReplicationFactor > 0 {
		fmt.Fprintf(&b, " rf=%d", volume.ReplicationFactor)
	}
	if volume.AckProfile != "" {
		fmt.Fprintf(&b, " ack=%s", volume.AckProfile)
	}
	if volume.Reason != "" {
		fmt.Fprintf(&b, " reason=%s", volume.Reason)
	}
	b.WriteByte('\n')
	if volume.PVCName != "" || volume.Namespace != "" {
		fmt.Fprintf(&b, "pvc %s/%s\n", emptyAsDash(volume.Namespace), emptyAsDash(volume.PVCName))
	}
	if volume.PrimaryReplica != "" || volume.PrimaryNode != "" || volume.PublishTarget != "" {
		fmt.Fprintf(&b, "primary %s on %s frontend=%s\n", emptyAsDash(volume.PrimaryReplica), emptyAsDash(volume.PrimaryNode), emptyAsDash(volume.PublishTarget))
	}
	if volume.DesiredReplicas > 0 || volume.ObservedReplicas > 0 {
		fmt.Fprintf(&b, "replicas desired=%d observed=%d\n", volume.DesiredReplicas, volume.ObservedReplicas)
	}
	for _, replica := range volume.Replicas {
		fmt.Fprintf(&b, "%s %s %s %s", explicitUnavailable(replica.ReplicaID), emptyAsDash(replica.KubernetesNode), emptyAsDash(replica.Role), emptyAsDash(replica.ReplicationRole))
		if replica.DurableFrontierKnown {
			fmt.Fprintf(&b, " durable_lsn=%d", replica.DurableFrontierLSN)
		}
		if replica.CandidateReady {
			b.WriteString(" candidate_ready=true")
		} else if replica.CandidateReadyReason != "" {
			fmt.Fprintf(&b, " candidate_ready=false reason=%s", replica.CandidateReadyReason)
		}
		if replica.StalePrimaryFenced {
			b.WriteString(" stale_primary_fenced=true")
		}
		b.WriteByte('\n')
		for _, condition := range replica.Conditions {
			fmt.Fprintf(&b, "  condition %s severity=%s reason=%s %s\n", condition.Type, condition.Severity, condition.Reason, condition.Message)
		}
	}
	for _, condition := range volume.Conditions {
		fmt.Fprintf(&b, "condition %s severity=%s reason=%s %s\n", condition.Type, condition.Severity, condition.Reason, condition.Message)
	}
	for _, action := range volume.NextActions {
		fmt.Fprintf(&b, "next action: %s\n", action)
	}
	if volume.SupportBundleHint != "" {
		fmt.Fprintf(&b, "support bundle: %s\n", volume.SupportBundleHint)
	}
	return b.String()
}

func MarshalObservationJSON(v any) ([]byte, error) {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func RenderClusterEventsJSONL(events []ClusterEvent) (string, error) {
	ordered := append([]ClusterEvent(nil), events...)
	fallbackNow := time.Now().UTC()
	for i := range ordered {
		if ordered[i].EventTime.IsZero() {
			ordered[i].EventTime = fallbackNow
		} else {
			ordered[i].EventTime = ordered[i].EventTime.UTC()
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].EventTime.Before(ordered[j].EventTime)
	})
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, event := range ordered {
		if err := enc.Encode(event); err != nil {
			return "", err
		}
	}
	return b.String(), nil
}
