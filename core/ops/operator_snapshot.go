package ops

import "time"

const ObservationOperatorSnapshotArtifact = "operator-snapshot.json"

type OperatorFoundationSnapshot struct {
	APIVersion  string                          `json:"api_version"`
	Kind        string                          `json:"kind"`
	ReadOnly    bool                            `json:"read_only"`
	Mutation    OperatorMutationBoundary        `json:"mutation"`
	CRDContract ManagedVolumeCRDContract        `json:"crd_contract"`
	Cluster     OperatorClusterStatus           `json:"cluster"`
	Volumes     []ManagedVolumeOperatorContract `json:"volumes"`
}

type OperatorMutationBoundary struct {
	MutationAllowed bool     `json:"mutation_allowed"`
	AllowedModes    []string `json:"allowed_modes"`
	NonClaims       []string `json:"non_claims"`
}

type OperatorClusterStatus struct {
	Status             string                 `json:"status"`
	NodeCount          int                    `json:"node_count"`
	Nodes              []OperatorNodeStatus   `json:"nodes,omitempty"`
	VolumeCount        int                    `json:"volume_count"`
	ReadyVolumeCount   int                    `json:"ready_volume_count"`
	BlockedVolumeCount int                    `json:"blocked_volume_count"`
	StaleVolumeCount   int                    `json:"stale_volume_count"`
	Cleanup            *CleanupEvidence       `json:"cleanup,omitempty"`
	Conditions         []ObservationCondition `json:"conditions,omitempty"`
	NonClaims          []string               `json:"non_claims,omitempty"`
}

type OperatorNodeStatus struct {
	Name            string                 `json:"name"`
	KubernetesNode  string                 `json:"kubernetes_node,omitempty"`
	InternalIP      string                 `json:"internal_ip,omitempty"`
	Schedulable     bool                   `json:"schedulable"`
	Ready           bool                   `json:"ready"`
	Status          string                 `json:"status,omitempty"`
	ReasonCode      string                 `json:"reason_code,omitempty"`
	LastHeartbeatAt time.Time              `json:"last_heartbeat_at,omitempty"`
	ReplicaCount    int                    `json:"replica_count,omitempty"`
	RequiredImages  []string               `json:"required_images,omitempty"`
	MissingImages   []string               `json:"missing_images,omitempty"`
	Conditions      []ObservationCondition `json:"conditions,omitempty"`
	EvidenceRefs    []string               `json:"evidence_refs,omitempty"`
}

func BuildOperatorFoundationSnapshot(cluster ClusterEvidence) OperatorFoundationSnapshot {
	cluster = NormalizeObservationCluster(cluster)
	snapshot := OperatorFoundationSnapshot{
		APIVersion: SwBlockVolumeAPIVersion,
		Kind:       "ReadOnlyOperatorFoundationSnapshot",
		ReadOnly:   true,
		Mutation: OperatorMutationBoundary{
			MutationAllowed: false,
			AllowedModes: []string{
				ManagedVolumeActionModeReadOnly,
				ManagedVolumeActionModeDryRun,
			},
			NonClaims: []string{
				"no_promote",
				"no_repair",
				"no_rebuild",
				"no_failback",
				"no_delete",
				"no_cleanup_mutation",
			},
		},
		CRDContract: ManagedVolumeCRDContractDefinition(),
		Cluster: OperatorClusterStatus{
			Status:     cluster.Status,
			NodeCount:  len(cluster.Nodes),
			Nodes:      operatorNodeStatuses(cluster.Nodes),
			Cleanup:    cluster.Cleanup,
			Conditions: append([]ObservationCondition(nil), cluster.Conditions...),
			NonClaims:  append([]string(nil), cluster.NonClaims...),
		},
	}

	for _, managed := range cluster.ManagedVolumes {
		contract := ManagedVolumeOperatorContractFromProjection(managed)
		snapshot.Volumes = append(snapshot.Volumes, contract)
		snapshot.Cluster.VolumeCount++
		switch managed.Status {
		case ManagedVolumeStatusReady, ManagedVolumeStatusRecovered:
			snapshot.Cluster.ReadyVolumeCount++
		case ManagedVolumeStatusBlocked, ManagedVolumeStatusInvalid, ManagedVolumeStatusUnsafe:
			snapshot.Cluster.BlockedVolumeCount++
		}
		if managed.ReasonCode == ReasonEvidenceStale || hasCondition(managed.Conditions, ConditionEvidenceStale, "True") {
			snapshot.Cluster.StaleVolumeCount++
		}
	}
	if snapshot.Cluster.StaleVolumeCount > 0 && !hasCondition(snapshot.Cluster.Conditions, ConditionEvidenceStale, "True") {
		snapshot.Cluster.Conditions = append(snapshot.Cluster.Conditions, ObservationCondition{
			Type:     ConditionEvidenceStale,
			Status:   "True",
			Reason:   ReasonEvidenceStale,
			Severity: "warning",
			Message:  "one or more managed volumes have stale or unreachable evidence",
		})
	}
	return snapshot
}

func operatorNodeStatuses(nodes []NodeEvidence) []OperatorNodeStatus {
	if len(nodes) == 0 {
		return nil
	}
	out := make([]OperatorNodeStatus, 0, len(nodes))
	for _, node := range nodes {
		status, reason := classifyNodeReadiness(node)
		out = append(out, OperatorNodeStatus{
			Name:            defaultString(node.NodeName, node.KubernetesNode),
			KubernetesNode:  node.KubernetesNode,
			InternalIP:      node.InternalIP,
			Schedulable:     node.Schedulable,
			Ready:           node.Ready,
			Status:          status,
			ReasonCode:      reason,
			LastHeartbeatAt: node.LastHeartbeatAt,
			ReplicaCount:    node.ReplicaCount,
			RequiredImages:  append([]string(nil), node.RequiredImages...),
			MissingImages:   append([]string(nil), node.MissingImages...),
			Conditions:      nodeReadinessConditions(node, status, reason),
			EvidenceRefs:    nodeEvidenceRefs(node),
		})
	}
	return out
}

func hasCondition(conditions []ObservationCondition, conditionType, status string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && (status == "" || condition.Status == status) {
			return true
		}
	}
	return false
}
