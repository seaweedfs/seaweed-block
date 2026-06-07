package ops

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"
)

const DefaultSwBlockClusterName = "sw-block"

type OperatorStatusSource interface {
	ClusterEvidence(ctx context.Context) (ClusterEvidence, error)
}

type OperatorNodeEvidenceEnricher interface {
	EnrichNodeEvidence(ctx context.Context, namespace string, cluster ClusterEvidence) (ClusterEvidence, error)
}

type OperatorStatusWriter interface {
	WriteClusterStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockClusterCRDStatus) error
	WriteVolumeStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockVolumeCRDStatus) error
}

type OperatorEventSink interface {
	EmitEvent(ctx context.Context, event OperatorKubernetesEvent) error
}

type OperatorObjectRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
}

type SwBlockClusterCRDStatus struct {
	ObservedAt         time.Time              `json:"observedAt,omitempty"`
	ObservedGeneration int64                  `json:"observedGeneration,omitempty"`
	NodeCount          int                    `json:"nodeCount"`
	Nodes              []SwBlockNodeCRDStatus `json:"nodes,omitempty"`
	VolumeCount        int                    `json:"volumeCount"`
	ReadyVolumeCount   int                    `json:"readyVolumeCount"`
	BlockedVolumeCount int                    `json:"blockedVolumeCount"`
	StaleVolumeCount   int                    `json:"staleVolumeCount"`
	Conditions         []ObservationCondition `json:"conditions,omitempty"`
	EvidenceRefs       []string               `json:"evidenceRefs,omitempty"`
	SupportBundleRefs  []string               `json:"supportBundleRefs,omitempty"`
	Cleanup            *SwBlockCleanupStatus  `json:"cleanup,omitempty"`
	SafeNextSteps      []SwBlockSafeNextStep  `json:"safeNextSteps,omitempty"`
	MutationAllowed    bool                   `json:"mutationAllowed"`
	AllowedActionModes []string               `json:"allowedActionModes,omitempty"`
	NonClaims          []string               `json:"nonClaims,omitempty"`
}

type SwBlockNodeCRDStatus struct {
	Name            string                 `json:"name"`
	KubernetesNode  string                 `json:"kubernetesNode,omitempty"`
	InternalIP      string                 `json:"internalIP,omitempty"`
	Schedulable     bool                   `json:"schedulable"`
	Ready           bool                   `json:"ready"`
	Status          string                 `json:"status,omitempty"`
	ReasonCode      string                 `json:"reasonCode,omitempty"`
	LastHeartbeatAt time.Time              `json:"lastHeartbeatAt,omitempty"`
	ReplicaCount    int                    `json:"replicaCount,omitempty"`
	RequiredImages  []string               `json:"requiredImages,omitempty"`
	MissingImages   []string               `json:"missingImages,omitempty"`
	Conditions      []ObservationCondition `json:"conditions,omitempty"`
	EvidenceRefs    []string               `json:"evidenceRefs,omitempty"`
}

type SwBlockSafeNextStep struct {
	Type            string   `json:"type"`
	Mode            string   `json:"mode"`
	Command         string   `json:"command,omitempty"`
	ReasonCode      string   `json:"reasonCode,omitempty"`
	MutationAllowed bool     `json:"mutationAllowed"`
	EvidenceRefs    []string `json:"evidenceRefs,omitempty"`
}

type SwBlockCleanupStatus struct {
	Status                 string   `json:"status"`
	KubernetesResidueCount int      `json:"k8sResidueCount,omitempty"`
	ISCSIResidueCount      int      `json:"iscsiResidueCount,omitempty"`
	MultipathResidueCount  int      `json:"multipathResidueCount,omitempty"`
	ProcessResidueCount    int      `json:"processResidueCount,omitempty"`
	HostPathResidueCount   int      `json:"hostPathResidueCount,omitempty"`
	FailureCount           int      `json:"failureCount,omitempty"`
	FailedPhase            string   `json:"failedPhase,omitempty"`
	ReasonCodes            []string `json:"reasonCodes,omitempty"`
	EvidenceRef            string   `json:"evidenceRef,omitempty"`
}

func swBlockCleanupStatus(cleanup *CleanupEvidence) *SwBlockCleanupStatus {
	if cleanup == nil {
		return nil
	}
	return &SwBlockCleanupStatus{
		Status:                 cleanup.Status,
		KubernetesResidueCount: cleanup.KubernetesResidueCount,
		ISCSIResidueCount:      cleanup.ISCSIResidueCount,
		MultipathResidueCount:  cleanup.MultipathResidueCount,
		ProcessResidueCount:    cleanup.ProcessResidueCount,
		HostPathResidueCount:   cleanup.HostPathResidueCount,
		FailureCount:           cleanup.FailureCount,
		FailedPhase:            cleanup.FailedPhase,
		ReasonCodes:            append([]string(nil), cleanup.ReasonCodes...),
		EvidenceRef:            cleanup.EvidenceRef,
	}
}

func supportBundleRefsFromCluster(cluster ClusterEvidence) []string {
	seen := map[string]struct{}{}
	var refs []string
	add := func(ref string) {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return
		}
		if _, ok := seen[ref]; ok {
			return
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	if cluster.Cleanup != nil {
		add(cluster.Cleanup.EvidenceRef)
	}
	for _, volume := range cluster.Volumes {
		add(volume.SupportBundleHint)
		for _, condition := range volume.Conditions {
			for _, ref := range condition.EvidenceRefs {
				add(ref)
			}
		}
		for _, replica := range volume.Replicas {
			add(replica.SupportBundlePath)
			for _, condition := range replica.Conditions {
				for _, ref := range condition.EvidenceRefs {
					add(ref)
				}
			}
		}
	}
	for _, managed := range cluster.ManagedVolumes {
		for _, ref := range managed.EvidenceRefs {
			add(ref)
		}
		for _, condition := range managed.Conditions {
			for _, ref := range condition.EvidenceRefs {
				add(ref)
			}
		}
		for _, action := range managed.Actions {
			for _, ref := range action.EvidenceRefs {
				add(ref)
			}
		}
	}
	for _, node := range cluster.Nodes {
		for _, ref := range nodeEvidenceRefs(node) {
			add(ref)
		}
	}
	return refs
}

func safeNextStepsFromCluster(cluster ClusterEvidence, evidenceRefs []string) []SwBlockSafeNextStep {
	var steps []SwBlockSafeNextStep
	if shouldSuggestCollectBundle(cluster, evidenceRefs) {
		steps = append(steps, SwBlockSafeNextStep{
			Type:            ManagedVolumeActionCollectBundle,
			Mode:            ManagedVolumeActionModeReadOnly,
			Command:         `bash scripts/collect-helm-support-bundle.sh "$PWD"`,
			ReasonCode:      supportBundleReason(cluster),
			MutationAllowed: false,
			EvidenceRefs:    append([]string(nil), evidenceRefs...),
		})
	}
	if step := cleanupSafeNextStep(cluster.Cleanup); step != nil {
		steps = append(steps, *step)
	}
	return steps
}

func shouldSuggestCollectBundle(cluster ClusterEvidence, evidenceRefs []string) bool {
	if cluster.Status != ObservationStatusOK || cleanupRequired(cluster.Cleanup) {
		return true
	}
	if len(evidenceRefs) == 0 {
		return false
	}
	if cluster.Cleanup != nil && !cleanupRequired(cluster.Cleanup) && len(evidenceRefs) == 1 && evidenceRefs[0] == cluster.Cleanup.EvidenceRef {
		return false
	}
	return true
}

func supportBundleReason(cluster ClusterEvidence) string {
	if cluster.Cleanup != nil && cluster.Cleanup.Status != "" && cluster.Cleanup.Status != "ok" {
		if len(cluster.Cleanup.ReasonCodes) > 0 {
			return cluster.Cleanup.ReasonCodes[0]
		}
		return ConditionCleanupRequired
	}
	if cluster.Status != "" && cluster.Status != ObservationStatusOK {
		return cluster.Status
	}
	return "support_evidence_available"
}

type SwBlockVolumeCRDStatus struct {
	VolumeID       string                   `json:"volumeID,omitempty"`
	PVCName        string                   `json:"pvcName,omitempty"`
	Status         string                   `json:"status"`
	ReasonCode     string                   `json:"reasonCode,omitempty"`
	ObservedAt     time.Time                `json:"observedAt,omitempty"`
	Conditions     []ObservationCondition   `json:"conditions,omitempty"`
	NonClaims      []string                 `json:"nonClaims,omitempty"`
	EvidenceRefs   []string                 `json:"evidenceRefs,omitempty"`
	AllowedActions []SwBlockVolumeCRDAction `json:"allowedActions,omitempty"`
}

type SwBlockVolumeCRDAction struct {
	Type            string   `json:"type"`
	Mode            string   `json:"mode"`
	SideEffectClass string   `json:"sideEffectClass,omitempty"`
	OwnerExecutor   string   `json:"ownerExecutor,omitempty"`
	MutationAllowed bool     `json:"mutationAllowed"`
	Preconditions   []string `json:"preconditions,omitempty"`
	InvariantRefs   []string `json:"invariantRefs,omitempty"`
	EvidenceRefs    []string `json:"evidenceRefs,omitempty"`
}

type OperatorKubernetesEvent struct {
	InvolvedObject OperatorObjectRef `json:"involvedObject"`
	Type           string            `json:"type"`
	Reason         string            `json:"reason"`
	Message        string            `json:"message"`
	EvidenceRefs   []string          `json:"evidenceRefs,omitempty"`
	ObservedAt     time.Time         `json:"observedAt,omitempty"`
}

type OperatorStatusReconciler struct {
	Namespace   string
	ClusterName string
	Source      OperatorStatusSource
	Writer      OperatorStatusWriter
	EventSink   OperatorEventSink
	Now         func() time.Time
}

type OperatorStatusReconcileResult struct {
	ClusterRef OperatorObjectRef   `json:"clusterRef"`
	VolumeRefs []OperatorObjectRef `json:"volumeRefs"`
	EventCount int                 `json:"eventCount"`
}

func (r OperatorStatusReconciler) Reconcile(ctx context.Context) (OperatorStatusReconcileResult, error) {
	if r.Source == nil {
		return OperatorStatusReconcileResult{}, fmt.Errorf("operator status source is required")
	}
	if r.Writer == nil {
		return OperatorStatusReconcileResult{}, fmt.Errorf("operator status writer is required")
	}
	cluster, err := r.Source.ClusterEvidence(ctx)
	if err != nil {
		return OperatorStatusReconcileResult{}, err
	}
	cluster = NormalizeObservationCluster(cluster)
	snapshot := BuildOperatorFoundationSnapshot(cluster)
	observedAt := cluster.CapturedAt
	if observedAt.IsZero() {
		observedAt = r.now()()
	}

	namespace := defaultString(r.Namespace, "default")
	clusterName := defaultString(r.ClusterName, DefaultSwBlockClusterName)
	clusterRef := OperatorObjectRef{
		APIVersion: SwBlockVolumeAPIVersion,
		Kind:       SwBlockClusterKind,
		Namespace:  namespace,
		Name:       clusterName,
	}
	clusterStatus := SwBlockClusterCRDStatus{
		ObservedAt:         observedAt,
		NodeCount:          len(cluster.Nodes),
		Nodes:              swBlockNodeStatuses(cluster.Nodes),
		VolumeCount:        snapshot.Cluster.VolumeCount,
		ReadyVolumeCount:   snapshot.Cluster.ReadyVolumeCount,
		BlockedVolumeCount: snapshot.Cluster.BlockedVolumeCount,
		StaleVolumeCount:   snapshot.Cluster.StaleVolumeCount,
		Conditions:         append([]ObservationCondition(nil), snapshot.Cluster.Conditions...),
		MutationAllowed:    snapshot.Mutation.MutationAllowed,
		AllowedActionModes: append([]string(nil), snapshot.Mutation.AllowedModes...),
		NonClaims:          append([]string(nil), snapshot.Mutation.NonClaims...),
	}
	clusterStatus.SupportBundleRefs = supportBundleRefsFromCluster(cluster)
	clusterStatus.SafeNextSteps = safeNextStepsFromCluster(cluster, clusterStatus.SupportBundleRefs)
	if snapshot.Cluster.Cleanup != nil && snapshot.Cluster.Cleanup.EvidenceRef != "" {
		clusterStatus.EvidenceRefs = append(clusterStatus.EvidenceRefs, snapshot.Cluster.Cleanup.EvidenceRef)
	}
	clusterStatus.Cleanup = swBlockCleanupStatus(snapshot.Cluster.Cleanup)
	if err := r.Writer.WriteClusterStatus(ctx, clusterRef, clusterStatus); err != nil {
		return OperatorStatusReconcileResult{}, err
	}

	result := OperatorStatusReconcileResult{ClusterRef: clusterRef}
	for _, volume := range snapshot.Volumes {
		volumeRef := OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockVolumeKind,
			Namespace:  namespace,
			Name:       SwBlockVolumeObjectName(volume.Status),
		}
		volumeStatus := SwBlockVolumeCRDStatus{
			VolumeID:       volume.Status.VolumeID,
			PVCName:        volume.Status.PVCName,
			Status:         volume.Status.Status,
			ReasonCode:     volume.Status.ReasonCode,
			ObservedAt:     observedAt,
			Conditions:     append([]ObservationCondition(nil), volume.Status.Conditions...),
			NonClaims:      append([]string(nil), volume.Status.NonClaims...),
			EvidenceRefs:   append([]string(nil), volume.Status.EvidenceRefs...),
			AllowedActions: swBlockVolumeCRDActions(volume.AllowedActions),
		}
		if err := r.Writer.WriteVolumeStatus(ctx, volumeRef, volumeStatus); err != nil {
			return OperatorStatusReconcileResult{}, err
		}
		result.VolumeRefs = append(result.VolumeRefs, volumeRef)
		if r.EventSink == nil {
			continue
		}
		for _, event := range volume.Events {
			if err := r.EventSink.EmitEvent(ctx, OperatorKubernetesEvent{
				InvolvedObject: volumeRef,
				Type:           event.Type,
				Reason:         event.Reason,
				Message:        event.Message,
				EvidenceRefs:   append([]string(nil), event.EvidenceRefs...),
				ObservedAt:     observedAt,
			}); err == nil {
				result.EventCount++
			}
		}
	}
	return result, nil
}

func swBlockVolumeCRDActions(actions []ManagedVolumeOperatorAction) []SwBlockVolumeCRDAction {
	if len(actions) == 0 {
		return nil
	}
	out := make([]SwBlockVolumeCRDAction, 0, len(actions))
	for _, action := range actions {
		out = append(out, SwBlockVolumeCRDAction{
			Type:            action.Type,
			Mode:            action.Mode,
			SideEffectClass: action.SideEffectClass,
			OwnerExecutor:   action.OwnerExecutor,
			MutationAllowed: action.MutationAllowed,
			Preconditions:   append([]string(nil), action.Preconditions...),
			InvariantRefs:   append([]string(nil), action.InvariantRefs...),
			EvidenceRefs:    append([]string(nil), action.EvidenceRefs...),
		})
	}
	return out
}

func swBlockNodeStatuses(nodes []NodeEvidence) []SwBlockNodeCRDStatus {
	if len(nodes) == 0 {
		return nil
	}
	out := make([]SwBlockNodeCRDStatus, 0, len(nodes))
	for _, node := range nodes {
		status, reason := classifyNodeReadiness(node)
		out = append(out, SwBlockNodeCRDStatus{
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

func classifyNodeReadiness(node NodeEvidence) (string, string) {
	switch {
	case nodeHasConditionReason(node, ReasonCSIDriverNotRegistered):
		return ManagedVolumeStatusBlocked, ReasonCSIDriverNotRegistered
	case nodeHasConditionReason(node, ReasonCSINodePodNotReady):
		return ManagedVolumeStatusBlocked, ReasonCSINodePodNotReady
	case len(node.MissingImages) > 0:
		return ManagedVolumeStatusBlocked, ReasonImageMissingOnNode
	case !node.Ready:
		return ManagedVolumeStatusUnknown, ReasonNodeNotReady
	case !node.Schedulable:
		return ManagedVolumeStatusBlocked, ReasonNodeSchedulingDisabled
	default:
		return ManagedVolumeStatusReady, ReasonNodeReady
	}
}

func nodeHasConditionReason(node NodeEvidence, reason string) bool {
	for _, condition := range node.Conditions {
		if condition.Reason == reason {
			return true
		}
	}
	return false
}

func nodeReadinessConditions(node NodeEvidence, status, reason string) []ObservationCondition {
	conditions := append([]ObservationCondition(nil), node.Conditions...)
	switch status {
	case ManagedVolumeStatusReady:
		return ensureCondition(conditions, ObservationCondition{
			Type:     ConditionReady,
			Status:   "True",
			Reason:   reason,
			Severity: "info",
			Message:  "node is ready for Seaweed Block",
		})
	case ManagedVolumeStatusBlocked:
		conditions = ensureCondition(conditions, ObservationCondition{
			Type:     ConditionReady,
			Status:   "False",
			Reason:   reason,
			Severity: "warning",
			Message:  "node is blocked for Seaweed Block",
		})
		return ensureCondition(conditions, ObservationCondition{
			Type:     ConditionBlocked,
			Status:   "True",
			Reason:   reason,
			Severity: "warning",
			Message:  "node requires operator attention before scheduling Seaweed Block workloads",
		})
	default:
		conditions = ensureCondition(conditions, ObservationCondition{
			Type:     ConditionReady,
			Status:   "Unknown",
			Reason:   reason,
			Severity: "warning",
			Message:  "node readiness evidence is unavailable or not ready",
		})
		return ensureCondition(conditions, ObservationCondition{
			Type:     ConditionEvidenceStale,
			Status:   "True",
			Reason:   reason,
			Severity: "warning",
			Message:  "node readiness evidence is insufficient",
		})
	}
}

func ensureCondition(conditions []ObservationCondition, condition ObservationCondition) []ObservationCondition {
	if hasCondition(conditions, condition.Type, condition.Status) {
		return conditions
	}
	return append(conditions, condition)
}

func nodeEvidenceRefs(node NodeEvidence) []string {
	var refs []string
	for _, condition := range node.Conditions {
		refs = append(refs, condition.EvidenceRefs...)
	}
	return refs
}

func (r OperatorStatusReconciler) now() func() time.Time {
	if r.Now != nil {
		return r.Now
	}
	return time.Now
}

func SwBlockVolumeObjectName(status ManagedVolumeOperatorStatus) string {
	if status.PVCName != "" {
		return kubernetesName(status.PVCName)
	}
	if status.VolumeID != "" {
		return kubernetesName(status.VolumeID)
	}
	return "unknown-volume"
}

func kubernetesName(in string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(in) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '.':
			b.WriteRune('-')
		case unicode.IsSpace(r) || r == '_' || r == ':':
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "unknown-volume"
	}
	if len(out) > 63 {
		out = strings.TrimRight(out[:63], "-")
	}
	if out == "" {
		return "unknown-volume"
	}
	return out
}
