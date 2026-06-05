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
		VolumeCount:        snapshot.Cluster.VolumeCount,
		ReadyVolumeCount:   snapshot.Cluster.ReadyVolumeCount,
		BlockedVolumeCount: snapshot.Cluster.BlockedVolumeCount,
		StaleVolumeCount:   snapshot.Cluster.StaleVolumeCount,
		Conditions:         append([]ObservationCondition(nil), snapshot.Cluster.Conditions...),
		MutationAllowed:    snapshot.Mutation.MutationAllowed,
		AllowedActionModes: append([]string(nil), snapshot.Mutation.AllowedModes...),
		NonClaims:          append([]string(nil), snapshot.Mutation.NonClaims...),
	}
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
