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

type OperatorSwBlockVolumeSource interface {
	ListSwBlockVolumes(ctx context.Context, namespace string) ([]SwBlockVolumeObject, error)
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
	InstallDrift       *SwBlockInstallDrift   `json:"installDrift,omitempty"`
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

type SwBlockInstallDrift struct {
	Status               string `json:"status"`
	ReasonCode           string `json:"reasonCode,omitempty"`
	ChartName            string `json:"chartName,omitempty"`
	CurrentChartVersion  string `json:"currentChartVersion,omitempty"`
	DesiredChartVersion  string `json:"desiredChartVersion,omitempty"`
	CurrentAppVersion    string `json:"currentAppVersion,omitempty"`
	DesiredAppVersion    string `json:"desiredAppVersion,omitempty"`
	CurrentImage         string `json:"currentImage,omitempty"`
	DesiredImage         string `json:"desiredImage,omitempty"`
	CurrentCSIImage      string `json:"currentCsiImage,omitempty"`
	DesiredCSIImage      string `json:"desiredCsiImage,omitempty"`
	CurrentOperatorImage string `json:"currentOperatorImage,omitempty"`
	DesiredOperatorImage string `json:"desiredOperatorImage,omitempty"`
	EvidenceRef          string `json:"evidenceRef,omitempty"`
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

func swBlockInstallDriftStatus(drift *InstallDriftEvidence) *SwBlockInstallDrift {
	if drift == nil {
		return nil
	}
	return &SwBlockInstallDrift{
		Status:               drift.Status,
		ReasonCode:           drift.ReasonCode,
		ChartName:            drift.ChartName,
		CurrentChartVersion:  drift.CurrentChartVersion,
		DesiredChartVersion:  drift.DesiredChartVersion,
		CurrentAppVersion:    drift.CurrentAppVersion,
		DesiredAppVersion:    drift.DesiredAppVersion,
		CurrentImage:         drift.CurrentImage,
		DesiredImage:         drift.DesiredImage,
		CurrentCSIImage:      drift.CurrentCSIImage,
		DesiredCSIImage:      drift.DesiredCSIImage,
		CurrentOperatorImage: drift.CurrentOperatorImage,
		DesiredOperatorImage: drift.DesiredOperatorImage,
		EvidenceRef:          drift.EvidenceRef,
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
	if cluster.InstallDrift != nil {
		add(cluster.InstallDrift.EvidenceRef)
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
	for _, node := range cluster.Nodes {
		status, reason := classifyNodeReadiness(node)
		if status != ManagedVolumeStatusReady && reason != "" {
			return reason
		}
	}
	return "support_evidence_available"
}

type SwBlockVolumeCRDStatus struct {
	VolumeID              string                              `json:"volumeID,omitempty"`
	PVCName               string                              `json:"pvcName,omitempty"`
	Status                string                              `json:"status"`
	ReasonCode            string                              `json:"reasonCode,omitempty"`
	ObservedAt            time.Time                           `json:"observedAt,omitempty"`
	Conditions            []ObservationCondition              `json:"conditions,omitempty"`
	DeleteSafety          *SwBlockVolumeCRDDeleteSafety       `json:"deleteSafety"`
	ReplicaReintegrations []SwBlockVolumeCRDReturnedReplica   `json:"replicaReintegrations,omitempty"`
	ExecutorPreflights    []SwBlockVolumeCRDExecutorPreflight `json:"executorPreflights,omitempty"`
	ExecutorContracts     []SwBlockVolumeCRDExecutorContract  `json:"executorContracts,omitempty"`
	NonClaims             []string                            `json:"nonClaims,omitempty"`
	EvidenceRefs          []string                            `json:"evidenceRefs,omitempty"`
	AllowedActions        []SwBlockVolumeCRDAction            `json:"allowedActions,omitempty"`
}

type SwBlockVolumeCRDReturnedReplica struct {
	ReplicaID             string   `json:"replicaID"`
	State                 string   `json:"state"`
	ReasonCode            string   `json:"reasonCode,omitempty"`
	FrontendFenced        bool     `json:"frontendFenced"`
	FrontendPrimaryReady  bool     `json:"frontendPrimaryReady"`
	AckEligibilityKnown   bool     `json:"ackEligibilityKnown"`
	AckEligible           bool     `json:"ackEligible"`
	DurableFrontierKnown  bool     `json:"durableFrontierKnown"`
	DurableFrontierLSN    uint64   `json:"durableFrontierLsn,omitempty"`
	RequiredFrontierKnown bool     `json:"requiredFrontierKnown,omitempty"`
	RequiredFrontierLSN   uint64   `json:"requiredFrontierLsn,omitempty"`
	RuntimeEndpoint       string   `json:"runtimeEndpoint,omitempty"`
	TargetDataAddr        string   `json:"targetDataAddr,omitempty"`
	SessionID             uint64   `json:"sessionID,omitempty"`
	Epoch                 uint64   `json:"epoch,omitempty"`
	EndpointVersion       uint64   `json:"endpointVersion,omitempty"`
	FromLSN               uint64   `json:"fromLsn,omitempty"`
	FrontierHintLSN       uint64   `json:"frontierHintLsn,omitempty"`
	BasePinLSN            uint64   `json:"basePinLsn,omitempty"`
	EvidenceRefs          []string `json:"evidenceRefs,omitempty"`
}

type SwBlockVolumeCRDExecutorPreflight struct {
	ActionType             string   `json:"actionType"`
	ReplicaID              string   `json:"replicaID,omitempty"`
	Decision               string   `json:"decision"`
	Reason                 string   `json:"reason"`
	Mode                   string   `json:"mode"`
	SideEffectClass        string   `json:"sideEffectClass"`
	OwnerExecutor          string   `json:"ownerExecutor"`
	MutationAllowed        bool     `json:"mutationAllowed"`
	FrontendFenced         bool     `json:"frontendFenced"`
	AckEligibilityKnown    bool     `json:"ackEligibilityKnown"`
	AckEligible            bool     `json:"ackEligible"`
	DurableFrontierKnown   bool     `json:"durableFrontierKnown"`
	DurableFrontierLSN     uint64   `json:"durableFrontierLsn,omitempty"`
	RequiredFrontierKnown  bool     `json:"requiredFrontierKnown"`
	RequiredFrontierLSN    uint64   `json:"requiredFrontierLsn,omitempty"`
	EvidenceRequired       string   `json:"evidenceRequired,omitempty"`
	EvidenceRefs           []string `json:"evidenceRefs,omitempty"`
	ForbiddenMutationClass []string `json:"forbiddenMutationClass,omitempty"`
}

type SwBlockVolumeCRDExecutorContract struct {
	ActionType               string   `json:"actionType"`
	ReplicaID                string   `json:"replicaID,omitempty"`
	Decision                 string   `json:"decision"`
	Reason                   string   `json:"reason"`
	OwnerExecutor            string   `json:"ownerExecutor"`
	ExecutionEnabled         bool     `json:"executionEnabled"`
	MutationAllowed          bool     `json:"mutationAllowed"`
	PreflightDecision        string   `json:"preflightDecision,omitempty"`
	PreflightReason          string   `json:"preflightReason,omitempty"`
	AllowedMutationClass     []string `json:"allowedMutationClass,omitempty"`
	ForbiddenMutationClass   []string `json:"forbiddenMutationClass,omitempty"`
	TerminalEvidenceRequired []string `json:"terminalEvidenceRequired,omitempty"`
	EvidenceRefs             []string `json:"evidenceRefs,omitempty"`
}

type SwBlockVolumeCRDDeleteSafety struct {
	ActionType              string   `json:"actionType,omitempty"`
	Decision                string   `json:"decision,omitempty"`
	State                   string   `json:"state,omitempty"`
	Reason                  string   `json:"reason,omitempty"`
	FinalizerReleaseAllowed bool     `json:"finalizerReleaseAllowed"`
	MissingFacts            []string `json:"missingFacts,omitempty"`
	EvidenceRefs            []string `json:"evidenceRefs,omitempty"`
	SafeNextAction          string   `json:"safeNextAction,omitempty"`
}

type SwBlockVolumeCRDAction struct {
	Type             string   `json:"type"`
	Mode             string   `json:"mode"`
	SideEffectClass  string   `json:"sideEffectClass,omitempty"`
	OwnerExecutor    string   `json:"ownerExecutor,omitempty"`
	Decision         string   `json:"decision,omitempty"`
	DecisionReason   string   `json:"decisionReason,omitempty"`
	MissingFacts     []string `json:"missingFacts,omitempty"`
	MutationAllowed  bool     `json:"mutationAllowed"`
	Preconditions    []string `json:"preconditions,omitempty"`
	InvariantRefs    []string `json:"invariantRefs,omitempty"`
	EvidenceRequired string   `json:"evidenceRequired,omitempty"`
	EvidenceRefs     []string `json:"evidenceRefs,omitempty"`
}

type SwBlockReplicaEligibilityCRDStatus struct {
	ObservedAt                         time.Time              `json:"observedAt,omitempty"`
	ObservedGeneration                 int64                  `json:"observedGeneration,omitempty"`
	Executor                           string                 `json:"executor,omitempty"`
	ReasonCode                         string                 `json:"reasonCode,omitempty"`
	AckEligibilityKnown                bool                   `json:"ackEligibilityKnown"`
	AckEligible                        bool                   `json:"ackEligible"`
	FrontendFencedAfterExecution       bool                   `json:"frontendFencedAfterExecution"`
	PrimaryUnchanged                   bool                   `json:"primaryUnchanged"`
	DurableFrontierCovered             bool                   `json:"durableFrontierCovered"`
	NoCrossVolumeIdentityChange        bool                   `json:"noCrossVolumeIdentityChange"`
	FrontendPublicationDecision        string                 `json:"frontendPublicationDecision,omitempty"`
	FrontendPublicationReason          string                 `json:"frontendPublicationReason,omitempty"`
	FrontendPublicationMutationAllowed bool                   `json:"frontendPublicationMutationAllowed"`
	EvidenceGeneration                 string                 `json:"evidenceGeneration,omitempty"`
	Conditions                         []ObservationCondition `json:"conditions,omitempty"`
	EvidenceRefs                       []string               `json:"evidenceRefs,omitempty"`
	NonClaims                          []string               `json:"nonClaims,omitempty"`
}

type SwBlockReplicaRebuildCRDStatus struct {
	ObservedAt                  time.Time              `json:"observedAt,omitempty"`
	ObservedGeneration          int64                  `json:"observedGeneration,omitempty"`
	Executor                    string                 `json:"executor,omitempty"`
	State                       string                 `json:"state,omitempty"`
	ReasonCode                  string                 `json:"reasonCode,omitempty"`
	FrontendFencedBeforeRebuild bool                   `json:"frontendFencedBeforeRebuild"`
	PrimaryUnchanged            bool                   `json:"primaryUnchanged"`
	DurableFrontierKnown        bool                   `json:"durableFrontierKnown"`
	DurableFrontierLSN          uint64                 `json:"durableFrontierLsn,omitempty"`
	RequiredFrontierKnown       bool                   `json:"requiredFrontierKnown"`
	RequiredFrontierLSN         uint64                 `json:"requiredFrontierLsn,omitempty"`
	DurableFrontierCaughtUp     bool                   `json:"durableFrontierCaughtUp"`
	RebuildTrafficStarted       bool                   `json:"rebuildTrafficStarted"`
	PublicationDecision         string                 `json:"publicationDecision,omitempty"`
	PublicationReason           string                 `json:"publicationReason,omitempty"`
	PublicationMutationAllowed  bool                   `json:"publicationMutationAllowed"`
	NoFrontendPublication       bool                   `json:"noFrontendPublication"`
	NoCrossVolumeIdentityChange bool                   `json:"noCrossVolumeIdentityChange"`
	EvidenceGeneration          string                 `json:"evidenceGeneration,omitempty"`
	Conditions                  []ObservationCondition `json:"conditions,omitempty"`
	EvidenceRefs                []string               `json:"evidenceRefs,omitempty"`
	NonClaims                   []string               `json:"nonClaims,omitempty"`
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
	Volumes     OperatorSwBlockVolumeSource
	EventSink   OperatorEventSink
	Now         func() time.Time
}

type OperatorStatusReconcileResult struct {
	ClusterRef          OperatorObjectRef   `json:"clusterRef"`
	VolumeRefs          []OperatorObjectRef `json:"volumeRefs"`
	EventCount          int                 `json:"eventCount"`
	FinalizerPatchCount int                 `json:"finalizerPatchCount"`
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
	namespace := defaultString(r.Namespace, "default")
	if r.Volumes != nil {
		volumes, err := r.Volumes.ListSwBlockVolumes(ctx, namespace)
		if err != nil {
			return OperatorStatusReconcileResult{}, err
		}
		cluster = ProjectSwBlockVolumeDeleteSafety(cluster, volumes)
	}
	snapshot := BuildOperatorFoundationSnapshot(cluster)
	observedAt := cluster.CapturedAt
	if observedAt.IsZero() {
		observedAt = r.now()()
	}

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
	clusterStatus.InstallDrift = swBlockInstallDriftStatus(snapshot.Cluster.InstallDrift)
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
			VolumeID:              volume.Status.VolumeID,
			PVCName:               volume.Status.PVCName,
			Status:                volume.Status.Status,
			ReasonCode:            volume.Status.ReasonCode,
			ObservedAt:            observedAt,
			Conditions:            append([]ObservationCondition(nil), volume.Status.Conditions...),
			DeleteSafety:          swBlockVolumeCRDDeleteSafety(volume.Status.DeleteSafety),
			ReplicaReintegrations: swBlockVolumeCRDReturnedReplicas(volume.Status.ReplicaReintegrations),
			ExecutorPreflights:    swBlockVolumeCRDExecutorPreflights(volume.Status.ExecutorPreflights),
			ExecutorContracts:     swBlockVolumeCRDExecutorContracts(volume.Status.ExecutorContracts),
			NonClaims:             append([]string(nil), volume.Status.NonClaims...),
			EvidenceRefs:          append([]string(nil), volume.Status.EvidenceRefs...),
			AllowedActions:        swBlockVolumeCRDActions(volume.AllowedActions),
		}
		if err := r.Writer.WriteVolumeStatus(ctx, volumeRef, volumeStatus); err != nil {
			if IsKubernetesStatusNotFound(err) {
				continue
			}
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

func ProjectSwBlockVolumeDeleteSafety(cluster ClusterEvidence, volumes []SwBlockVolumeObject) ClusterEvidence {
	cluster = NormalizeObservationCluster(cluster)
	for _, volume := range volumes {
		if volume.DeletionTimestamp == nil {
			continue
		}
		volumeID := volume.Status.VolumeID
		pvcName := firstNonEmpty(volume.Status.PVCName, volume.Spec.PVCName, volume.Ref.Name)
		applyDeleteSafetyFacts(&cluster, deleteSafetyProjectionFacts{
			VolumeID:         volumeID,
			PVCName:          pvcName,
			DeleteRequested:  true,
			FinalizerPresent: lifecycleOwnerStringSliceContains(volume.Finalizers, SwBlockVolumeFinalizerName),
			EvidencePath:     "swblockvolume/" + volume.Ref.Namespace + "/" + volume.Ref.Name,
		})
	}
	return NormalizeObservationCluster(cluster)
}

func swBlockVolumeCRDReturnedReplicas(returned []ReturnedReplicaProjection) []SwBlockVolumeCRDReturnedReplica {
	if len(returned) == 0 {
		return nil
	}
	out := make([]SwBlockVolumeCRDReturnedReplica, 0, len(returned))
	for _, replica := range returned {
		out = append(out, SwBlockVolumeCRDReturnedReplica{
			ReplicaID:             replica.ReplicaID,
			State:                 replica.State,
			ReasonCode:            replica.ReasonCode,
			FrontendFenced:        replica.FrontendFenced,
			FrontendPrimaryReady:  replica.FrontendPrimaryReady,
			AckEligibilityKnown:   replica.AckEligibilityKnown,
			AckEligible:           replica.AckEligible,
			DurableFrontierKnown:  replica.DurableFrontierKnown,
			DurableFrontierLSN:    replica.DurableFrontierLSN,
			RequiredFrontierKnown: replica.RequiredFrontierKnown,
			RequiredFrontierLSN:   replica.RequiredFrontierLSN,
			EvidenceRefs:          append([]string(nil), replica.EvidenceRefs...),
		})
	}
	return out
}

func swBlockVolumeCRDExecutorPreflights(preflights []ReturnedReplicaExecutorPreflight) []SwBlockVolumeCRDExecutorPreflight {
	if len(preflights) == 0 {
		return nil
	}
	out := make([]SwBlockVolumeCRDExecutorPreflight, 0, len(preflights))
	for _, preflight := range preflights {
		out = append(out, SwBlockVolumeCRDExecutorPreflight{
			ActionType:             preflight.ActionType,
			ReplicaID:              preflight.ReplicaID,
			Decision:               preflight.Decision,
			Reason:                 preflight.Reason,
			Mode:                   preflight.Mode,
			SideEffectClass:        preflight.SideEffectClass,
			OwnerExecutor:          preflight.OwnerExecutor,
			MutationAllowed:        preflight.MutationAllowed,
			FrontendFenced:         preflight.FrontendFenced,
			AckEligibilityKnown:    preflight.AckEligibilityKnown,
			AckEligible:            preflight.AckEligible,
			DurableFrontierKnown:   preflight.DurableFrontierKnown,
			DurableFrontierLSN:     preflight.DurableFrontierLSN,
			RequiredFrontierKnown:  preflight.RequiredFrontierKnown,
			RequiredFrontierLSN:    preflight.RequiredFrontierLSN,
			EvidenceRequired:       preflight.EvidenceRequired,
			EvidenceRefs:           append([]string(nil), preflight.EvidenceRefs...),
			ForbiddenMutationClass: append([]string(nil), preflight.ForbiddenMutationClass...),
		})
	}
	return out
}

func swBlockVolumeCRDExecutorContracts(contracts []ReturnedReplicaExecutorContract) []SwBlockVolumeCRDExecutorContract {
	if len(contracts) == 0 {
		return nil
	}
	out := make([]SwBlockVolumeCRDExecutorContract, 0, len(contracts))
	for _, contract := range contracts {
		out = append(out, SwBlockVolumeCRDExecutorContract{
			ActionType:               contract.ActionType,
			ReplicaID:                contract.ReplicaID,
			Decision:                 contract.Decision,
			Reason:                   contract.Reason,
			OwnerExecutor:            contract.OwnerExecutor,
			ExecutionEnabled:         contract.ExecutionEnabled,
			MutationAllowed:          contract.MutationAllowed,
			PreflightDecision:        contract.PreflightDecision,
			PreflightReason:          contract.PreflightReason,
			AllowedMutationClass:     append([]string(nil), contract.AllowedMutationClass...),
			ForbiddenMutationClass:   append([]string(nil), contract.ForbiddenMutationClass...),
			TerminalEvidenceRequired: append([]string(nil), contract.TerminalEvidenceRequired...),
			EvidenceRefs:             append([]string(nil), contract.EvidenceRefs...),
		})
	}
	return out
}

func swBlockVolumeCRDDeleteSafety(decision *SwBlockVolumeDeleteSafetyDecision) *SwBlockVolumeCRDDeleteSafety {
	if decision == nil {
		return nil
	}
	return &SwBlockVolumeCRDDeleteSafety{
		ActionType:              decision.ActionType,
		Decision:                decision.Decision,
		State:                   decision.State,
		Reason:                  decision.Reason,
		FinalizerReleaseAllowed: decision.FinalizerReleaseAllowed,
		MissingFacts:            append([]string(nil), decision.MissingFacts...),
		EvidenceRefs:            append([]string(nil), decision.EvidenceRefs...),
		SafeNextAction:          decision.SafeNextAction,
	}
}

func swBlockVolumeCRDActions(actions []ManagedVolumeOperatorAction) []SwBlockVolumeCRDAction {
	if len(actions) == 0 {
		return nil
	}
	out := make([]SwBlockVolumeCRDAction, 0, len(actions))
	for _, action := range actions {
		out = append(out, SwBlockVolumeCRDAction{
			Type:             action.Type,
			Mode:             action.Mode,
			SideEffectClass:  action.SideEffectClass,
			OwnerExecutor:    action.OwnerExecutor,
			Decision:         action.Decision,
			DecisionReason:   action.DecisionReason,
			MissingFacts:     append([]string(nil), action.MissingFacts...),
			MutationAllowed:  action.MutationAllowed,
			Preconditions:    append([]string(nil), action.Preconditions...),
			InvariantRefs:    append([]string(nil), action.InvariantRefs...),
			EvidenceRequired: action.EvidenceRequired,
			EvidenceRefs:     append([]string(nil), action.EvidenceRefs...),
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
	case !node.Ready:
		return ManagedVolumeStatusUnknown, ReasonNodeNotReady
	case !node.Schedulable:
		return ManagedVolumeStatusBlocked, ReasonNodeSchedulingDisabled
	case len(node.MissingImages) > 0:
		return ManagedVolumeStatusBlocked, ReasonImageMissingOnNode
	case nodeHasConditionReason(node, ReasonCSIDriverNotRegistered):
		return ManagedVolumeStatusBlocked, ReasonCSIDriverNotRegistered
	case nodeHasConditionReason(node, ReasonCSINodePodNotReady):
		return ManagedVolumeStatusBlocked, ReasonCSINodePodNotReady
	case nodeHasConditionReason(node, ReasonISCSIPrereqMissing):
		return ManagedVolumeStatusBlocked, ReasonISCSIPrereqMissing
	case nodeHasConditionReason(node, ReasonMultipathPrereqMissing):
		return ManagedVolumeStatusBlocked, ReasonMultipathPrereqMissing
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
	conditions := removeConditionType(node.Conditions, ConditionReady)
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

func removeConditionType(conditions []ObservationCondition, typ string) []ObservationCondition {
	if len(conditions) == 0 {
		return nil
	}
	out := make([]ObservationCondition, 0, len(conditions))
	for _, condition := range conditions {
		if condition.Type == typ {
			continue
		}
		out = append(out, condition)
	}
	return out
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
