package ops

import (
	"context"
	"fmt"
	"time"
)

type LifecycleOwnerClient interface {
	ListSwBlockVolumes(ctx context.Context, namespace string) ([]SwBlockVolumeObject, error)
	PatchSwBlockVolumeFinalizers(ctx context.Context, ref OperatorObjectRef, finalizers []string) error
}

type SwBlockVolumeObject struct {
	Ref               OperatorObjectRef `json:"ref"`
	Finalizers        []string          `json:"finalizers,omitempty"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
	Spec              SwBlockVolumeSpec `json:"spec,omitempty"`
	Status            SwBlockVolumeCRDStatus
}

type SwBlockVolumeSpec struct {
	PVCName      string `json:"pvcName,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}

type SwBlockReplicaEligibilityObject struct {
	Ref    OperatorObjectRef                  `json:"ref"`
	Spec   SwBlockReplicaEligibilitySpec      `json:"spec,omitempty"`
	Status SwBlockReplicaEligibilityCRDStatus `json:"status,omitempty"`
}

type SwBlockReplicaEligibilitySpec struct {
	VolumeName string `json:"volumeName,omitempty"`
	VolumeID   string `json:"volumeID,omitempty"`
	PVCName    string `json:"pvcName,omitempty"`
	ReplicaID  string `json:"replicaID,omitempty"`
}

type SwBlockReplicaRebuildObject struct {
	Ref    OperatorObjectRef              `json:"ref"`
	Spec   SwBlockReplicaRebuildSpec      `json:"spec,omitempty"`
	Status SwBlockReplicaRebuildCRDStatus `json:"status,omitempty"`
}

type SwBlockReplicaRebuildSpec struct {
	VolumeName      string `json:"volumeName,omitempty"`
	VolumeID        string `json:"volumeID,omitempty"`
	PVCName         string `json:"pvcName,omitempty"`
	ReplicaID       string `json:"replicaID,omitempty"`
	SourceReplicaID string `json:"sourceReplicaID,omitempty"`
	RuntimeEndpoint string `json:"runtimeEndpoint,omitempty"`
	TargetDataAddr  string `json:"targetDataAddr,omitempty"`
	SessionID       uint64 `json:"sessionID,omitempty"`
	Epoch           uint64 `json:"epoch,omitempty"`
	EndpointVersion uint64 `json:"endpointVersion,omitempty"`
	FromLSN         uint64 `json:"fromLsn,omitempty"`
	FrontierHintLSN uint64 `json:"frontierHintLsn,omitempty"`
	BasePinLSN      uint64 `json:"basePinLsn,omitempty"`
}

type LifecycleOwnerReconciler struct {
	Namespace string
	Client    LifecycleOwnerClient
	EventSink OperatorEventSink
	DryRun    bool
	Now       func() time.Time
}

type LifecycleOwnerReconcileResult struct {
	VolumeCount            int `json:"volumeCount"`
	FinalizerPatchCount    int `json:"finalizerPatchCount"`
	FinalizerAddedCount    int `json:"finalizerAddedCount"`
	FinalizerHeldCount     int `json:"finalizerHeldCount"`
	FinalizerReleasedCount int `json:"finalizerReleasedCount"`
	EventCount             int `json:"eventCount"`
}

func (r LifecycleOwnerReconciler) Reconcile(ctx context.Context) (LifecycleOwnerReconcileResult, error) {
	if r.Client == nil {
		return LifecycleOwnerReconcileResult{}, fmt.Errorf("lifecycle owner client is required")
	}
	namespace := defaultString(r.Namespace, "default")
	volumes, err := r.Client.ListSwBlockVolumes(ctx, namespace)
	if err != nil {
		return LifecycleOwnerReconcileResult{}, err
	}
	result := LifecycleOwnerReconcileResult{VolumeCount: len(volumes)}
	for _, volume := range volumes {
		hasFinalizer := lifecycleOwnerStringSliceContains(volume.Finalizers, SwBlockVolumeFinalizerName)
		if volume.DeletionTimestamp != nil {
			if !hasFinalizer {
				continue
			}
			decision := volume.Status.DeleteSafety
			if lifecycleOwnerReleaseAllowed(decision) {
				next := lifecycleOwnerRemoveString(volume.Finalizers, SwBlockVolumeFinalizerName)
				if !r.DryRun {
					if err := r.Client.PatchSwBlockVolumeFinalizers(ctx, volume.Ref, next); err != nil {
						return LifecycleOwnerReconcileResult{}, err
					}
					result.FinalizerPatchCount++
				}
				result.FinalizerReleasedCount++
				if r.EventSink != nil && !r.DryRun {
					if err := r.EventSink.EmitEvent(ctx, OperatorKubernetesEvent{
						InvolvedObject: volume.Ref,
						Type:           "Normal",
						Reason:         ReasonDeleteFinalizerReleased,
						Message:        "Seaweed Block protection finalizer released after clean delete-safety evidence",
						EvidenceRefs:   append([]string(nil), decision.EvidenceRefs...),
						ObservedAt:     r.now()(),
					}); err == nil {
						result.EventCount++
					}
				}
				continue
			}
			result.FinalizerHeldCount++
			if r.EventSink != nil && !r.DryRun {
				holdReason, evidenceRefs := lifecycleOwnerHoldReason(decision)
				if err := r.EventSink.EmitEvent(ctx, OperatorKubernetesEvent{
					InvolvedObject: volume.Ref,
					Type:           "Warning",
					Reason:         holdReason,
					Message:        "Seaweed Block protection finalizer held until delete-safety evidence allows release",
					EvidenceRefs:   evidenceRefs,
					ObservedAt:     r.now()(),
				}); err == nil {
					result.EventCount++
				}
			}
			continue
		}
		if hasFinalizer {
			continue
		}
		next := append([]string(nil), volume.Finalizers...)
		next = append(next, SwBlockVolumeFinalizerName)
		if !r.DryRun {
			if err := r.Client.PatchSwBlockVolumeFinalizers(ctx, volume.Ref, next); err != nil {
				return LifecycleOwnerReconcileResult{}, err
			}
			result.FinalizerPatchCount++
		}
		result.FinalizerAddedCount++
		if r.EventSink == nil || r.DryRun {
			continue
		}
		if err := r.EventSink.EmitEvent(ctx, OperatorKubernetesEvent{
			InvolvedObject: volume.Ref,
			Type:           "Normal",
			Reason:         ReasonDeleteFinalizerAdded,
			Message:        "Seaweed Block protection finalizer added",
			ObservedAt:     r.now()(),
		}); err == nil {
			result.EventCount++
		}
	}
	return result, nil
}

func lifecycleOwnerReleaseAllowed(decision *SwBlockVolumeCRDDeleteSafety) bool {
	return decision != nil &&
		decision.FinalizerReleaseAllowed &&
		decision.Decision == ManagedVolumeActionDecisionAllowed &&
		decision.State == DeleteSafetyStateReleasable
}

func lifecycleOwnerHoldReason(decision *SwBlockVolumeCRDDeleteSafety) (string, []string) {
	if decision == nil || decision.Reason == "" {
		return ReasonCleanupEvidenceMissing, nil
	}
	return decision.Reason, append([]string(nil), decision.EvidenceRefs...)
}

func (r LifecycleOwnerReconciler) now() func() time.Time {
	if r.Now != nil {
		return r.Now
	}
	return time.Now
}

func lifecycleOwnerStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func lifecycleOwnerRemoveString(values []string, remove string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != remove {
			out = append(out, value)
		}
	}
	return out
}
