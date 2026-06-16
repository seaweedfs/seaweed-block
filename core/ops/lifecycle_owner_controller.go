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
}

type LifecycleOwnerReconciler struct {
	Namespace string
	Client    LifecycleOwnerClient
	EventSink OperatorEventSink
	DryRun    bool
	Now       func() time.Time
}

type LifecycleOwnerReconcileResult struct {
	VolumeCount         int `json:"volumeCount"`
	FinalizerPatchCount int `json:"finalizerPatchCount"`
	FinalizerAddedCount int `json:"finalizerAddedCount"`
	EventCount          int `json:"eventCount"`
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
		if volume.DeletionTimestamp != nil || lifecycleOwnerStringSliceContains(volume.Finalizers, SwBlockVolumeFinalizerName) {
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
