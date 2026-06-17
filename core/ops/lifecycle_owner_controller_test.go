package ops

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestLifecycleOwnerReconcilerAddsProtectionFinalizerOnly(t *testing.T) {
	client := &fakeLifecycleOwnerClient{volumes: []SwBlockVolumeObject{{
		Ref: OperatorObjectRef{
			APIVersion: SwBlockVolumeAPIVersion,
			Kind:       SwBlockVolumeKind,
			Namespace:  "kube-system",
			Name:       "demo",
		},
		Finalizers: []string{"example.com/foreign"},
	}}}
	events := &fakeOperatorEventSink{}
	result, err := (LifecycleOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
		EventSink: events,
		Now:       func() time.Time { return time.Date(2026, 6, 15, 1, 0, 0, 0, time.UTC) },
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 1 || result.FinalizerAddedCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	want := []string{"example.com/foreign", SwBlockVolumeFinalizerName}
	if !reflect.DeepEqual(client.patches[0].finalizers, want) {
		t.Fatalf("finalizers=%+v want %+v", client.patches[0].finalizers, want)
	}
	if events.countByReason(ReasonDeleteFinalizerAdded) != 1 {
		t.Fatalf("events=%+v", events.events)
	}
}

func TestLifecycleOwnerReconcilerIsIdempotent(t *testing.T) {
	client := &fakeLifecycleOwnerClient{volumes: []SwBlockVolumeObject{{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "demo"},
	}}}
	reconciler := LifecycleOwnerReconciler{Namespace: "kube-system", Client: client}
	if _, err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	client.volumes[0].Finalizers = append([]string(nil), client.patches[0].finalizers...)
	result, err := reconciler.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 || len(client.patches) != 1 {
		t.Fatalf("result=%+v patches=%+v", result, client.patches)
	}
}

func TestLifecycleOwnerReconcilerSkipsDeletingObjectsInAddSlice(t *testing.T) {
	deletingAt := time.Date(2026, 6, 15, 1, 0, 0, 0, time.UTC)
	client := &fakeLifecycleOwnerClient{volumes: []SwBlockVolumeObject{{
		Ref:               OperatorObjectRef{Namespace: "kube-system", Name: "deleting"},
		DeletionTimestamp: &deletingAt,
	}}}
	result, err := (LifecycleOwnerReconciler{Namespace: "kube-system", Client: client}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 || result.FinalizerAddedCount != 0 || len(client.patches) != 0 {
		t.Fatalf("result=%+v patches=%+v", result, client.patches)
	}
}

func TestLifecycleOwnerReconcilerHoldsDeletingObjectWithoutReleaseEvidence(t *testing.T) {
	deletingAt := time.Date(2026, 6, 15, 1, 0, 0, 0, time.UTC)
	client := &fakeLifecycleOwnerClient{volumes: []SwBlockVolumeObject{{
		Ref:               OperatorObjectRef{Namespace: "kube-system", Name: "deleting"},
		Finalizers:        []string{SwBlockVolumeFinalizerName},
		DeletionTimestamp: &deletingAt,
	}}}
	events := &fakeOperatorEventSink{}
	result, err := (LifecycleOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 || result.FinalizerHeldCount != 1 || len(client.patches) != 0 {
		t.Fatalf("result=%+v patches=%+v", result, client.patches)
	}
	if events.countByReason(ReasonCleanupEvidenceMissing) != 1 {
		t.Fatalf("events=%+v", events.events)
	}
}

func TestLifecycleOwnerReconcilerHoldsBlockedDeletingObject(t *testing.T) {
	deletingAt := time.Date(2026, 6, 15, 1, 0, 0, 0, time.UTC)
	client := &fakeLifecycleOwnerClient{volumes: []SwBlockVolumeObject{{
		Ref:               OperatorObjectRef{Namespace: "kube-system", Name: "blocked"},
		Finalizers:        []string{SwBlockVolumeFinalizerName},
		DeletionTimestamp: &deletingAt,
		Status: SwBlockVolumeCRDStatus{DeleteSafety: &SwBlockVolumeCRDDeleteSafety{
			Decision: ManagedVolumeActionDecisionRejected,
			State:    DeleteSafetyStateBlocked,
			Reason:   "iscsi_node_records_present",
		}},
	}}}
	result, err := (LifecycleOwnerReconciler{Namespace: "kube-system", Client: client}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 || result.FinalizerHeldCount != 1 || len(client.patches) != 0 {
		t.Fatalf("result=%+v patches=%+v", result, client.patches)
	}
}

func TestLifecycleOwnerReconcilerReleasesOnlyProtectionFinalizerWhenAllowed(t *testing.T) {
	deletingAt := time.Date(2026, 6, 15, 1, 0, 0, 0, time.UTC)
	client := &fakeLifecycleOwnerClient{volumes: []SwBlockVolumeObject{{
		Ref:               OperatorObjectRef{Namespace: "kube-system", Name: "clean"},
		Finalizers:        []string{"example.com/foreign", SwBlockVolumeFinalizerName},
		DeletionTimestamp: &deletingAt,
		Status: SwBlockVolumeCRDStatus{DeleteSafety: &SwBlockVolumeCRDDeleteSafety{
			Decision:                ManagedVolumeActionDecisionAllowed,
			State:                   DeleteSafetyStateReleasable,
			Reason:                  ReasonDeleteFinalizerReleasable,
			FinalizerReleaseAllowed: true,
			EvidenceRefs:            []string{"cleanup-summary.txt"},
		}},
	}}}
	events := &fakeOperatorEventSink{}
	result, err := (LifecycleOwnerReconciler{
		Namespace: "kube-system",
		Client:    client,
		EventSink: events,
	}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 1 || result.FinalizerReleasedCount != 1 || result.FinalizerHeldCount != 0 {
		t.Fatalf("result=%+v", result)
	}
	if want := []string{"example.com/foreign"}; !reflect.DeepEqual(client.patches[0].finalizers, want) {
		t.Fatalf("finalizers=%+v want %+v", client.patches[0].finalizers, want)
	}
	if events.countByReason(ReasonDeleteFinalizerReleased) != 1 {
		t.Fatalf("events=%+v", events.events)
	}
}

func TestLifecycleOwnerReconcilerDryRunDoesNotPatch(t *testing.T) {
	client := &fakeLifecycleOwnerClient{volumes: []SwBlockVolumeObject{{
		Ref: OperatorObjectRef{Namespace: "kube-system", Name: "demo"},
	}}}
	result, err := (LifecycleOwnerReconciler{Namespace: "kube-system", Client: client, DryRun: true}).Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizerPatchCount != 0 || result.FinalizerAddedCount != 1 || len(client.patches) != 0 {
		t.Fatalf("result=%+v patches=%+v", result, client.patches)
	}
}

type fakeLifecycleOwnerClient struct {
	volumes []SwBlockVolumeObject
	patches []fakeLifecycleOwnerPatch
}

type fakeLifecycleOwnerPatch struct {
	ref        OperatorObjectRef
	finalizers []string
}

func (f *fakeLifecycleOwnerClient) ListSwBlockVolumes(context.Context, string) ([]SwBlockVolumeObject, error) {
	return append([]SwBlockVolumeObject(nil), f.volumes...), nil
}

func (f *fakeLifecycleOwnerClient) PatchSwBlockVolumeFinalizers(_ context.Context, ref OperatorObjectRef, finalizers []string) error {
	f.patches = append(f.patches, fakeLifecycleOwnerPatch{
		ref:        ref,
		finalizers: append([]string(nil), finalizers...),
	})
	return nil
}
