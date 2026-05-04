package master

import (
	"context"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestG15c_LifecycleService_CreateVolumePersistsDesiredIntent(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)

	resp, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId:          "pvc-a",
		SizeBytes:         1 << 30,
		ReplicationFactor: 2,
		PvcName:           "demo-pvc",
		PvcNamespace:      "demo-ns",
		PvName:            "pvc-a",
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.GetVolumeId() != "pvc-a" || resp.GetSizeBytes() != 1<<30 || resp.GetReplicationFactor() != 2 {
		t.Fatalf("response=%+v", resp)
	}
	if resp.GetPvcName() != "demo-pvc" || resp.GetPvcNamespace() != "demo-ns" || resp.GetPvName() != "pvc-a" {
		t.Fatalf("kubernetes metadata response=%+v", resp)
	}
	rec, ok := h.Lifecycle().Volumes.GetVolume("pvc-a")
	if !ok {
		t.Fatal("desired volume not persisted")
	}
	if rec.Spec.VolumeID != "pvc-a" || rec.Spec.SizeBytes != 1<<30 || rec.Spec.ReplicationFactor != 2 {
		t.Fatalf("record=%+v", rec)
	}
	if rec.Spec.PVCName != "demo-pvc" || rec.Spec.PVCNamespace != "demo-ns" || rec.Spec.PVName != "pvc-a" {
		t.Fatalf("kubernetes metadata record=%+v", rec)
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("pvc-a"); ok {
		t.Fatal("CreateVolume must not mint authority")
	}
}

func TestG15c_LifecycleService_CreateVolumeIdempotentAndConflictRejected(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	req := &control.CreateVolumeRequest{VolumeId: "pvc-a", SizeBytes: 1 << 20, ReplicationFactor: 1}
	if _, err := svc.CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}
	if _, err := svc.CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("idempotent CreateVolume: %v", err)
	}
	_, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId:          "pvc-a",
		SizeBytes:         2 << 20,
		ReplicationFactor: 1,
	})
	if err == nil {
		t.Fatal("expected conflict")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.AlreadyExists {
		t.Fatalf("code=%v want AlreadyExists", st.Code())
	}
}

func TestG15c_LifecycleService_DeleteVolumeRemovesDesiredIntent(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: "pvc-a"}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("pvc-a"); ok {
		t.Fatal("desired volume still present after delete")
	}
}

func TestG15e_LifecycleService_DeleteVolumeRemovesPlacementIntent(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	stores := h.Lifecycle()
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId:          "pvc-a",
		SizeBytes:         1 << 20,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "pvc-a",
		DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{
			VolumeID:  "pvc-a",
			ServerID:  "m02",
			PoolID:    "default",
			ReplicaID: "r1",
			Source:    lifecycle.PlacementSourceExistingReplica,
		}},
	}); err != nil {
		t.Fatalf("ApplyPlan: %v", err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: "pvc-a"}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if _, ok := stores.Volumes.GetVolume("pvc-a"); ok {
		t.Fatal("desired volume still present after delete")
	}
	if _, ok := stores.Placements.GetPlacement("pvc-a"); ok {
		t.Fatal("placement intent still present after delete")
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("pvc-a"); ok {
		t.Fatal("DeleteVolume must not mint or mutate authority")
	}
}

func TestG15c_LifecycleWireMessagesAreNotAuthorityShaped(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(control.CreateVolumeRequest{}),
		reflect.TypeOf(control.CreateVolumeResponse{}),
		reflect.TypeOf(control.DeleteVolumeRequest{}),
		reflect.TypeOf(control.DeleteVolumeResponse{}),
	} {
		for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy", "Primary"} {
			if _, ok := typ.FieldByName(forbidden); ok {
				t.Fatalf("%s must not carry %s", typ.Name(), forbidden)
			}
		}
	}
}
