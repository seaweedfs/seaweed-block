package master

import (
	"context"
	"fmt"
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
		Protocol:          "nvme",
		FrontendTransport: "rdma",
		SourceSnapshotId:  "snap-abc",
		PvcName:           "demo-pvc",
		PvcNamespace:      "demo-ns",
		PvcUid:            "uid-123",
		PvName:            "pvc-a",
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.GetVolumeId() != "pvc-a" || resp.GetSizeBytes() != 1<<30 || resp.GetReplicationFactor() != 2 {
		t.Fatalf("response=%+v", resp)
	}
	if resp.GetPvcName() != "demo-pvc" || resp.GetPvcNamespace() != "demo-ns" || resp.GetPvcUid() != "uid-123" || resp.GetPvName() != "pvc-a" {
		t.Fatalf("kubernetes metadata response=%+v", resp)
	}
	if resp.GetProtocol() != "nvme" {
		t.Fatalf("response protocol=%q want nvme", resp.GetProtocol())
	}
	if resp.GetFrontendTransport() != "rdma" {
		t.Fatalf("response transport=%q want rdma", resp.GetFrontendTransport())
	}
	if resp.GetSourceSnapshotId() != "snap-abc" {
		t.Fatalf("response source snapshot=%q", resp.GetSourceSnapshotId())
	}
	rec, ok := h.Lifecycle().Volumes.GetVolume("pvc-a")
	if !ok {
		t.Fatal("desired volume not persisted")
	}
	if rec.Spec.VolumeID != "pvc-a" || rec.Spec.SizeBytes != 1<<30 || rec.Spec.ReplicationFactor != 2 {
		t.Fatalf("record=%+v", rec)
	}
	if rec.Spec.Protocol != "nvme" {
		t.Fatalf("record protocol=%q want nvme", rec.Spec.Protocol)
	}
	if rec.Spec.FrontendTransport != "rdma" {
		t.Fatalf("record transport=%q want rdma", rec.Spec.FrontendTransport)
	}
	if rec.Spec.SourceSnapshotID != "snap-abc" {
		t.Fatalf("record source snapshot=%q", rec.Spec.SourceSnapshotID)
	}
	if rec.Spec.PVCName != "demo-pvc" || rec.Spec.PVCNamespace != "demo-ns" || rec.Spec.PVCUID != "uid-123" || rec.Spec.PVName != "pvc-a" {
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

func TestPhase175LifecycleServiceHoldsPendingRestoreBeforePlacementDelete(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	const volumeID = "restored-a"
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId: volumeID, SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotId: "snap-abc",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID: volumeID, DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{
			VolumeID: volumeID, ServerID: "m02", PoolID: "default", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: volumeID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete pending restore error=%v", err)
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume(volumeID); !ok {
		t.Fatal("pending restore volume intent was deleted")
	}
	if _, ok := h.Lifecycle().Placements.GetPlacement(volumeID); !ok {
		t.Fatal("pending restore placement was deleted")
	}
	if _, err := h.Lifecycle().Volumes.MarkRestoreComplete(volumeID, "snap-abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("delete completed restore: %v", err)
	}
}

func TestPhase175ConcurrentRestoreCreateDeleteNeverReturnsHoldAfterPlacementLoss(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	for i := 0; i < 32; i++ {
		volumeID := fmt.Sprintf("restored-%d", i)
		if _, err := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{
			VolumeID: volumeID, DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{
				VolumeID: volumeID, ServerID: "m02", PoolID: "default", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica,
			}},
		}); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		createDone := make(chan error, 1)
		deleteDone := make(chan error, 1)
		go func() {
			<-start
			_, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
				VolumeId: volumeID, SizeBytes: 1 << 20, ReplicationFactor: 1, SourceSnapshotId: "snap-abc",
			})
			createDone <- err
		}()
		go func() {
			<-start
			_, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: volumeID})
			deleteDone <- err
		}()
		close(start)
		if err := <-createDone; err != nil {
			t.Fatal(err)
		}
		deleteErr := <-deleteDone
		switch status.Code(deleteErr) {
		case codes.OK:
		case codes.FailedPrecondition:
			if _, ok := h.Lifecycle().Placements.GetPlacement(volumeID); !ok {
				t.Fatalf("iteration %d returned restore hold after deleting placement", i)
			}
		default:
			t.Fatalf("iteration %d delete error=%v", i, deleteErr)
		}
		if rec, ok := h.Lifecycle().Volumes.GetVolume(volumeID); ok {
			if _, err := h.Lifecycle().Volumes.MarkRestoreComplete(volumeID, rec.Spec.SourceSnapshotID); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
				t.Fatal(err)
			}
		}
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
