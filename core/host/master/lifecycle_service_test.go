package master

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestG15c_LifecycleService_CreateVolumePersistsDesiredIntent(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	source := createLifecycleSnapshot(t, h, "source-a")

	resp, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId:          "pvc-a",
		SizeBytes:         source.SizeBytes,
		ReplicationFactor: 2,
		Protocol:          "nvme",
		FrontendTransport: "rdma",
		SourceSnapshotId:  source.SnapshotID,
		PvcName:           "demo-pvc",
		PvcNamespace:      "demo-ns",
		PvcUid:            "uid-123",
		PvName:            "pvc-a",
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.GetVolumeId() != "pvc-a" || resp.GetSizeBytes() != source.SizeBytes || resp.GetReplicationFactor() != 2 {
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
	if resp.GetSourceSnapshotId() != source.SnapshotID {
		t.Fatalf("response source snapshot=%q", resp.GetSourceSnapshotId())
	}
	rec, ok := h.Lifecycle().Volumes.GetVolume("pvc-a")
	if !ok {
		t.Fatal("desired volume not persisted")
	}
	if rec.Spec.VolumeID != "pvc-a" || rec.Spec.SizeBytes != source.SizeBytes || rec.Spec.ReplicationFactor != 2 {
		t.Fatalf("record=%+v", rec)
	}
	if rec.Spec.Protocol != "nvme" {
		t.Fatalf("record protocol=%q want nvme", rec.Spec.Protocol)
	}
	if rec.Spec.FrontendTransport != "rdma" {
		t.Fatalf("record transport=%q want rdma", rec.Spec.FrontendTransport)
	}
	if rec.Spec.SourceSnapshotID != source.SnapshotID {
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
	source := createLifecycleSnapshot(t, h, "source-hold")
	const volumeID = "restored-a"
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId: volumeID, SizeBytes: source.SizeBytes, ReplicationFactor: 1, SourceSnapshotId: source.SnapshotID,
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
	if _, err := h.Lifecycle().Volumes.MarkRestoreComplete(volumeID, source.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("delete completed restore: %v", err)
	}
}

func TestPhase175RestoreIntentRequiresCurrentSnapshotButExistingRetryIsIndependent(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	request := &control.CreateVolumeRequest{
		VolumeId: "restored-a", SizeBytes: 8 * 4096, ReplicationFactor: 1, SourceSnapshotId: "missing",
	}
	if _, err := svc.CreateVolume(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unconfigured snapshot restore error=%v", err)
	}
	source := createLifecycleSnapshot(t, h, "source-validation")
	if _, err := svc.CreateVolume(context.Background(), request); status.Code(err) != codes.NotFound {
		t.Fatalf("missing source snapshot error=%v", err)
	}
	request.SourceSnapshotId = source.SnapshotID
	request.SizeBytes = source.SizeBytes + 4096
	if _, err := svc.CreateVolume(context.Background(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("source snapshot size mismatch error=%v", err)
	}
	request.SizeBytes = source.SizeBytes
	if _, err := svc.CreateVolume(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Lifecycle().Volumes.MarkRestoreComplete(request.VolumeId, source.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if err := h.snapshotCoordinator.Delete(source.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateVolume(context.Background(), request); err != nil {
		t.Fatalf("idempotent completed restore retry depends on deleted snapshot: %v", err)
	}
}

func TestPhase175ConcurrentRestoreCreateDeleteNeverReturnsHoldAfterPlacementLoss(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	source := createLifecycleSnapshot(t, h, "source-race")
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
				VolumeId: volumeID, SizeBytes: source.SizeBytes, ReplicationFactor: 1, SourceSnapshotId: source.SnapshotID,
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

func createLifecycleSnapshot(t *testing.T, h *Host, name string) snapshot.Record {
	t.Helper()
	manager, err := snapshot.OpenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authority := snapshot.SourceAuthority{
		VolumeID: "source-volume", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, RuntimeEndpoint: "https://snapshot.example:9443",
	}
	coordinator, err := snapshot.NewCoordinator(manager, fixedSnapshotResolver{authority: authority}, fixedSnapshotRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	h.snapshotCoordinator = coordinator
	record, err := coordinator.Create(context.Background(), snapshot.CreateRequest{Name: name, SourceVolumeID: authority.VolumeID})
	if err != nil {
		t.Fatal(err)
	}
	return record
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
