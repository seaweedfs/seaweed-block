package master

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRestoreCreateVolumePersistsRequestBeforeTargetAndHealsRetry(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	svc := newServices(h)
	h.cfg.LayoutCreateVolume = func(context.Context, lifecycle.VolumeSpec) error { return nil }
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{VolumeId: "source", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	g := layout.Geometry{SizeBytes: 1 << 20, BlockSize: 4096, ChunkBytes: 4 << 20}
	h.cfg.LayoutReadVolume = func(_ context.Context, source lifecycle.VolumeSpec) (layout.Volume, error) {
		id, err := layout.ParseVolumeID(source.LayoutID)
		return layout.Volume{ID: id, Name: source.VolumeID, Geometry: g, Format: layout.VolumeFormat}, err
	}
	h.cfg.LayoutReadHead = func(_ context.Context, source lifecycle.VolumeSpec) (layout.Head, error) {
		id, _ := layout.ParseVolumeID(source.LayoutID)
		return layout.Head{VolumeID: id, State: layout.StateLive, Snapshots: map[string]layout.Snapshot{"daily": {}}}, nil
	}
	var targetID string
	var calls int
	h.cfg.LayoutCreateTarget = func(_ context.Context, source, target lifecycle.VolumeSpec, snapshot string) error {
		calls++
		rec, ok := h.Lifecycle().Volumes.GetVolume(target.VolumeID)
		if !ok || rec.StoreReady || rec.Spec != target || snapshot != "daily" || source.VolumeID != "source" {
			t.Fatalf("target called before durable request: %+v, source=%+v snapshot=%s", rec, source, snapshot)
		}
		if target.SourceLayoutID != source.LayoutID {
			t.Fatalf("target did not bind source layout id: %+v source=%+v", target, source)
		}
		if targetID == "" {
			targetID = target.LayoutID
		}
		if target.LayoutID != targetID || target.SizeBytes != g.SizeBytes || target.BlockSize != g.BlockSize || target.ChunkBytes != g.ChunkBytes {
			t.Fatalf("target replay changed id or source geometry: %+v", target)
		}
		if calls == 1 {
			return errors.New("interrupted after AddDependent")
		}
		return nil
	}
	req := &control.CreateVolumeRequest{VolumeId: "target", SourceVolume: "source", SourceSnapshot: "daily", CreateRequestId: "restore-1"}
	if _, err := svc.CreateVolume(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("interrupted restore: %v; want retryable unavailable", err)
	}
	if rec, _ := h.Lifecycle().Volumes.GetVolume("target"); rec.StoreReady {
		t.Fatal("interrupted restore became placeable")
	}
	resp, err := svc.CreateVolume(context.Background(), req)
	if err != nil || !resp.GetStoreReady() || resp.GetLayoutId() != targetID || calls != 2 {
		t.Fatalf("replay: %+v %v, calls=%d", resp, err, calls)
	}
	req.SourceSnapshot = "weekly"
	if _, err := svc.CreateVolume(context.Background(), req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("rebound request: %v; want conflict", err)
	}
	wrong := &control.CreateVolumeRequest{VolumeId: "wrong", SourceVolume: "source", SourceSnapshot: "missing", CreateRequestId: "restore-wrong"}
	if _, err := svc.CreateVolume(context.Background(), wrong); status.Code(err) != codes.NotFound {
		t.Fatalf("missing snapshot: %v; want not found", err)
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("wrong"); ok {
		t.Fatal("missing snapshot created a target record")
	}
	h.cfg.LayoutReadHead = func(_ context.Context, source lifecycle.VolumeSpec) (layout.Head, error) {
		id, _ := layout.ParseVolumeID(source.LayoutID)
		return layout.Head{VolumeID: id, State: layout.StateDeleting, Snapshots: map[string]layout.Snapshot{"daily": {}}}, nil
	}
	deleting := &control.CreateVolumeRequest{VolumeId: "from-deleting", SourceVolume: "source", SourceSnapshot: "daily", CreateRequestId: "restore-deleting"}
	if _, err := svc.CreateVolume(context.Background(), deleting); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("deleting source: %v; want refusal", err)
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("from-deleting"); ok {
		t.Fatal("deleting source created a target record")
	}
	unsafe := &control.CreateVolumeRequest{VolumeId: "net-target", SourceVolume: "source", SourceSnapshot: "daily", CreateRequestId: "restore-net", ReplicationFactor: 2}
	if _, err := svc.CreateVolume(context.Background(), unsafe); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RF2 restore: %v; want refusal", err)
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("net-target"); ok {
		t.Fatal("RF2 restore created a target record")
	}
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{VolumeId: "net-source", SizeBytes: 1 << 20, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	unsafe = &control.CreateVolumeRequest{VolumeId: "from-net", SourceVolume: "net-source", SourceSnapshot: "daily", CreateRequestId: "restore-from-net"}
	if _, err := svc.CreateVolume(context.Background(), unsafe); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RF3 source restore: %v; want refusal", err)
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("from-net"); ok {
		t.Fatal("RF3 source restore created a target record")
	}
}

func TestCreateVolumeRetriesFilerCreateBeforeStoreReady(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	failure := errors.New("filer unavailable")
	var calls []lifecycle.VolumeSpec
	h.cfg.LayoutCreateVolume = func(_ context.Context, spec lifecycle.VolumeSpec) error {
		calls = append(calls, spec)
		if len(calls) == 1 {
			return failure
		}
		return nil
	}
	svc := newServices(h)
	req := &control.CreateVolumeRequest{VolumeId: "v", SizeBytes: 1 << 20, ReplicationFactor: 1}
	if _, err := svc.CreateVolume(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("first create=%v, want retryable unavailable", err)
	}
	partial, ok := h.Lifecycle().Volumes.GetVolume("v")
	if !ok || partial.StoreReady {
		t.Fatalf("partial create advertised readiness: %+v", partial)
	}
	resp, err := svc.CreateVolume(context.Background(), req)
	if err != nil || !resp.GetStoreReady() {
		t.Fatalf("replay did not heal: response=%+v err=%v", resp, err)
	}
	if len(calls) != 2 || calls[0] != calls[1] || calls[0].LayoutID != partial.Spec.LayoutID {
		t.Fatalf("replay changed persisted identity: calls=%+v partial=%+v", calls, partial)
	}
	if _, err := svc.CreateVolume(context.Background(), req); err != nil || len(calls) != 2 {
		t.Fatalf("ready replay must not create an absent HEAD after service: calls=%d err=%v", len(calls), err)
	}
}

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
	if resp.GetBlockSize() != 4096 || resp.GetChunkBytes() != 4<<20 {
		t.Fatalf("response geometry=%+v", resp)
	}
	if resp.GetStoreReady() {
		t.Fatal("record without filer creation advertised store readiness")
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
	rec, ok := h.Lifecycle().Volumes.GetVolume("pvc-a")
	if !ok {
		t.Fatal("desired volume not persisted")
	}
	if rec.Spec.VolumeID != "pvc-a" || rec.Spec.SizeBytes != 1<<30 || rec.Spec.ReplicationFactor != 2 {
		t.Fatalf("record=%+v", rec)
	}
	if rec.Spec.BlockSize != 4096 || rec.Spec.ChunkBytes != 4<<20 {
		t.Fatalf("record geometry=%+v", rec.Spec)
	}
	if resp.GetLayoutId() == "" || resp.GetLayoutId() != rec.Spec.LayoutID {
		t.Fatal("create response does not carry the durable layout identity")
	}
	if rec.Spec.Protocol != "nvme" {
		t.Fatalf("record protocol=%q want nvme", rec.Spec.Protocol)
	}
	if rec.Spec.FrontendTransport != "rdma" {
		t.Fatalf("record transport=%q want rdma", rec.Spec.FrontendTransport)
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
