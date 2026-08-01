package csi

import (
	"context"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeSnapshotServiceClient struct {
	contexts       []context.Context
	createRequest  *control.CreateSnapshotRequest
	restoreRequest *control.RestoreSnapshotRequest
	restoreReply   *control.RestoreSnapshotResponse
	getReply       *control.SnapshotRecord
}

func (f *fakeSnapshotServiceClient) CreateSnapshot(ctx context.Context, request *control.CreateSnapshotRequest, _ ...grpc.CallOption) (*control.SnapshotRecord, error) {
	f.contexts = append(f.contexts, ctx)
	f.createRequest = request
	return validWireSnapshot("snap-a", request.GetName(), request.GetSourceVolumeId()), nil
}

func (f *fakeSnapshotServiceClient) GetSnapshot(ctx context.Context, request *control.GetSnapshotRequest, _ ...grpc.CallOption) (*control.SnapshotRecord, error) {
	f.contexts = append(f.contexts, ctx)
	if f.getReply != nil {
		return f.getReply, nil
	}
	return validWireSnapshot(request.GetSnapshotId(), "daily", "vol-a"), nil
}

func (f *fakeSnapshotServiceClient) ListSnapshots(ctx context.Context, _ *control.ListSnapshotsRequest, _ ...grpc.CallOption) (*control.ListSnapshotsResponse, error) {
	f.contexts = append(f.contexts, ctx)
	return &control.ListSnapshotsResponse{Snapshots: []*control.SnapshotRecord{validWireSnapshot("snap-a", "daily", "vol-a")}}, nil
}

func (f *fakeSnapshotServiceClient) DeleteSnapshot(ctx context.Context, _ *control.DeleteSnapshotRequest, _ ...grpc.CallOption) (*control.DeleteSnapshotResponse, error) {
	f.contexts = append(f.contexts, ctx)
	return &control.DeleteSnapshotResponse{}, nil
}

func (f *fakeSnapshotServiceClient) RestoreSnapshot(ctx context.Context, request *control.RestoreSnapshotRequest, _ ...grpc.CallOption) (*control.RestoreSnapshotResponse, error) {
	f.contexts = append(f.contexts, ctx)
	f.restoreRequest = request
	if f.restoreReply != nil {
		return f.restoreReply, nil
	}
	return &control.RestoreSnapshotResponse{SnapshotId: request.GetSnapshotId(), TargetVolumeId: request.GetTargetVolumeId()}, nil
}

func (f *fakeSnapshotServiceClient) AbortSnapshotRestore(ctx context.Context, request *control.AbortSnapshotRestoreRequest, _ ...grpc.CallOption) (*control.AbortSnapshotRestoreResponse, error) {
	f.contexts = append(f.contexts, ctx)
	return &control.AbortSnapshotRestoreResponse{SnapshotId: request.GetSnapshotId(), TargetVolumeId: request.GetTargetVolumeId()}, nil
}

func validWireSnapshot(snapshotID, name, sourceVolumeID string) *control.SnapshotRecord {
	return &control.SnapshotRecord{
		SnapshotId: snapshotID, Name: name, SourceVolumeId: sourceVolumeID,
		CreatedAt: timestamppb.New(time.Unix(1_700_000_000, 0)), State: SnapshotStateReady, SizeBytes: 1 << 20,
	}
}

func TestPhase175ControlSnapshotProvisionerMapsRequestsAndAuthorizesEveryRPC(t *testing.T) {
	client := &fakeSnapshotServiceClient{}
	provisioner, err := NewControlSnapshotProvisioner(client, "secret-token")
	if err != nil {
		t.Fatal(err)
	}
	created, err := provisioner.CreateSnapshot(context.Background(), "daily", "vol-a")
	if err != nil || created.SnapshotID != "snap-a" || created.SizeBytes != 1<<20 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	if _, err := provisioner.GetSnapshot(context.Background(), "snap-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := provisioner.ListSnapshots(context.Background(), "vol-a"); err != nil {
		t.Fatal(err)
	}
	if err := provisioner.DeleteSnapshot(context.Background(), "snap-a"); err != nil {
		t.Fatal(err)
	}
	if err := provisioner.RestoreSnapshot(context.Background(), "snap-a", "restored-a"); err != nil {
		t.Fatal(err)
	}
	if client.createRequest.GetName() != "daily" || client.createRequest.GetSourceVolumeId() != "vol-a" {
		t.Fatalf("create request=%+v", client.createRequest)
	}
	if client.restoreRequest.GetSnapshotId() != "snap-a" || client.restoreRequest.GetTargetVolumeId() != "restored-a" {
		t.Fatalf("restore request=%+v", client.restoreRequest)
	}
	if len(client.contexts) != 5 {
		t.Fatalf("authorized contexts=%d", len(client.contexts))
	}
	for i, ctx := range client.contexts {
		outgoing, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			t.Fatalf("rpc %d has no outgoing metadata", i)
		}
		values := outgoing.Get("authorization")
		if len(values) != 1 || values[0] != "Bearer secret-token" {
			t.Fatalf("rpc %d authorization=%v", i, values)
		}
	}
}

func TestPhase175ControlSnapshotProvisionerRejectsRestoreIdentityMismatch(t *testing.T) {
	client := &fakeSnapshotServiceClient{restoreReply: &control.RestoreSnapshotResponse{SnapshotId: "other", TargetVolumeId: "restored-a"}}
	provisioner, err := NewControlSnapshotProvisioner(client, "secret-token")
	if err != nil {
		t.Fatal(err)
	}
	if err := provisioner.RestoreSnapshot(context.Background(), "snap-a", "restored-a"); err == nil {
		t.Fatal("expected restore response identity mismatch")
	}
}

func TestPhase175ControlSnapshotProvisionerRejectsGetIdentityMismatch(t *testing.T) {
	client := &fakeSnapshotServiceClient{getReply: validWireSnapshot("snap-b", "daily", "vol-a")}
	provisioner, err := NewControlSnapshotProvisioner(client, "secret-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provisioner.GetSnapshot(context.Background(), "snap-a"); err == nil {
		t.Fatal("expected get response identity mismatch")
	}
}
