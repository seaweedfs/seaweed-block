package master

import (
	"context"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestWorkspaceMutationRequestIDsSurviveMasterRestart(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	open := func() (*Host, control.WorkspaceServiceClient, control.LifecycleServiceClient) {
		h, e := New(cfg)
		if e != nil {
			t.Fatal(e)
		}
		h.Start()
		c, e := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Close(); closeTestMaster(t, h) })
		return h, control.NewWorkspaceServiceClient(c), control.NewLifecycleServiceClient(c)
	}
	h, w, l := open()
	for _, v := range []string{"v", "other"} {
		if _, e := l.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: v, SizeBytes: 1 << 20, ReplicationFactor: 1}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node", DataAddr: "127.0.0.1:9000", CtrlAddr: "127.0.0.1:9001"}); e != nil {
		t.Fatal(e)
	}
	b := &control.WorkspaceBinding{RequestId: "bind-1", VolumeId: "v", ServerId: "node", DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}
	original, e := w.RegisterWorkspaceBinding(ctx, b)
	if e != nil {
		t.Fatal(e)
	}
	p := &control.WorkspacePlacement{RequestId: "place-1", VolumeId: "v", ServerId: "node", ReplicaId: "r1"}
	placed, e := w.PlaceWorkspace(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	closeTestMaster(t, h)
	_, w, _ = open()
	got, e := w.RegisterWorkspaceBinding(ctx, b)
	if e != nil || !proto.Equal(got, original) {
		t.Fatalf("lost original binding after restart: %v %v", got, e)
	}
	again, e := w.PlaceWorkspace(ctx, p)
	if e != nil || !proto.Equal(again, placed) {
		t.Fatalf("lost original placement after restart: %v %v", again, e)
	}
	changed := proto.Clone(b).(*control.WorkspaceBinding)
	changed.VolumeId = "other"
	changed.DataAddr = "127.0.0.1:9202"
	changed.CtrlAddr = "127.0.0.1:9102"
	if _, e = w.RegisterWorkspaceBinding(ctx, changed); status.Code(e) != codes.AlreadyExists {
		t.Fatalf("same request id changed body accepted after restart: %v", e)
	}
	changedP := proto.Clone(p).(*control.WorkspacePlacement)
	changedP.ReplicaId = "r2"
	if _, e = w.PlaceWorkspace(ctx, changedP); status.Code(e) != codes.AlreadyExists {
		t.Fatalf("placement request id changed body: %v", e)
	}
	// IDs are shared by all mutations, not silently scoped to a method.
	if _, e = w.PlaceWorkspace(ctx, &control.WorkspacePlacement{RequestId: "bind-1", VolumeId: "v", ServerId: "node", ReplicaId: "r1"}); status.Code(e) != codes.AlreadyExists {
		t.Fatalf("cross-method request id reused: %v", e)
	}
}
