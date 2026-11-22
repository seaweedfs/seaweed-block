package volume

import (
	"context"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"testing"
)

func TestWorkspaceLeaseGuardCarriesHolderServerIdentity(t *testing.T) {
	seen := ""
	client := funcLeaseClient(func(_ context.Context, id *control.WorkspaceIdentity, _ ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
		seen = id.ServerId
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: 1000000000}, nil
	})
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1, ServerId: "node-a"}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if seen != "node-a" {
		t.Fatalf("master saw holder server %q, want node-a", seen)
	}
}
