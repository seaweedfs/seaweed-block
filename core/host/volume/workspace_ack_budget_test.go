package volume

import (
	"context"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

// slowAckClient is a master that answers the revocation ACK after delay
// (its bounded HEAD read and write), honouring the call's deadline.
type slowAckClient struct {
	delay time.Duration
	acked chan struct{}
}

func (c slowAckClient) RenewWorkspaceLease(context.Context, *control.WorkspaceIdentity, ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(30 * time.Second), Revoking: true}, nil
}

func (c slowAckClient) AckWorkspaceRevoked(ctx context.Context, _ *control.WorkspaceRevocationAck, _ ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	select {
	case <-time.After(c.delay):
		close(c.acked)
		return &control.WorkspaceEmpty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A master that takes 2.5 s to answer the ACK (above its 2 s HEAD read
// alone, still inside its bounded steps) gets a completed ACK: the holder's
// ACK bound exceeds the master's.
func TestWorkspaceRevocationAckOutlastsTheMastersBoundedSteps(t *testing.T) {
	if WorkspaceAckTimeout <= 2*time.Second+500*time.Millisecond {
		t.Fatalf("WorkspaceAckTimeout %v does not exceed the master's 2 s HEAD read plus its write and reconcile", WorkspaceAckTimeout)
	}
	client := slowAckClient{delay: 2500 * time.Millisecond, acked: make(chan struct{})}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := g.Run(ctx); err != nil {
		t.Fatalf("a 2.5 s ACK answer failed the revocation: %v", err)
	}
	select {
	case <-client.acked:
	default:
		t.Fatal("the master's ACK answer was not awaited")
	}
}
