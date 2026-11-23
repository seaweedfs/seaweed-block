package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

type workspaceTestView struct{}

func (workspaceTestView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}
}

type workspaceTestClient struct {
	renew func() (*control.WorkspaceLeaseFact, error)
	ack   func() error
}

func (c workspaceTestClient) RenewWorkspaceLease(context.Context, *control.WorkspaceIdentity, ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	return c.renew()
}
func (c workspaceTestClient) AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	return &control.WorkspaceEmpty{}, c.ack()
}

func TestWorkspaceLeaseExpiryFencesWithoutTimerAndCannotReopen(t *testing.T) {
	now := time.Unix(1000, 0)
	renewals, stops := 0, 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		renewals++
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second)}, nil
	}, ack: func() error { t.Fatal("expiry must not claim revocation ACK"); return nil }}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { stops++; return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if g.Projection().Healthy {
		t.Fatal("unleased frontend served")
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !g.Projection().Healthy {
		t.Fatal("valid lease not admitted")
	}
	now = now.Add(2 * time.Second)
	if g.Projection().Healthy {
		t.Fatal("expired lease served without timer")
	}
	if err := g.Refresh(context.Background()); err == nil {
		t.Fatal("expired guard reopened")
	}
	if renewals != 1 || stops != 1 {
		t.Fatal("fence did not stop without another renewal", renewals, stops)
	}
}

func TestWorkspaceDelayedRenewalCannotUndoExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	calls := 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		calls++
		if calls == 2 {
			now = now.Add(2 * time.Second)
		}
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second)}, nil
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err == nil {
		t.Fatal("late response reopened expired lease")
	}
	if g.Projection().Healthy {
		t.Fatal("late renewal served")
	}
}

func TestWorkspaceRevocationAckOnlyAfterFrontendAndCutsStop(t *testing.T) {
	// Keep the injected clock in the live context deadline domain.
	now := time.Now()
	revoking := false
	attempts, acks := 0, 0
	var g *WorkspaceLeaseGuard
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(30 * time.Second), Revoking: revoking}, nil
	},
		ack: func() error {
			if attempts != 2 || g.Projection().Healthy {
				t.Fatal("ACK before successful drain")
			}
			acks++
			return nil
		}}
	var err error
	g, err = NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) {
		if g.Projection().Healthy {
			t.Fatal("drain began before admission closed")
		}
		if cause := g.FenceCause(); cause != "revoked by the master" {
			t.Fatalf("drain cause %q; want revocation, not an expired RPC deadline", cause)
		}
		attempts++
		if attempts == 1 {
			return 0, errors.New("drain still failed")
		}
		return 7, nil
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !g.Held() || !g.Projection().Healthy || attempts != 0 || acks != 0 {
		t.Fatal("valid first grant must serve without a drain or ACK")
	}
	revoking = true
	if err := g.Refresh(context.Background()); err == nil {
		t.Fatal("failed stop accepted")
	}
	if acks != 0 {
		t.Fatal("failed stop acked")
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if acks != 1 {
		t.Fatal("successful stop not acked")
	}
}

func TestWorkspaceLeaseRunFinishesAfterSuccessfulRevocationAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second), Revoking: true}, nil
	},
		ack: func() error { cancel(); return nil }}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Run(ctx); err != nil {
		t.Fatalf("successful revocation ACK reported failure: %v", err)
	}
}
