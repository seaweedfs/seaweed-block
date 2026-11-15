package volume

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestWorkspaceFenceBeforeHeldInstallIsTerminal(t *testing.T) {
	wire := &runtimeLeaseServer{ttl: time.Second}
	h := workspaceRuntimeHost(t, runtimeWire(t, wire), true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	arrived, release := make(chan struct{}), make(chan struct{})
	var stops atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- h.runWorkspaceLease(ctx, func(context.Context, *control.WorkspaceIdentity) (uint64, error) {
			stops.Add(1)
			return 7, nil
		}, func() { close(arrived); <-release })
	}()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("installer did not reach gate")
	}
	h.FenceWorkspaceLease()
	close(release)
	// A terminal fence must settle the real runtime without any RPC grant.
	select {
	case <-done:
	case <-wire.first:
		cancel()
		_ = runtimeDone(t, done)
		t.Fatal("installer renewed after synchronous host fence")
	case <-time.After(5 * time.Second):
		cancel()
		_ = runtimeDone(t, done)
		t.Fatal("fenced installer did not settle")
	}
	if stops.Load() != 1 || h.ProjectionView().Projection().Healthy {
		t.Fatal("fence failed to stop installed guard", stops.Load())
	}
}

func TestWorkspaceCanceledReplyCannotPublishGrant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	arrived, reply := make(chan struct{}), make(chan struct{})
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		close(arrived)
		<-reply
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(time.Second)}, nil
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.Refresh(ctx) }()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal did not reach gate")
	}
	cancel()
	close(reply)
	if err := runtimeDone(t, done); err == nil {
		t.Error("canceled renewal reported success")
	}
	if g.Projection().Healthy {
		t.Error("canceled reply published a serving grant")
	}
}
