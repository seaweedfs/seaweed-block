package master

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

// slowGuardStore delays every workspace-lease Put once armed (issue #205).
type slowGuardStore struct {
	authority.AuthorityStore
	delayNS atomic.Int64
}

func (s *slowGuardStore) Put(r authority.DurableRecord) error {
	if d := time.Duration(s.delayNS.Load()); d > 0 && r.Workspace != nil && r.Workspace.Lease != nil {
		time.Sleep(d)
	}
	return s.AuthorityStore.Put(r)
}

// deadlineLeaseClient calls the publisher through the master's leaseFact and,
// like a gRPC client, returns at the caller's deadline even when the master
// call is still blocked. In-flight calls are joined before the store closes.
type deadlineLeaseClient struct {
	p        *authority.Publisher
	inflight *sync.WaitGroup
}

func (c deadlineLeaseClient) RenewWorkspaceLease(ctx context.Context, id *control.WorkspaceIdentity, _ ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	type result struct {
		fact *control.WorkspaceLeaseFact
		err  error
	}
	done := make(chan result, 1)
	c.inflight.Add(1)
	go func() {
		defer c.inflight.Done()
		l, err := c.p.RenewWorkspaceLease(context.Background(), id.VolumeId, id.ReplicaId, id.Epoch)
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{fact: leaseFact(id.VolumeId, l)}
	}()
	select {
	case r := <-done:
		return r.fact, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (deadlineLeaseClient) AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	panic("a slow store must not lead to a clean ACK")
}

// A live host must keep its lease while the master's authority store is slow:
// a store write longer than the host's 2 s renewal timeout must not fence the
// guard and shut the volume down (issue #205). TTL 6 s gives the guard 4 s of
// validity (6 s minus half the default 4 s skew), renewed every ~1.3 s.
func TestWorkspaceLeaseGuardSurvivesSlowAuthorityStore(t *testing.T) {
	disk, err := authority.NewFileAuthorityStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	slow := &slowGuardStore{AuthorityStore: disk}
	var inflight sync.WaitGroup
	t.Cleanup(func() {
		slow.delayNS.Store(0)
		inflight.Wait()
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	dir := &skewDirective{asks: make(chan authority.AssignmentAsk, 1)}
	p := authority.NewPublisher(dir, authority.WithStore(slow), authority.WithWorkspaceLeaseConfig(6*time.Second, nil))
	seedSkewPublisher(t, p, dir)
	var stops atomic.Int32
	g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}},
		deadlineLeaseClient{p: p, inflight: &inflight}, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1},
		func(context.Context) (uint64, error) { stops.Add(1); return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Error("guard did not settle")
		}
	})
	for wait := time.Now().Add(3 * time.Second); !g.Projection().Healthy; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(wait) {
			t.Fatal("first grant did not arrive")
		}
	}
	slow.delayNS.Store(int64(2500 * time.Millisecond))
	start := time.Now()
	for time.Since(start) < 10*time.Second {
		if !g.Projection().Healthy || stops.Load() != 0 {
			t.Fatalf("guard fenced %s after the store slowed to 2.5 s per write (stop calls %d)", time.Since(start).Round(time.Millisecond), stops.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
}
