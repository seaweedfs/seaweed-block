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

type heldSkewLeaseClient struct {
	skewLeaseClient
	calls   atomic.Int32
	initial *control.WorkspaceLeaseFact
	entered chan *control.WorkspaceLeaseFact
	release chan struct{}
}

func (c *heldSkewLeaseClient) RenewWorkspaceLease(ctx context.Context, id *control.WorkspaceIdentity, opts ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	reply, err := c.skewLeaseClient.RenewWorkspaceLease(ctx, id, opts...)
	if err != nil {
		return nil, err
	}
	if c.calls.Add(1) == 1 {
		c.initial = reply
		return reply, nil
	}
	select {
	case c.entered <- reply:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-c.release:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestWorkspaceHeldRenewalBoundary(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-old-expiry", true: "after-old-expiry"}[late], func(t *testing.T) {
			store, err := authority.NewFileAuthorityStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			// Current-time clocks keep the guard's real RPC deadline in the future.
			start := time.Now()
			var masterNS, hostNS atomic.Int64
			masterNS.Store(start.UnixNano())
			hostNS.Store(start.UnixNano())
			dir := &skewDirective{asks: make(chan authority.AssignmentAsk, 1)}
			p := authority.NewPublisher(dir, authority.WithStore(store), authority.WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, masterNS.Load()) }), authority.WithWorkspaceMaxClockSkew(4*time.Second))
			seedSkewPublisher(t, p, dir)
			client := &heldSkewLeaseClient{skewLeaseClient: skewLeaseClient{p}, entered: make(chan *control.WorkspaceLeaseFact, 1), release: make(chan struct{})}
			g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return time.Unix(0, hostNS.Load()) })
			if err != nil {
				t.Fatal(err)
			}
			// No wall-clock bound on the test context (#227): the held client
			// is released by the test; only the guard's own RPC timeout remains.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := g.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			oldLease := *p.WorkspaceState("v").Lease
			oldEarly := start.Add(time.Duration(client.initial.ValidForNs))
			requestStart := start.Add(time.Second)
			hostNS.Store(requestStart.UnixNano())
			masterNS.Store(requestStart.UnixNano())
			done := make(chan struct{})
			var refreshErr error
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(client.release) }) }
			go func() { defer close(done); refreshErr = g.Refresh(ctx) }()
			t.Cleanup(func() {
				release()
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("held renewal unsettled")
				}
			})
			var reply *control.WorkspaceLeaseFact
			select {
			case reply = <-client.entered:
			case <-ctx.Done():
				t.Fatal("renewal did not enter", ctx.Err())
			}
			// A renewal is answered from memory: the grant is in the reply, and
			// the durable lease holds the horizon (>= every grant), so the
			// persisted expiry is not the grant any more (#205).
			renewed := *p.WorkspaceState("v").Lease
			freshEarly := requestStart.Add(time.Duration(reply.ValidForNs))
			if freshEarly.Sub(oldEarly) < time.Second || reply.ExpiresAtNs <= client.initial.ExpiresAtNs || renewed.ExpiresAtNS < reply.ExpiresAtNs || renewed.MaxClockSkewNS != int64(4*time.Second) {
				t.Fatal("fixture did not extend the grant under the durable horizon", client.initial, reply, oldLease, renewed)
			}
			arrival := oldEarly.Add(-time.Second)
			if late {
				arrival = oldEarly.Add(freshEarly.Sub(oldEarly) / 2)
			}
			if !arrival.Before(freshEarly) || ctx.Err() != nil {
				t.Fatal("new-grant or context expiry would mask old-boundary check")
			}
			// Do not call Projection here: it must not pre-latch the old expiry.
			hostNS.Store(arrival.UnixNano())
			masterNS.Store(arrival.UnixNano())
			release()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("held response did not settle", ctx.Err())
			}
			if late {
				if refreshErr == nil || g.Projection().Healthy {
					t.Fatal("late renewal revived lease after OLD early expiry while NEW grant remained future")
				}
			} else {
				if refreshErr != nil || !g.Projection().Healthy {
					t.Fatal("timely renewal refused", refreshErr)
				}
				hostNS.Store(oldEarly.Add(freshEarly.Sub(oldEarly) / 2).UnixNano())
				if !g.Projection().Healthy {
					t.Fatal("timely grant failed to extend old early expiry")
				}
			}
			q := authority.WorkspaceMoveRequest{RequestID: "renewed-forced", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
			head := authority.WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r", RemoteLSN: 42}
			// The barrier is max(durable horizon, renewed grant) + skew; the old
			// grant's expiry plus skew is still too early.
			barrierBase := renewed.ExpiresAtNS
			if reply.ExpiresAtNs > barrierBase {
				barrierBase = reply.ExpiresAtNs
			}
			boundary := time.Unix(0, barrierBase).Add(time.Duration(renewed.MaxClockSkewNS))
			for _, when := range []time.Time{time.Unix(0, client.initial.ExpiresAtNs).Add(4 * time.Second), time.Unix(0, reply.ExpiresAtNs).Add(4 * time.Second), boundary.Add(-time.Nanosecond)} {
				masterNS.Store(when.UnixNano())
				if _, err := p.MoveWorkspace(ctx, q, head); err == nil {
					t.Fatal("forced move ignored renewed expiry plus persisted margin")
				}
				if len(p.WorkspaceState("v").Moves) != 0 {
					t.Fatal("refusal wrote journal")
				}
			}
			masterNS.Store(boundary.UnixNano())
			hostNS.Store(boundary.Add(-time.Second).UnixNano())
			grant, err := p.MoveWorkspace(ctx, q, head)
			if err != nil || !grant.Issued || grant.NewEpoch <= 1 || g.Projection().Healthy {
				t.Fatal("renewed boundary grant overlaps old admission", grant, err)
			}
		})
	}
}
