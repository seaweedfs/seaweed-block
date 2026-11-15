package volume

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

type WorkspaceLeaseClient interface {
	RenewWorkspaceLease(context.Context, *control.WorkspaceIdentity, ...grpc.CallOption) (*control.WorkspaceLeaseFact, error)
	AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error)
}

// WorkspaceLeaseGuard wraps the projection used by BOTH frontend and captures.
// Stop must close/drain the frontend and stop all cuts before returning the last
// committed HEAD gen. No ACK is sent unless Stop succeeded. A fenced guard never
// reopens, even if a delayed renewal response arrives; a new assignment needs a
// new guard. W4b wires Stop to the chunk-enabled blockvolume lifecycle.
type WorkspaceLeaseGuard struct {
	base      frontend.ProjectionView
	client    WorkspaceLeaseClient
	id        control.WorkspaceIdentity
	stop      func(context.Context) (uint64, error)
	now       func() time.Time
	operation sync.Mutex // serial renew/drain/ACK, never acquired by Projection
	mu        sync.Mutex // no RPC, Stop, or base projection call under mu
	until     time.Time
	fenced    bool
	stopped   bool
	headGen   uint64
	revoked   bool
}

func NewWorkspaceLeaseGuard(base frontend.ProjectionView, client WorkspaceLeaseClient, id *control.WorkspaceIdentity, stop func(context.Context) (uint64, error), now func() time.Time) (*WorkspaceLeaseGuard, error) {
	if base == nil || client == nil || id == nil || id.GetVolumeId() == "" || id.GetReplicaId() == "" || id.GetEpoch() == 0 || stop == nil {
		return nil, fmt.Errorf("workspace lease guard requires identity, client and stop callback")
	}
	if now == nil {
		now = time.Now
	}
	return &WorkspaceLeaseGuard{base: base, client: client, id: control.WorkspaceIdentity{VolumeId: id.VolumeId, ReplicaId: id.ReplicaId, Epoch: id.Epoch}, stop: stop, now: now}, nil
}

func (g *WorkspaceLeaseGuard) Projection() frontend.Projection {
	p := g.base.Projection()
	p.Healthy = p.Healthy && g.permits(p)
	return p
}

// permits does not call the base view, so the host's shared projection can
// apply this lease gate without recursion. Never hold mu across RPC/drain.
func (g *WorkspaceLeaseGuard) permits(p frontend.Projection) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.until.IsZero() && !g.now().Before(g.until) {
		g.fenced = true
	}
	return !g.fenced && !g.until.IsZero() && p.VolumeID == g.id.VolumeId && p.ReplicaID == g.id.ReplicaId && p.Epoch == g.id.Epoch
}

// Refresh anchors validity to the request START. Reply latency can only shorten
// the client's usable lease, never extend it past the master's grant window.
func (g *WorkspaceLeaseGuard) Refresh(ctx context.Context) error {
	g.operation.Lock()
	defer g.operation.Unlock()
	start := g.now()
	g.mu.Lock()
	if !g.until.IsZero() && !start.Before(g.until) {
		g.fenced = true
	}
	fenced, until := g.fenced, g.until
	g.mu.Unlock()
	if fenced {
		return g.drain(ctx, g.revoked)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	if !until.IsZero() {
		var c context.CancelFunc
		rpcCtx, c = context.WithDeadline(rpcCtx, until)
		defer c()
	}
	defer cancel()
	reply, err := g.client.RenewWorkspaceLease(rpcCtx, &g.id)
	if err != nil {
		g.mu.Lock()
		expired := !g.until.IsZero() && !g.now().Before(g.until)
		if expired {
			g.fenced = true
		}
		g.mu.Unlock()
		if expired {
			_ = g.drain(ctx, false)
		}
		return err
	}
	if reply.GetVolumeId() != g.id.VolumeId || reply.GetReplicaId() != g.id.ReplicaId || reply.GetEpoch() != g.id.Epoch || reply.GetValidForNs() <= 0 || reply.GetValidForNs() > int64(30*time.Second) {
		g.mu.Lock()
		g.fenced = true
		g.mu.Unlock()
		_ = g.drain(ctx, false)
		return fmt.Errorf("invalid workspace lease reply")
	}
	g.mu.Lock()
	// Check the OLD deadline before accepting a newer grant. In particular a
	// reply received after expiry cannot undo a fence latched by a frontend.
	if ctx.Err() != nil || rpcCtx.Err() != nil || (!g.until.IsZero() && !g.now().Before(g.until)) || !g.now().Before(start.Add(time.Duration(reply.ValidForNs))) {
		g.fenced = true
	}
	if !g.fenced {
		g.until = start.Add(time.Duration(reply.ValidForNs))
	}
	if reply.Revoking {
		g.fenced = true
		g.revoked = true
	}
	closed := g.fenced
	g.mu.Unlock()
	if closed {
		return g.drain(ctx, reply.Revoking)
	}
	return nil
}

func (g *WorkspaceLeaseGuard) drain(parent context.Context, ack bool) error {
	if !g.stopped {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		gen, err := g.stop(ctx)
		cancel()
		if err != nil {
			return err
		}
		g.headGen, g.stopped = gen, true
	}
	if ack {
		if err := parent.Err(); err != nil {
			return err
		}
		g.mu.Lock()
		until := g.until
		valid := !until.IsZero() && g.now().Before(until)
		g.mu.Unlock()
		if !valid {
			return fmt.Errorf("workspace lease expired before clean ACK")
		}
		if g.headGen == 0 {
			return fmt.Errorf("revocation drained without a committed HEAD gen")
		}
		leaseCtx, leaseCancel := context.WithDeadline(parent, until)
		defer leaseCancel()
		ctx, cancel := context.WithTimeout(leaseCtx, 2*time.Second)
		defer cancel()
		_, err := g.client.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: &g.id, HeadGen: g.headGen})
		return err
	}
	return fmt.Errorf("workspace lease fenced")
}

// Run owns renewal and physical close. Projection independently checks expiry,
// so a delayed timer never grants extra serving/cut time.
func (g *WorkspaceLeaseGuard) Run(ctx context.Context) error {
	for {
		err := g.Refresh(ctx)
		g.mu.Lock()
		closed := g.fenced
		g.mu.Unlock()
		if closed {
			return err
		}
		g.mu.Lock()
		delay := g.until.Sub(g.now()) / 3
		if g.until.IsZero() {
			delay = time.Second
		}
		g.mu.Unlock()
		if delay < 50*time.Millisecond {
			delay = 50 * time.Millisecond
		}
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
		select {
		case <-ctx.Done():
			g.operation.Lock()
			g.mu.Lock()
			g.fenced = true
			g.mu.Unlock()
			err := g.drain(ctx, false)
			g.operation.Unlock()
			return err
		case <-time.After(delay):
		}
	}
}
