package volume

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
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
	// cause says why the guard fenced (the first reason wins); logged once,
	// because the shutdown it triggers is otherwise silent (issue #205).
	cause  string
	logged bool
	// stopPending is a scheduling-only mode, it changes no admission or ACK
	// rule: set by StopPending when a detach starts, cleared by
	// ClearStopPending only when that detach provably did not happen and
	// serving resumed; otherwise it lasts until the guard fences. kick wakes
	// Run from its sleep.
	stopPending bool
	kick        chan struct{}
	// refusedBeforeGrant counts failed renewals before the first grant
	// (under mu), for the one grant log line.
	refusedBeforeGrant int
	// after schedules Run's sleep between renewals (time.After; tests
	// replace it before Run starts to observe the delays chosen).
	after func(time.Duration) <-chan time.Time
	// barrierAt, when set, returns the master's barrier time T (the host's
	// master wait keeps the one copy): before the first grant Run sleeps until
	// just past T instead of polling every second. Set before Run.
	barrierAt func() time.Time
	// observe, when set, receives every renewal's start and result (the
	// host's master wait). Set before Run; never called with mu held.
	observe func(time.Time, error)
}

// stopPendingPoll caps Run's sleep after a completed renewal while a stop is
// pending (the normal sleep is up to 10 s). It bounds scheduling only: a kick
// does not interrupt a renewal already in flight, and the time from a
// revocation to its ACK also includes that renewal, the drain and the ACK.
const stopPendingPoll = 250 * time.Millisecond

// WorkspaceAckTimeout bounds the holder's revocation ACK call (within the
// lease deadline). It must exceed what the master spends before it answers
// (core/host/master AckWorkspaceRevoked): its 2 s bounded HEAD read, then the
// ACK's durable write and the bindings' reconcile (about 0.3 s together,
// measured), behind its workspace lock. 5 s leaves about 2.7 s for that lock
// and for slower disks. A shorter bound lets a slow but successful ACK fail
// here (the holder exits 75 although the master may have recorded it).
const WorkspaceAckTimeout = 5 * time.Second

// WorkspaceDrainTimeout is the existing fail-closed lease drain bound.
// INV-WS-STARTUP-RETRY-001 also bounds the process cleanup by this duration.
const WorkspaceDrainTimeout = 5 * time.Second

// StopPending records that a graceful stop of this workspace has begun (the
// chunk layer is being detached) and wakes Run to renew at once; while it
// stays pending Run sleeps at most stopPendingPoll between renewals.
// Idempotent, safe from any goroutine, never blocks and makes no RPC.
func (g *WorkspaceLeaseGuard) StopPending() {
	g.mu.Lock()
	g.stopPending = true
	g.mu.Unlock()
	select {
	case g.kick <- struct{}{}:
	default:
	}
}

// ClearStopPending leaves stop-pending after a detach that provably did not
// happen (serving resumed): Run's next sleep is the normal renewal delay
// again, and a kick not yet consumed is dropped. Never blocks, makes no RPC.
func (g *WorkspaceLeaseGuard) ClearStopPending() {
	g.mu.Lock()
	g.stopPending = false
	g.mu.Unlock()
	select {
	case <-g.kick:
	default:
	}
}

// renewDelay is Run's sleep after a renewal: a third of the remaining lease
// (1 s before any grant) within [50 ms, 10 s], and at most stopPendingPoll
// while a stop is pending.
func (g *WorkspaceLeaseGuard) renewDelay() time.Duration {
	g.mu.Lock()
	now := g.now()
	delay := g.until.Sub(now) / 3
	pregrant := g.until.IsZero()
	pending := g.stopPending
	g.mu.Unlock()
	if pregrant {
		// Before the first grant: poll about once a second, jittered so the
		// volumes of one host do not stay in step; while the master has said
		// it refuses until T, sleep until just past T (it refuses every call
		// before; the request's own latency would otherwise land it early).
		delay = jittered(time.Second, time.Second/5)
		if g.barrierAt != nil {
			if t := g.barrierAt(); t.After(now) {
				delay = min(t.Sub(now)+barrierWakeMargin+jittered(0, barrierWakeJitter), barrierSleepCap)
			} else if !t.IsZero() && now.Sub(t) < barrierFastPollFor {
				// Just past T every lapsed volume re-grants at once and the
				// master refuses the ones that find its writer busy: retry
				// those soon, not a second later.
				delay = jittered(barrierFastPoll, barrierFastPoll/4)
			}
		}
		if !pending {
			return delay
		}
	}
	if delay < 50*time.Millisecond {
		delay = 50 * time.Millisecond
	}
	if delay > 10*time.Second {
		delay = 10 * time.Second
	}
	if pending && delay > stopPendingPoll {
		delay = stopPendingPoll
	}
	return delay
}

// fenceLocked latches the fence and records its first cause. Caller holds mu.
func (g *WorkspaceLeaseGuard) fenceLocked(cause string) {
	g.fenced = true
	if g.cause == "" {
		g.cause = cause
	}
}

// FenceCause is why the guard fenced, or "" while it has not.
func (g *WorkspaceLeaseGuard) FenceCause() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cause
}

// logFence logs the fence and its cause once. Never called with mu held.
func (g *WorkspaceLeaseGuard) logFence() {
	g.mu.Lock()
	if !g.fenced || g.logged {
		g.mu.Unlock()
		return
	}
	g.logged = true
	cause := g.cause
	g.mu.Unlock()
	log.Printf("workspace lease: fenced volume=%s replica=%s epoch=%d: %s", g.id.VolumeId, g.id.ReplicaId, g.id.Epoch, cause)
}

func NewWorkspaceLeaseGuard(base frontend.ProjectionView, client WorkspaceLeaseClient, id *control.WorkspaceIdentity, stop func(context.Context) (uint64, error), now func() time.Time) (*WorkspaceLeaseGuard, error) {
	if base == nil || client == nil || id == nil || id.GetVolumeId() == "" || id.GetReplicaId() == "" || id.GetEpoch() == 0 || stop == nil {
		return nil, fmt.Errorf("workspace lease guard requires identity, client and stop callback")
	}
	if now == nil {
		now = time.Now
	}
	return &WorkspaceLeaseGuard{base: base, client: client, id: control.WorkspaceIdentity{VolumeId: id.VolumeId, ReplicaId: id.ReplicaId, ServerId: id.ServerId, Epoch: id.Epoch}, stop: stop, now: now, kick: make(chan struct{}, 1), after: time.After}, nil
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
	return g.heldLocked() && p.VolumeID == g.id.VolumeId && p.ReplicaID == g.id.ReplicaId && p.Epoch == g.id.Epoch
}

// Held reports whether the lease is granted and valid now (the lease term of
// permits, without the projection identity). Never call with mu held.
func (g *WorkspaceLeaseGuard) Held() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.heldLocked()
}

// heldLocked latches the expiry fence; the caller owns mu.
func (g *WorkspaceLeaseGuard) heldLocked() bool {
	if !g.until.IsZero() && !g.now().Before(g.until) {
		g.fenceLocked(fmt.Sprintf("lease expired at %s without a successful renewal", g.until.UTC().Format(time.RFC3339Nano)))
	}
	return !g.fenced && !g.until.IsZero()
}

// Refresh anchors validity to the request START. Reply latency can only shorten
// the client's usable lease, never extend it past the master's grant window.
// ValidForNs already excludes half the master-configured clock-skew margin;
// until is the early self-fence deadline, not the raw ExpiresAtNs wall clock.
func (g *WorkspaceLeaseGuard) Refresh(ctx context.Context) error {
	g.operation.Lock()
	defer g.operation.Unlock()
	start := g.now()
	g.mu.Lock()
	if !g.until.IsZero() && !start.Before(g.until) {
		g.fenceLocked(fmt.Sprintf("lease expired at %s without a successful renewal", g.until.UTC().Format(time.RFC3339Nano)))
	}
	fenced, until := g.fenced, g.until
	g.mu.Unlock()
	if fenced {
		g.logFence()
		return g.drain(ctx, g.revoked)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, renewalRPCTimeout)
	if !until.IsZero() {
		var c context.CancelFunc
		rpcCtx, c = context.WithDeadline(rpcCtx, until)
		defer c()
	}
	defer cancel()
	reply, err := g.client.RenewWorkspaceLease(rpcCtx, &g.id)
	if g.observe != nil && ctx.Err() == nil {
		g.observe(start, err)
	}
	if err != nil {
		end := g.now()
		elapsed := end.Sub(start).Round(time.Millisecond)
		hit := renewalLimitHit(start, end, until)
		g.mu.Lock()
		if g.until.IsZero() {
			g.refusedBeforeGrant++
		}
		expired := !g.until.IsZero() && !end.Before(g.until)
		if expired {
			g.fenceLocked(fmt.Sprintf("lease expired after a failed renewal (renewal failed after %s, %s: %v)", elapsed, hit, err))
		}
		g.mu.Unlock()
		// Every failed renewal is visible with its latency and with which limit,
		// if any, the call reached: a run of them is the warning before an expiry
		// fence (issue #205).
		log.Printf("workspace lease: renewal failed after %s volume=%s epoch=%d (%s; lease valid until %s): %v",
			elapsed, g.id.VolumeId, g.id.Epoch, hit, until.UTC().Format(time.RFC3339Nano), err)
		if expired {
			g.logFence()
			return g.drainAfterFence(ctx, fmt.Errorf("workspace lease expired: renewal failed after %s (%s): %w", elapsed, hit, err))
		}
		return err
	}
	if reply.GetVolumeId() != g.id.VolumeId || reply.GetReplicaId() != g.id.ReplicaId || reply.GetEpoch() != g.id.Epoch || reply.GetValidForNs() <= 0 || reply.GetValidForNs() > int64(30*time.Second) {
		g.mu.Lock()
		g.fenceLocked(fmt.Sprintf("invalid lease reply (volume=%q replica=%q epoch=%d validForNs=%d)", reply.GetVolumeId(), reply.GetReplicaId(), reply.GetEpoch(), reply.GetValidForNs()))
		g.mu.Unlock()
		g.logFence()
		return g.drainAfterFence(ctx, fmt.Errorf("invalid workspace lease reply"))
	}
	g.mu.Lock()
	// Check the OLD deadline before accepting a newer grant. In particular a
	// reply received after expiry cannot undo a fence latched by a frontend.
	switch {
	case ctx.Err() != nil:
		g.fenceLocked("shutdown while a renewal was in flight")
	// A late reply is refused in all three cases; the cause names which limit
	// it missed, so a valid lease is not reported as an expired one.
	case !g.until.IsZero() && !g.now().Before(g.until):
		g.fenceLocked(fmt.Sprintf("renewal reply arrived after the lease deadline (renewal took %s)", g.now().Sub(start).Round(time.Millisecond)))
	case !g.now().Before(start.Add(time.Duration(reply.ValidForNs))):
		g.fenceLocked(fmt.Sprintf("renewal reply arrived after its own grant ended (renewal took %s)", g.now().Sub(start).Round(time.Millisecond)))
	case rpcCtx.Err() != nil:
		g.fenceLocked(fmt.Sprintf("renewal reply arrived after the renewal call's context ended (renewal took %s; lease still valid until %s)", g.now().Sub(start).Round(time.Millisecond), g.until.UTC().Format(time.RFC3339Nano)))
	}
	firstGrant := !g.fenced && g.until.IsZero()
	refused := g.refusedBeforeGrant
	if !g.fenced {
		g.until = start.Add(time.Duration(reply.ValidForNs))
	}
	if reply.Revoking {
		g.fenceLocked("revoked by the master")
		g.revoked = true
	}
	closed := g.fenced
	g.mu.Unlock()
	if firstGrant && !closed {
		// Once per guard: the first grant, or the re-grant after refused
		// renewals (a restart before the forced-move barrier); unix_ms is the
		// request start, which the lease validity is anchored to. A first
		// reply that revokes (or arrives fenced) grants nothing: no line.
		what := "granted"
		if refused > 0 {
			what = fmt.Sprintf("re-granted after %d refused renewal(s)", refused)
		}
		log.Printf("workspace lease: %s volume=%s replica=%s epoch=%d valid for %s at unix_ms=%d",
			what, g.id.VolumeId, g.id.ReplicaId, g.id.Epoch, time.Duration(reply.ValidForNs), start.UnixMilli())
	}
	if closed {
		g.logFence()
		return g.drain(ctx, reply.Revoking)
	}
	return nil
}

// Before the first grant: after the master's barrier time the guard wakes
// barrierWakeMargin plus up to barrierWakeJitter later (every volume a master
// restart lapsed gets the same T); one sleep is at most barrierSleepCap.
const (
	barrierWakeMargin = 100 * time.Millisecond
	barrierWakeJitter = 500 * time.Millisecond
	barrierSleepCap   = 2 * time.Minute
	// For barrierFastPollFor after T the pre-grant poll is barrierFastPoll.
	barrierFastPoll    = 200 * time.Millisecond
	barrierFastPollFor = 5 * time.Second
)

// jittered returns base plus a uniform offset in [-spread, +spread) (only
// [0, spread) when base is zero).
func jittered(base, spread time.Duration) time.Duration {
	if spread <= 0 {
		return base
	}
	if base == 0 {
		return rand.N(spread)
	}
	return base - spread + rand.N(2*spread)
}

// renewalRPCTimeout bounds one renewal call; the lease deadline may bound it
// sooner (Refresh).
const renewalRPCTimeout = 2 * time.Second

// renewalLimitHit says which limit a failed renewal call had reached when it
// returned, on the guard's clock: the lease deadline (when it came before the
// per-call timeout), the 2 s per-call timeout, or neither (the call failed
// for another reason). It separates a slow master from a dead lease.
func renewalLimitHit(start, end, until time.Time) string {
	callLimit := start.Add(renewalRPCTimeout)
	switch {
	case !until.IsZero() && until.Before(callLimit) && !end.Before(until):
		return "the call reached the lease deadline"
	case !end.Before(callLimit):
		return "the call reached the 2s renewal timeout"
	default:
		return "the call failed before any deadline"
	}
}

// drainAfterFence drains after a fence and returns err. If the stop itself
// failed, its error (which names the fence cause) is kept next to err.
// Caller holds operation.
func (g *WorkspaceLeaseGuard) drainAfterFence(ctx context.Context, err error) error {
	derr := g.drain(ctx, false)
	if !g.stopped {
		return fmt.Errorf("%w; %w", err, derr)
	}
	return err
}

func (g *WorkspaceLeaseGuard) drain(parent context.Context, ack bool) error {
	if !g.stopped {
		ctx, cancel := context.WithTimeout(context.Background(), WorkspaceDrainTimeout)
		gen, err := g.stop(ctx)
		cancel()
		if err != nil {
			g.mu.Lock()
			cause := g.cause
			g.mu.Unlock()
			return fmt.Errorf("workspace lease fenced (%s): stop failed: %w", cause, err)
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
		ctx, cancel := context.WithTimeout(leaseCtx, WorkspaceAckTimeout)
		defer cancel()
		_, err := g.client.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: &g.id, HeadGen: g.headGen})
		return err
	}
	g.mu.Lock()
	cause := g.cause
	g.mu.Unlock()
	return fmt.Errorf("workspace lease fenced: %s", cause)
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
		wake := g.after(g.renewDelay())
		select {
		case <-ctx.Done():
			g.operation.Lock()
			g.mu.Lock()
			g.fenceLocked("shutdown requested (context cancelled)")
			g.mu.Unlock()
			g.logFence()
			err := g.drain(ctx, false)
			g.operation.Unlock()
			return err
		case <-wake:
		case <-g.kick:
		}
	}
}
