// Package replication implements per-volume replication coordination:
// fan-out of live writes from primary to replicas (ReplicationVolume),
// per-replica runtime state (ReplicaPeer), and the seam to the master
// authority callback (wired at T4a-5).
//
// T4a scope: best_effort single-replica live ship. Durability closure
// (sync_all, sync_quorum, barriers) lands at T4b. Recovery pipeline
// (probe, catch-up, rebuild, promotion) lands at T4c.
package replication

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweed-block/core/transport"
)

// ReplicaTarget carries the authoritative per-replica addressing and
// lineage information from a master assignment event. Produced by the
// Host authority-callback wire (T4a-5) and consumed by
// ReplicationVolume.UpdateReplicaSet (T4a-4).
type ReplicaTarget struct {
	ReplicaID       string
	DataAddr        string
	ControlAddr     string
	Epoch           uint64
	EndpointVersion uint64
}

// ReplicaState is the coarse peer health state. T4a introduced
// Healthy ↔ Degraded; T4c-2 extends with CatchingUp + NeedsRebuild
// per memo §2.2.
//
// Transitions (T4c-2):
//
//	Healthy → CatchingUp        (probe classifies as catch-up-required)
//	CatchingUp → Healthy         (catch-up done-ack received)
//	CatchingUp → NeedsRebuild    (ErrWALRecycled OR retry budget exhausted)
//	Degraded → CatchingUp        (probe after transient drop)
//	Degraded → NeedsRebuild      (probe classifies as gap-too-large)
//	NeedsRebuild → (terminal in T4c) — T5 introduces NeedsRebuild → Rebuilding
//
// The state machine is V3-native (no V2 file maps 1-to-1 — V2's
// ReplicaState enum at `wal_shipper.go:24-32` is reference-only;
// transitions in V3 are coordinator-driven, not in-shipper).
type ReplicaState int

const (
	ReplicaUnknown ReplicaState = iota
	ReplicaHealthy
	ReplicaDegraded
	// ReplicaCatchingUp — peer is in active catch-up streaming.
	// Higher layers MUST NOT retry barrier or fence while in this
	// state; the catch-up sender owns the conn until it completes
	// (transitions to Healthy) or escalates (transitions to
	// NeedsRebuild).
	ReplicaCatchingUp
	// ReplicaNeedsRebuild — terminal in T4c. Reached on
	// ErrWALRecycled (gap exceeds substrate retention) or retry-
	// budget exhaustion. T5 introduces the recovery-from-here path
	// (NeedsRebuild → Rebuilding → Healthy).
	ReplicaNeedsRebuild
)

func (s ReplicaState) String() string {
	switch s {
	case ReplicaHealthy:
		return "healthy"
	case ReplicaDegraded:
		return "degraded"
	case ReplicaCatchingUp:
		return "catching_up"
	case ReplicaNeedsRebuild:
		return "needs_rebuild"
	default:
		return "unknown"
	}
}

// liveShipTargetLSN is the placeholder TargetLSN stamped into every
// live steady-state lineage. It is > 0 (required by the replica's
// acceptMutationLineage gate) and stable across all ShipEntry calls
// within a peer's lifetime so the replica's "same-authority ⇒ same
// TargetLSN" fence holds. Distinct from recovery TargetLSNs, which
// come from engine-frozen recovery contracts at T4c.
const liveShipTargetLSN uint64 = 1

// peerSessionIDCounter mints unique SessionIDs for live steady-state
// sessions. Recovery-bound sessions carry engine-minted SessionIDs;
// those arrive with the T4c recovery command chain.
var peerSessionIDCounter atomic.Uint64

// ReplicaPeer is the per-remote-replica runtime handle. Owns its
// coarse health state, the peer-scoped live-ship RecoveryLineage, and
// the lifecycle of its registered session in the executor. Bridges
// ReplicationVolume fan-out (T4a-4) to the transport layer (T4a-2).
type ReplicaPeer struct {
	target    ReplicaTarget
	executor  *transport.BlockExecutor
	sessionID uint64
	lineage   transport.RecoveryLineage

	mu     sync.Mutex
	state  ReplicaState
	closed bool
}

// NewReplicaPeer constructs a per-replica runtime handle and registers
// a live-ship session against the given executor. The session's
// lineage is derived from target.{Epoch, EndpointVersion}, a freshly
// minted peer-local SessionID, and a constant liveShipTargetLSN.
// Recovery-minted sessions (with engine-frozen TargetLSNs) arrive at
// T4c when the recovery command chain ports over.
//
// Called by: ReplicationVolume.UpdateReplicaSet (T4a-4) when adding
// a new peer to the authoritative replica set.
// Owns: ReplicaState; the peer's registered session in the executor
// (released by Close via InvalidateSession); the peer-scoped
// RecoveryLineage.
// Borrows: *transport.BlockExecutor — caller retains ownership and
// is responsible for executor lifecycle. Peer does not close the
// executor (BUG-005-class discipline).
func NewReplicaPeer(target ReplicaTarget, executor *transport.BlockExecutor) (*ReplicaPeer, error) {
	if executor == nil {
		return nil, fmt.Errorf("replication: NewReplicaPeer: executor is nil")
	}
	if target.Epoch == 0 || target.EndpointVersion == 0 {
		return nil, fmt.Errorf(
			"replication: NewReplicaPeer: target needs nonzero epoch (got %d) and endpointVersion (got %d)",
			target.Epoch, target.EndpointVersion)
	}
	sessionID := peerSessionIDCounter.Add(1)
	lineage := transport.RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           target.Epoch,
		EndpointVersion: target.EndpointVersion,
		TargetLSN:       liveShipTargetLSN,
	}
	if err := executor.RegisterLiveShipSession(lineage); err != nil {
		return nil, fmt.Errorf("replication: NewReplicaPeer: register session: %w", err)
	}
	return &ReplicaPeer{
		target:    target,
		executor:  executor,
		sessionID: sessionID,
		lineage:   lineage,
		state:     ReplicaHealthy,
	}, nil
}

// Target returns the peer's ReplicaTarget. Read-only accessor for
// callers that need to inspect the authoritative lineage or address.
func (p *ReplicaPeer) Target() ReplicaTarget { return p.target }

// State returns the peer's current coarse health state. Thread-safe.
func (p *ReplicaPeer) State() ReplicaState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// ShipEntry ships one live WAL entry to this peer via its BlockExecutor.
// Returns error on transport failure; on error the peer is marked
// Degraded via Invalidate (forward-carry CARRY-1 from T4a-2). A
// Degraded peer rejects subsequent ShipEntry calls until the recovery
// pipeline (T4c) drives it back to Healthy.
//
// The lineage param shape matches the sketch in v3-phase-15-t4-sketch
// so ReplicationVolume (T4a-4) can thread authoritative lineage info
// through. For T4a, the peer ships under its own registered lineage
// (peer owns its live-ship session); caller-supplied lineage is
// informational only. T4b/T4c may tighten this as recovery-bound
// sessions arrive.
//
// Called by: ReplicationVolume.OnLocalWrite (T4a-4) fan-out loop.
// Owns: peer-state mutation on ship error (via Invalidate).
// Borrows: data slice — caller retains, ShipEntry does not mutate
// and does not retain past return.
func (p *ReplicaPeer) ShipEntry(ctx context.Context, lineage transport.RecoveryLineage, lba uint32, lsn uint64, data []byte) error {
	_ = lineage // T4a: peer-owned lineage; caller param is informational.
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("replication: ShipEntry: peer %s closed", p.target.ReplicaID)
	}
	if p.state == ReplicaDegraded {
		p.mu.Unlock()
		return fmt.Errorf("replication: ShipEntry: peer %s degraded", p.target.ReplicaID)
	}
	peerLineage := p.lineage
	p.mu.Unlock()

	if err := p.executor.Ship(p.target.ReplicaID, peerLineage, lba, lsn, data); err != nil {
		log.Printf("replication: ship FAILED peer=%s addr=%s lba=%d lsn=%d: %v",
			p.target.ReplicaID, p.target.DataAddr, lba, lsn, err)
		p.Invalidate(fmt.Sprintf("ship error: %v", err))
		return err
	}
	log.Printf("replication: ship ok peer=%s lba=%d lsn=%d",
		p.target.ReplicaID, lba, lsn)
	return nil
}

// Barrier runs a per-peer durability barrier and returns the
// replica's validated ack. On any transport / lineage-mismatch /
// short-payload error, translates error → peer.Invalidate +
// state=Degraded per INV-REPL-BARRIER-FAILURE-DEGRADES-PEER, then
// surfaces the error to the caller. This preserves V2's
// per-peer-execution-layer locality (V2 WALShipper.Barrier mutated
// peer state internally via failBarrier → markDegraded; V3 puts
// that mutation at the V2-faithful V3 location: ReplicaPeer.Barrier).
//
// MUST NOT gate the attempt on p.state == Degraded. The V2 "always
// attempt BarrierAll, even when all shippers are Disconnected or
// Degraded" invariant applies: an in-flight barrier may succeed
// and transition the peer back to a healthy posture; gating it
// out would make Degraded a one-way trap. Only the post-Close
// gate applies.
//
// Wire payload: sends MsgBarrierReq with the peer's registered
// lineage (unchanged from session registration). The caller-supplied
// targetLSN is passed through to BlockExecutor.Barrier but does NOT
// overwrite lineage.TargetLSN — replica's acceptMutationLineage
// requires same-authority messages to carry the same TargetLSN as
// the session was registered with, so overriding would self-reject.
// Caller uses targetLSN at the coordinator layer (comparing against
// ack.AchievedLSN) to decide per-mode durability arithmetic.
//
// Called by: DurabilityCoordinator.SyncLocalAndReplicas (T4b-4)
// per-peer fan-out.
// Owns: the error → Invalidate translation (peer state mutation).
// Borrows: ctx + targetLSN from caller; lineage is read from peer's
// own registered state.
func (p *ReplicaPeer) Barrier(ctx context.Context, targetLSN uint64) (transport.BarrierAck, error) {
	if err := ctx.Err(); err != nil {
		return transport.BarrierAck{}, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return transport.BarrierAck{}, fmt.Errorf("replication: Barrier: peer %s closed", p.target.ReplicaID)
	}
	lineage := p.lineage
	p.mu.Unlock()

	log.Printf("replication: barrier wait peer=%s addr=%s targetLSN=%d epoch=%d",
		p.target.ReplicaID, p.target.DataAddr, targetLSN, lineage.Epoch)
	ack, err := p.executor.Barrier(p.target.ReplicaID, lineage, targetLSN)
	if err != nil {
		log.Printf("replication: barrier FAILED peer=%s addr=%s targetLSN=%d: %v",
			p.target.ReplicaID, p.target.DataAddr, targetLSN, err)
		p.Invalidate(fmt.Sprintf("barrier error: %v", err))
		return transport.BarrierAck{}, err
	}
	log.Printf("replication: barrier ack peer=%s targetLSN=%d achievedLSN=%d",
		p.target.ReplicaID, targetLSN, ack.AchievedLSN)
	return ack, nil
}

// Fence issues a synchronous fence exchange against this peer using
// the caller-supplied lineage. Returns nil on success or a
// descriptive error on any failure. On error, marks the peer
// Degraded via Invalidate (parallel to Barrier).
//
// Lineage is caller-supplied (not peer-owned) because fence is used
// to confirm a SPECIFIC authority tuple was received — typically
// when the coordinator wants to verify an old lineage was acked
// before advancing. The caller knows which lineage matters; the
// peer doesn't drive the choice.
//
// Called by: DurabilityCoordinator (T4b-4) on quorum-loss recovery;
// future T4c recovery pipeline.
// Owns: error → Invalidate translation on failure.
// Borrows: ctx + lineage from caller.
func (p *ReplicaPeer) Fence(ctx context.Context, lineage transport.RecoveryLineage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("replication: Fence: peer %s closed", p.target.ReplicaID)
	}
	p.mu.Unlock()

	if err := p.executor.FenceSync(p.target.ReplicaID, lineage); err != nil {
		p.Invalidate(fmt.Sprintf("fence error: %v", err))
		return err
	}
	return nil
}

// SetState transitions the peer to the given state per the T4c-2
// state machine (see ReplicaState godoc). Invalid transitions log a
// warning but DO NOT panic — silent rejection lets the coordinator
// keep running while the misbehavior is investigated. The caller
// MUST hold appropriate context (e.g., ack observation or
// ErrWALRecycled detection) before requesting a transition.
//
// Allowed transitions (T4c-2, memo §2.2):
//
//	any        → Healthy        (initial / ack-confirmed steady)
//	Healthy    → Degraded       (T4a/T4b: ship/barrier failure)
//	Degraded   → Healthy        (recovery completed)
//	Healthy    → CatchingUp     (probe → catch-up decision)
//	Degraded   → CatchingUp     (probe after transient drop)
//	CatchingUp → Healthy        (catch-up done-ack)
//	CatchingUp → NeedsRebuild   (ErrWALRecycled OR retry exhausted)
//	Degraded   → NeedsRebuild   (probe → rebuild decision)
//	NeedsRebuild → (no successor in T4c — terminal)
//
// Called by: coordinator (T4c-2 catch-up flow, ErrWALRecycled
// escalation) and engine SessionClose handler (close-with-success
// returns to Healthy).
// Owns: peer-state mutation under internal lock.
func (p *ReplicaPeer) SetState(next ReplicaState) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	prev := p.state
	if !replicaStateTransitionAllowed(prev, next) {
		p.mu.Unlock()
		log.Printf("replication: peer %s rejected illegal state transition %s → %s",
			p.target.ReplicaID, prev, next)
		return
	}
	p.state = next
	p.mu.Unlock()
	log.Printf("replication: peer %s state %s → %s", p.target.ReplicaID, prev, next)
}

// replicaStateTransitionAllowed is the table-driven transition gate.
// See SetState godoc for the rule list. NeedsRebuild is terminal in
// T4c; T5 will extend with NeedsRebuild → Rebuilding.
func replicaStateTransitionAllowed(prev, next ReplicaState) bool {
	if prev == next {
		return true // idempotent reset is harmless
	}
	if next == ReplicaHealthy {
		// Anything but NeedsRebuild can resume Healthy. NeedsRebuild
		// is terminal in T4c — must go through T5's Rebuilding path
		// before returning to Healthy.
		return prev != ReplicaNeedsRebuild
	}
	switch prev {
	case ReplicaUnknown, ReplicaHealthy:
		return next == ReplicaDegraded || next == ReplicaCatchingUp
	case ReplicaDegraded:
		return next == ReplicaCatchingUp || next == ReplicaNeedsRebuild
	case ReplicaCatchingUp:
		return next == ReplicaNeedsRebuild || next == ReplicaDegraded
	case ReplicaNeedsRebuild:
		return false // terminal in T4c
	}
	return false
}

// Invalidate marks the peer Degraded and logs the reason. T4a/T4b
// behavior preserved: Healthy ↔ Degraded transitions on ship /
// barrier failure. T4c-2 SetState handles the richer transitions
// (CatchingUp, NeedsRebuild). Calling Invalidate on an
// already-Degraded peer is a no-op (beyond the log line — still
// useful because the reason string may be new).
//
// Called by: ShipEntry on transport error; ReplicationVolume on
// explicit peer-down from authority updates.
// Owns: peer-state mutation under internal lock.
// Borrows: reason string — caller retains.
func (p *ReplicaPeer) Invalidate(reason string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	prev := p.state
	p.state = ReplicaDegraded
	p.mu.Unlock()
	log.Printf("replication: peer %s invalidated (prev=%s, reason=%s)",
		p.target.ReplicaID, prev, reason)
}

// Close tears down the peer's registered session in the executor and
// prevents further ShipEntry calls. Idempotent — second and later
// calls are no-ops and return nil. Does NOT close the executor
// (caller owns executor lifecycle, BUG-005 discipline).
//
// Called by: ReplicationVolume.UpdateReplicaSet when removing this
// peer from the authoritative replica set.
// Owns: invalidation of the peer's session in the executor; setting
// the closed flag.
// Borrows: nothing.
func (p *ReplicaPeer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	sessionID := p.sessionID
	replicaID := p.target.ReplicaID
	p.state = ReplicaUnknown
	p.mu.Unlock()
	p.executor.InvalidateSession(replicaID, sessionID, "replica peer closed")
	return nil
}
