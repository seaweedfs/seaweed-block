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
	"time"

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

// PeerProbeCooldown holds the per-peer backoff parameters used by
// ProbeIfDegraded / OnProbeAttempt (G5-5C §1.G #7).
//
// IMPORTANT: this is per-peer cooldown / backoff, NOT the global probe
// loop interval. Two distinct time concepts in G5-5C:
//
//   - Loop interval (ProbeLoopConfig.Interval, default 5s): how often
//     the probe loop wakes up and iterates over peers. Applies to the
//     whole volume's probe loop, not to any single peer.
//   - Per-peer cooldown (Base, Cap): how long a peer is gated AFTER a
//     probe attempt before the loop will dispatch the next probe to
//     that same peer. Starts at Base on first attempt or after a
//     success; doubles on each consecutive failure up to Cap; resets
//     to Base on success.
//
// These are independent. A peer can be in cooldown longer than one
// loop interval; the loop simply skips it on subsequent ticks until
// cooldown elapses.
type PeerProbeCooldown struct {
	// Base is the initial cooldown after a probe attempt and the
	// reset value after a successful probe. Default 5s.
	Base time.Duration
	// Cap is the maximum cooldown after consecutive failures.
	// Default 60s. Cap >= Base is enforced (Cap < Base normalizes
	// to Cap = Base).
	Cap time.Duration
}

// DefaultPeerProbeCooldown returns the architect-bound G5-5C defaults
// (5s base, 60s cap).
func DefaultPeerProbeCooldown() PeerProbeCooldown {
	return PeerProbeCooldown{
		Base: 5 * time.Second,
		Cap:  60 * time.Second,
	}
}

// ReplicaPeer is the per-remote-replica runtime handle. Owns its
// coarse health state, the peer-scoped live-ship RecoveryLineage, and
// the lifecycle of its registered session in the executor. Bridges
// ReplicationVolume fan-out (T4a-4) to the transport layer (T4a-2).
//
// G5-5C extension: also owns per-peer probe cooldown state (cooldown
// deadline, consecutive-failure count, in-flight flag) so the
// degraded-peer probe loop can gate / dispatch probes idempotently.
// All probe state is mutated under p.mu; the actual probe transport
// call MUST run with p.mu released to avoid lock-order crossings
// (architect G5-5C #2 binding 2026-04-27).
type ReplicaPeer struct {
	target    ReplicaTarget
	executor  *transport.BlockExecutor
	sessionID uint64
	lineage   transport.RecoveryLineage

	mu     sync.Mutex
	state  ReplicaState
	closed bool

	// G5-5C probe cooldown / in-flight state. All fields read+written
	// under p.mu only.
	cooldownCfg       PeerProbeCooldown // policy (Base, Cap); defaults set in NewReplicaPeer
	probeNextEligible time.Time         // wall-clock; before this, ProbeIfDegraded returns false
	probeFailureCount int               // consecutive failures (0 after success or initial); drives backoff doubling
	probeInFlight     bool              // true while a probe attempt is dispatched and not yet recorded
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
		target:      target,
		executor:    executor,
		sessionID:   sessionID,
		lineage:     lineage,
		state:       ReplicaHealthy,
		cooldownCfg: DefaultPeerProbeCooldown(),
	}, nil
}

// SetProbeCooldownConfig overrides the default probe cooldown config
// for this peer. Safe to call from any goroutine; takes effect from
// the next ProbeIfDegraded / OnProbeAttempt call. Cap < Base
// normalizes to Cap = Base. A zero Base or Cap leaves the existing
// value unchanged for that field.
//
// Called by: ReplicationVolume after creating a peer, to push the
// volume-level probe loop config down. Tests inject custom values to
// shorten test wall-clock without flakiness.
func (p *ReplicaPeer) SetProbeCooldownConfig(cfg PeerProbeCooldown) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cfg.Base > 0 {
		p.cooldownCfg.Base = cfg.Base
	}
	if cfg.Cap > 0 {
		p.cooldownCfg.Cap = cfg.Cap
	}
	if p.cooldownCfg.Cap < p.cooldownCfg.Base {
		p.cooldownCfg.Cap = p.cooldownCfg.Base
	}
}

// ProbeIfDegraded gates whether the probe loop should dispatch a probe
// for this peer right now. Returns true and atomically marks the peer
// in-flight iff ALL of:
//
//   - peer is not closed (§1.E (c): closed peers are torn-down lineage)
//   - peer.state == ReplicaDegraded (§1.A: only-on-degraded discipline)
//   - now >= probeNextEligible (per-peer cooldown elapsed)
//   - !probeInFlight (single in-flight per peer — INV-G5-5C-SINGLE-INFLIGHT-PER-PEER)
//
// On returning true, the caller MUST eventually call OnProbeAttempt
// to release the in-flight flag and advance cooldown. If the caller
// fails to call OnProbeAttempt (e.g., panic up the stack), the peer
// stays in-flight forever — callers MUST defer OnProbeAttempt
// immediately after the true return. ProbeLoop.dispatchProbe handles
// this via deferred resultFn invocation.
//
// CRITICAL: this method runs entirely under p.mu. The actual probe
// transport call (executor.Probe via the host-injected probeFn) MUST
// run with p.mu RELEASED to avoid lock-order crossings with shipper /
// other peers (architect G5-5C #2 binding 2026-04-27). The probe
// loop's dispatchProbe respects this — ProbeIfDegraded is the
// CooldownFn (synchronous, peer.mu-held); probeFn is dispatched
// after the gate returns.
//
// Pinned by:
//   - INV-G5-5C-SINGLE-INFLIGHT-PER-PEER
//   - INV-G5-5C-RECOVERY-BACKOFF (cooldown gate)
//   - INV-G5-5C-PRIMARY-RECOVERY-AUTHORITY-BOUNDED (closed-peer skip)
//
// Called by: ProbeLoop.tick via the default CooldownFn wrapper
// (DefaultProbeCooldownFn).
// Owns: probeInFlight transition false → true on success.
// Borrows: now from caller (testable wall-clock injection).
func (p *ReplicaPeer) ProbeIfDegraded(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	if p.state != ReplicaDegraded {
		return false
	}
	if p.probeInFlight {
		return false
	}
	if !p.probeNextEligible.IsZero() && now.Before(p.probeNextEligible) {
		return false
	}
	p.probeInFlight = true
	return true
}

// OnProbeAttempt records the outcome of a probe dispatched after a
// successful ProbeIfDegraded. Releases the in-flight flag and advances
// cooldown:
//
//   - success: probeFailureCount = 0; nextEligible = now + Base.
//   - failure: probeFailureCount++; nextEligible = now + min(Base * 2^(failureCount-1), Cap).
//
// Idempotent on a closed peer: if the peer was Closed during the
// in-flight window (§1.E (c) lineage teardown), this method clears
// the in-flight flag and returns silently — no cooldown update on
// a torn-down peer. This guarantees Close() semantics: a peer Close()
// followed by a delayed OnProbeAttempt does not leak in-flight or
// cooldown state into a fresh peer that may share the same
// underlying address.
//
// CRITICAL: like ProbeIfDegraded, runs entirely under p.mu. Must NOT
// call into the engine, the executor, or any other peer's lock. The
// engine-side recovery FSM advance (driving Decision via probe R/S/H)
// is the responsibility of the host's probeFn → adapter.OnProbeResult
// path, not this method.
//
// Pinned by:
//   - INV-G5-5C-RECOVERY-BACKOFF (5s → 10s → 20s → 40s → 60s cap)
//   - INV-G5-5C-SINGLE-INFLIGHT-PER-PEER (in-flight release)
//
// Called by: ProbeLoop.dispatchProbe via the default ResultFn wrapper
// (DefaultProbeResultFn).
// Owns: probeInFlight transition true → false; probeFailureCount
// monotonic update; probeNextEligible deadline update.
// Borrows: now from caller; success bool.
func (p *ReplicaPeer) OnProbeAttempt(now time.Time, success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Always clear in-flight, even on closed: prevents leaks.
	p.probeInFlight = false
	if p.closed {
		return
	}
	if success {
		p.probeFailureCount = 0
		p.probeNextEligible = now.Add(p.cooldownCfg.Base)
		return
	}
	p.probeFailureCount++
	// Compute backoff: Base * 2^(failureCount-1), capped at Cap.
	// Use multiplicative loop to avoid overflow on large counts;
	// once we reach Cap we stop doubling.
	delay := p.cooldownCfg.Base
	for i := 1; i < p.probeFailureCount; i++ {
		next := delay * 2
		if next > p.cooldownCfg.Cap || next < delay /* overflow guard */ {
			delay = p.cooldownCfg.Cap
			break
		}
		delay = next
	}
	if delay > p.cooldownCfg.Cap {
		delay = p.cooldownCfg.Cap
	}
	p.probeNextEligible = now.Add(delay)
}

// IsProbeInFlight reports whether a probe is currently dispatched
// against this peer. Test/diagnostic accessor.
func (p *ReplicaPeer) IsProbeInFlight() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.probeInFlight
}

// ProbeNextEligibleAt returns the wall-clock time at which the next
// probe attempt is allowed (after the current cooldown). Zero time
// means "no cooldown, eligible immediately". Test/diagnostic
// accessor.
func (p *ReplicaPeer) ProbeNextEligibleAt() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.probeNextEligible
}

// DefaultProbeCooldownFn returns a CooldownFn that delegates to each
// peer's ProbeIfDegraded(now) using the supplied clock function. The
// clock indirection is the standard testability seam (production
// passes time.Now; tests pass a controlled clock).
func DefaultProbeCooldownFn(now func() time.Time) CooldownFn {
	if now == nil {
		now = time.Now
	}
	return func(p *ReplicaPeer) bool {
		return p.ProbeIfDegraded(now())
	}
}

// DefaultProbeResultFn returns a ResultFn that delegates to each
// peer's OnProbeAttempt(now, err == nil) using the supplied clock.
func DefaultProbeResultFn(now func() time.Time) ResultFn {
	if now == nil {
		now = time.Now
	}
	return func(p *ReplicaPeer, err error) {
		p.OnProbeAttempt(now(), err == nil)
	}
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
		log.Printf("replication: ship gate-close peer=%s lba=%d lsn=%d (peer closed)",
			p.target.ReplicaID, lba, lsn)
		return fmt.Errorf("replication: ShipEntry: peer %s closed", p.target.ReplicaID)
	}
	if p.state == ReplicaDegraded {
		p.mu.Unlock()
		log.Printf("replication: ship gate-degraded peer=%s lba=%d lsn=%d (peer degraded)",
			p.target.ReplicaID, lba, lsn)
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
