package recovery

import (
	"errors"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Compile-time assertion: PeerShipCoordinator's
// MinPinAcrossActiveSessions signature satisfies the substrate's
// RecycleFloorSource interface (G7-redo priority 2.5). If this break
// it means the coordinator's pin-floor query method changed and the
// substrate gate needs to track.
var _ storage.RecycleFloorSource = (*PeerShipCoordinator)(nil)

// ReplicaID is the per-volume peer identifier the coordinator keys on.
// Defined locally so this package stays independent of engine/adapter
// types; integration code adapts to the project-wide peer identifier.
type ReplicaID string

// PeerShipPhase is the per-peer ship-phase enum required by
// `v3-recovery-live-line-backlog-spec.md` §3.2.
//
//	Idle              — no active recover session for this peer; Primary
//	                    may steady-live-ship per existing reachability
//	                    rules (§3.1).
//	DrainingHistorical — session active; historical WAL backlog has not
//	                    yet been drained to the frozen target. Primary
//	                    MUST NOT publish (peer, InSync) for recover
//	                    completion based on steady-live throughput
//	                    alone (§3.2 last paragraph).
//	SteadyLiveAllowed  — session active but Backlog drained ∧ baseDone
//	                    hold; live ship is the authoritative path until
//	                    barrier-ack closes the session.
type PeerShipPhase int

const (
	PhaseIdle PeerShipPhase = iota
	PhaseDrainingHistorical
	PhaseSteadyLiveAllowed
)

func (p PeerShipPhase) String() string {
	switch p {
	case PhaseIdle:
		return "Idle"
	case PhaseDrainingHistorical:
		return "DrainingHistorical"
	case PhaseSteadyLiveAllowed:
		return "SteadyLiveAllowed"
	default:
		return fmt.Sprintf("Phase(%d)", int(p))
	}
}

// LocalWriteRouting tells the WAL shipper which lane a freshly produced
// local write must travel on for a given peer, given that peer's current
// ship phase. Spec §3.2 #3: "one ordered outbound queue per peer mixing
// recover-tagged and post-target traffic with explicit LSN order".
//
// §IV.0 T3 (v3-recovery-algorithm-consensus.md): RouteSessionLane means
// all WAL for this peer MUST ship on the recover dual-lane session path —
// never a parallel steady bearer fork (§I P2).
type LocalWriteRouting int

const (
	// RouteSteadyLive — peer is Idle OR session has reached
	// SteadyLiveAllowed; the entry travels on the steady live-ship path.
	RouteSteadyLive LocalWriteRouting = iota
	// RouteSessionLane — peer is DrainingHistorical; the entry must be
	// queued on the session's ordered lane behind any outstanding
	// backlog (CHK-NO-FAKE-LIVE-DURING-BACKLOG).
	RouteSessionLane
)

// PeerShipCoordinator owns the per-peer ship-phase state machine and
// the "backlog drained" predicate. It does NOT own:
//   - the ship queue itself (transport's job),
//   - barrier wire round-trips (transport's job),
//   - pin / recycle floor advancement (separate concern; see §6 of
//     spec — surface added below as `PinFloor` so the recycle gate
//     can read a single number per peer).
//
// Locking: one top-level mutex; per-peer state read/written under it.
// Coordinator hot path is one map lookup + a few field updates per
// local write event; the mutex is fine for POC. If integration shows
// contention, shard by replicaID.
type PeerShipCoordinator struct {
	mu     sync.Mutex
	states map[ReplicaID]*peerShipState
}

type peerShipState struct {
	phase      PeerShipPhase
	sessionID  uint64
	fromLSN    uint64 // session's lower LSN bound (pinned at start; engine-owned)
	targetLSN  uint64 // session's frozen upper LSN bound
	shipCursor uint64 // highest LSN successfully shipped on the session lane
	baseDone   bool

	// pinFloor is the recycle gate value for THIS peer's session. May
	// equal fromLSN at session start; advances on replica BaseBatchAcked
	// facts (set externally via SetPinFloor). Read by the WAL recycle
	// path via PinFloor() and the cluster-wide
	// MinPinAcrossActiveSessions() helper.
	pinFloor uint64

	// barrierAttempt counts BarrierReq attempts made in THIS session.
	// Increments on each call to NextBarrierCut; starts at 0 at
	// StartSession; the first call yields 1.
	//
	// Pre-§IV.2.4 wire change (G0-wire ratified 2026-05-03 to keep
	// targetLSN as compat band; CCS not yet on wire), this counter is
	// the marker-line cutID per architect's Option B (plan §8.2.6):
	// `cut=CCS:<barrierAttempt>` in `barrier prepare` AND
	// `barrier handshake` log lines, providing QA's grep round-trip
	// even before the real CheckpointCutSeq is on the wire. When
	// C-class lands the CCS field on the BarrierReq/Resp payload,
	// this counter's logical value is preserved (only the source of
	// truth shifts from coordinator-local to wire-confirmed).
	barrierAttempt uint64
}

// NewPeerShipCoordinator constructs a fresh coordinator with no active
// peers.
func NewPeerShipCoordinator() *PeerShipCoordinator {
	return &PeerShipCoordinator{states: make(map[ReplicaID]*peerShipState)}
}

// StartSession begins a recover session for the peer.
//
// INV-SINGLE-FLIGHT-PER-REPLICA: returns error if a session is already
// active for this peer. Concurrent attempts are caller's responsibility
// to avoid; the coordinator only enforces the invariant.
//
// fromLSN is the session contract's lower LSN bound — typically
// `Recovery.R + 1` for a catch-up or the engine's pin choice for a
// rebuild. targetLSN is frozen at session start and does not move
// during the session (§2 "session target is fixed at start").
func (c *PeerShipCoordinator) StartSession(id ReplicaID, sessionID, fromLSN, targetLSN uint64) error {
	if sessionID == 0 {
		return errors.New("recovery: sessionID must be non-zero")
	}
	if targetLSN < fromLSN {
		return fmt.Errorf("recovery: targetLSN=%d < fromLSN=%d", targetLSN, fromLSN)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.states[id]; ok && existing.phase != PhaseIdle {
		return fmt.Errorf("recovery: replica %q already has active session=%d phase=%s",
			id, existing.sessionID, existing.phase)
	}
	c.states[id] = &peerShipState{
		phase:      PhaseDrainingHistorical,
		sessionID:  sessionID,
		fromLSN:    fromLSN,
		targetLSN:  targetLSN,
		shipCursor: fromLSN, // anchor cursor at the session's lower bound
		pinFloor:   fromLSN,
	}
	return nil
}

// RecordShipped advances the session's ship cursor to the given LSN
// after a session-lane MsgShipEntry has been successfully written to
// the wire AND acknowledged by the replica's apply path (or, for the
// looser POC, after the wire write). Monotonic: lower LSNs are
// silently ignored.
func (c *PeerShipCoordinator) RecordShipped(id ReplicaID, lsn uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return fmt.Errorf("recovery: RecordShipped on idle peer %q", id)
	}
	if lsn > st.shipCursor {
		st.shipCursor = lsn
	}
	return nil
}

// MarkBaseDone signals that the session's base lane has shipped its
// last block. Required for the SteadyLiveAllowed transition.
func (c *PeerShipCoordinator) MarkBaseDone(id ReplicaID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return fmt.Errorf("recovery: MarkBaseDone on idle peer %q", id)
	}
	st.baseDone = true
	return nil
}

// SetPinFloor advances the per-peer pin floor in response to a replica
// BaseBatchAck fact. Caller (sender's ack reader) supplies BOTH the
// proposed floor AND the primary's current S boundary (retainStart).
//
// Architect ACK on parameter form (vs callback): 2026-04-29
// (docs/recovery-pin-floor-wire.md §11 Resolution 2). Coordinator
// stays substrate-free; caller computes inputs.
//
// Inequality validations (INV-PIN-COMPATIBLE-WITH-RETENTION):
//
//   floor < primarySBoundary → return *Failure(PinUnderRetention)
//                              session must be invalidated, new lineage.
//   floor ≤ st.pinFloor      → silently ignored (monotonic).
//   floor > st.pinFloor      → advance.
//
// Pre-conditions: caller has already validated `floor ≤ walApplied`
// (the inequality (2) check from the spec); this method does NOT
// re-check that because the coordinator does not know walApplied.
func (c *PeerShipCoordinator) SetPinFloor(id ReplicaID, floor, primarySBoundary uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return fmt.Errorf("recovery: SetPinFloor on idle peer %q", id)
	}
	// Monotonic check FIRST. If the new floor wouldn't advance pin
	// anyway, there's no semantic decision to make — no need to
	// validate retention. This avoids spurious PinUnderRetention on
	// early-session acks where receiver's walApplied is still 0
	// while primary's S has already advanced past 0.
	if floor <= st.pinFloor {
		return nil
	}
	// Now we're about to advance: check retention compatibility.
	if primarySBoundary > 0 && floor < primarySBoundary {
		return newFailure(FailurePinUnderRetention, PhasePinUpdate,
			fmt.Errorf("replica %q: floor=%d below primary S=%d", id, floor, primarySBoundary))
	}
	st.pinFloor = floor
	return nil
}

// BacklogDrained evaluates the spec §3.2 predicate:
// shipCursor ≥ frozen targetLSN. Returns false for idle peers.
func (c *PeerShipCoordinator) BacklogDrained(id ReplicaID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return false
	}
	return st.shipCursor >= st.targetLSN
}

// TryAdvanceToSteadyLive attempts the §3.2 transition. Returns true on
// transition, false if any precondition fails. Idempotent.
//
// Preconditions (all required):
//   - phase == DrainingHistorical
//   - BacklogDrained (shipCursor ≥ targetLSN)
//   - baseDone (if the session has a base lane; for catch-up-only
//     sessions the coordinator caller may pre-mark this at start)
func (c *PeerShipCoordinator) TryAdvanceToSteadyLive(id ReplicaID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok {
		return false
	}
	if st.phase != PhaseDrainingHistorical {
		return false
	}
	if st.shipCursor < st.targetLSN {
		return false
	}
	if !st.baseDone {
		return false
	}
	st.phase = PhaseSteadyLiveAllowed
	return true
}

// CanEmitSessionComplete is the §5.2 system-close predicate: given the
// AchievedLSN echoed in the replica's barrier response, may the engine
// emit `SessionClosedCompleted`? True iff achieved ≥ frozen target.
//
// CHK-BARRIER-BEFORE-CLOSE: callers MUST have observed a successful
// barrier round-trip before calling this; it is purely the comparison.
//
// §IV.2.1 / FS-1 / Gate G0 — Tier 1 completion-authority site.
// The `achievedLSN >= st.targetLSN` predicate below is the historic
// recover(a,b) gate; per consensus §I P8, this is NOT the recover(a)
// completion authority. Migration target (per
// `sw-block/design/recover-semantics-adjustment-plan.md` §1 +
// `learn/2026-05-01-recover-target-audit.md` Tier 1) replaces this
// with PrimaryWalLegOk(P) under serializer lock + ReplicaWalWitness
// reporting at the BarrierHandshake cut. AchievedLSN becomes a
// per-cut witness, not a "crossed target" finishing condition. NO
// behavior change pre-Gate G0.
func (c *PeerShipCoordinator) CanEmitSessionComplete(id ReplicaID, achievedLSN uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return false
	}
	return achievedLSN >= st.targetLSN
}

// NextBarrierCut returns the next per-session BarrierReq attempt
// counter for this peer, incrementing in-place. First call after
// StartSession yields 1; subsequent calls within the same session
// increment monotonically. Per architect Option B (plan §8.2.6):
//
//   - Pre-§IV.2.4 wire change, this counter populates the marker
//     line `cut=CCS:<n>` so QA hardware oracle can grep round-trip
//     between `barrier prepare` and `barrier handshake`.
//   - When C-class wire lands CheckpointCutSeq on the
//     BarrierReq/Resp payload, this counter's logical value is
//     preserved; only the source-of-truth shifts.
//
// Returns 0 + error when no active session exists (caller MUST treat
// 0 as invalid and not log a marker with cut=CCS:0).
func (c *PeerShipCoordinator) NextBarrierCut(id ReplicaID) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return 0, fmt.Errorf("recovery: NextBarrierCut on idle peer %q", id)
	}
	st.barrierAttempt++
	return st.barrierAttempt, nil
}

// EndSession tears down the session for a peer (after barrier-ack
// success path or after explicit invalidation). Returns to PhaseIdle;
// pin floor is dropped (recycle gate releases for this peer).
//
// INV-SESSION-TEARDOWN-IS-EXPLICIT: this is the only documented exit
// from non-idle phases; failure paths must also call here.
func (c *PeerShipCoordinator) EndSession(id ReplicaID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.states, id)
}

// RouteLocalWrite tells the WAL shipper which lane a freshly produced
// local write must take for this peer at the moment of the call.
// Spec §3.2 #3 + CHK-NO-FAKE-LIVE-DURING-BACKLOG.
//
// While the session is active (any non-Idle phase), routing is
// RouteSessionLane. SteadyLiveAllowed is a publication-permission
// flag for the engine — it signals "backlog is drained, you may
// publish InSync after barrier" — NOT a routing decision. There is a
// short post-drain pre-barrier window where new local writes still
// must reach the replica via the recover-session connection so the
// barrier's AchievedLSN reflects them; routing them to a steady
// path during that window would either lose them (POC has no
// steady path) or arrive out of order with the session's barrier
// (production). After EndSession returns the peer to Idle, routing
// switches to RouteSteadyLive (§3.3).
//
// Architect ruling on G7-redo Layer-2 review: SteadyLiveAllowed is
// a status flag, not a routing gate.
func (c *PeerShipCoordinator) RouteLocalWrite(id ReplicaID, lsn uint64) LocalWriteRouting {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return RouteSteadyLive
	}
	return RouteSessionLane
}

// Phase returns the current ship phase for this peer (Idle if unknown).
func (c *PeerShipCoordinator) Phase(id ReplicaID) PeerShipPhase {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok {
		return PhaseIdle
	}
	return st.phase
}

// PinFloor returns the per-peer pin floor (0 if no active session).
func (c *PeerShipCoordinator) PinFloor(id ReplicaID) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok || st.phase == PhaseIdle {
		return 0
	}
	return st.pinFloor
}

// MinPinAcrossActiveSessions returns the smallest pinFloor across
// all peers that currently have an active session, or 0 if none.
// The Primary's WAL recycle path consults this to gate truncation
// (INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN).
func (c *PeerShipCoordinator) MinPinAcrossActiveSessions() (floor uint64, anyActive bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	first := true
	for _, st := range c.states {
		if st.phase == PhaseIdle {
			continue
		}
		if first || st.pinFloor < floor {
			floor = st.pinFloor
			first = false
		}
		anyActive = true
	}
	return floor, anyActive
}

// PeerStatus is a diagnostic snapshot for layer-3 / monitoring.
type PeerStatus struct {
	Phase      PeerShipPhase
	SessionID  uint64
	FromLSN    uint64
	TargetLSN  uint64
	ShipCursor uint64
	BaseDone   bool
	PinFloor   uint64
}

func (c *PeerShipCoordinator) Status(id ReplicaID) (PeerStatus, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[id]
	if !ok {
		return PeerStatus{Phase: PhaseIdle}, false
	}
	return PeerStatus{
		Phase:      st.phase,
		SessionID:  st.sessionID,
		FromLSN:    st.fromLSN,
		TargetLSN:  st.targetLSN,
		ShipCursor: st.shipCursor,
		BaseDone:   st.baseDone,
		PinFloor:   st.pinFloor,
	}, true
}
