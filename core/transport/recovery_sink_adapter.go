package transport

import (
	"context"
	"errors"
	"net"
	"sync"
)

// RecoverySink is the transport-side adapter that satisfies
// recovery.WalShipperSink by wrapping a per-replica WalShipper
// in a recovery-session bracket.
//
// Architect P1 review (2026-04-29) integration rules — this
// adapter is the layer that enforces them:
//
//  1. The sink's emit context (conn + lineage) MUST be set BEFORE
//     the underlying WalShipper transitions to Backlog mode (i.e.,
//     before StartSession). Otherwise EmitFunc would emit under
//     stale lineage during the backlog drain.
//  2. After EndSession returns the WalShipper to Realtime, the
//     emit context MUST be RESTORED to the steady-state value.
//     Otherwise the next steady-state Ship would emit under the
//     recovery session's lineage.
//
// This adapter does NOT pick a wire-body format. It inherits
// whatever EmitFunc the executor's WalShipperFor wired in (today:
// MsgShipEntry via EncodeShipEntry, set in P1). P2d resolves the
// dual-lane format question; this adapter survives that decision
// either unchanged (if the answer is "unify on MsgShipEntry") or
// via a swap of the underlying shipper construction in the
// executor (if the answer is "use frameWALEntry on the dual-lane
// port" or an envelope tag). Tests pin lifecycle ordering only.
//
// The adapter is safe to construct multiple times against the
// same (executor, replicaID) — WalShipperFor is idempotent and
// returns the same shipper instance to all callers (INV-SINGLE
// per spec §3).
type RecoverySink struct {
	e         *BlockExecutor
	replicaID string

	// Session-context: applied BEFORE StartSession (rule 1).
	// P2d (architect 2026-04-30): sessionProfile is fixed at
	// EmitProfileDualLaneWALFrame in production — recovery sessions
	// always emit on the dual-lane port using frameWALEntry.
	sessionConn    net.Conn
	sessionLineage RecoveryLineage
	sessionProfile EmitProfile

	// Pre-session / steady context: restored AFTER EndSession
	// (rule 2). nil conn / zero lineage / SteadyMsgShip profile is
	// honest — "no steady emit context active before this session".
	steadyConn    net.Conn
	steadyLineage RecoveryLineage
	steadyProfile EmitProfile

	// sealMu protects sealed and serializes NotifyAppend's check-
	// then-delegate against EndSession. After EndSession sets
	// sealed=true, NotifyAppend returns ErrSinkSealed instead of
	// delegating to WalShipper — matches senderBacklogSink's
	// post-flushAndSeal contract so callers (PrimaryBridge.PushLiveWrite)
	// get a clean "session is closing" signal during the brief
	// window between sink-end and bridge-map-removal.
	sealMu sync.Mutex
	sealed bool
}

// ErrSinkSealed is returned by RecoverySink.NotifyAppend after the
// session's EndSession has fired. Callers (PushLiveWrite) treat this
// as "session is closing — fall back to steady-live or retry on a
// fresh session". Mirrors the senderBacklogSink "sink sealed" error
// so both sink implementations look the same to the caller.
var ErrSinkSealed = errors.New("recovery: NotifyAppend after sink sealed (session is closing)")

// NewRecoverySink constructs a RecoverySink bound to the
// executor's per-replica WalShipper. Caller provides:
//
//   - sessionConn / sessionLineage: the emit context to install
//     BEFORE StartSession (the dual-lane port + recovery lineage).
//   - steadyConn / steadyLineage:   the emit context to restore
//     AFTER EndSession (the legacy port + steady lineage; or
//     zero values if there was no steady ship in flight).
//
// NewRecoverySink ensures the WalShipperEntry exists (via
// WalShipperFor) so the emit-context update in StartSession has
// somewhere to land.
//
// Caller obligations:
//
//   - Defer EndSession at the call site that calls StartSession.
//     If StartSession returns an error, the emit context has already
//     been swapped to session — only the deferred EndSession restores
//     steady. (recovery.Sender.Run defers sink.EndSession; production
//     callers using NewRecoverySink directly must do the same.)
//   - The caller is responsible for snapshotting the steady context
//     correctly. Passing nil sessionConn is a programmer error
//     (Realtime emits during the session would have nowhere to go);
//     passing nil steadyConn is honest only when no steady ship was
//     in flight before this session — the next steady emit will see
//     "no conn" and fail closed at the EmitFunc level.
func NewRecoverySink(
	e *BlockExecutor,
	replicaID string,
	sessionConn net.Conn,
	sessionLineage RecoveryLineage,
	steadyConn net.Conn,
	steadyLineage RecoveryLineage,
) *RecoverySink {
	return NewRecoverySinkWithProfiles(e, replicaID,
		sessionConn, sessionLineage, EmitProfileDualLaneWALFrame,
		steadyConn, steadyLineage, EmitProfileSteadyMsgShip,
	)
}

// NewRecoverySinkWithProfiles is NewRecoverySink with explicit profile
// arguments — for tests that want to exercise non-production
// profile combinations or for future variants where the steady
// profile differs from the legacy default.
func NewRecoverySinkWithProfiles(
	e *BlockExecutor,
	replicaID string,
	sessionConn net.Conn,
	sessionLineage RecoveryLineage,
	sessionProfile EmitProfile,
	steadyConn net.Conn,
	steadyLineage RecoveryLineage,
	steadyProfile EmitProfile,
) *RecoverySink {
	_ = e.WalShipperFor(replicaID) // ensure entry exists
	return &RecoverySink{
		e:              e,
		replicaID:      replicaID,
		sessionConn:    sessionConn,
		sessionLineage: sessionLineage,
		sessionProfile: sessionProfile,
		steadyConn:     steadyConn,
		steadyLineage:  steadyLineage,
		steadyProfile:  steadyProfile,
	}
}

// StartSession installs the session emit context, then transitions
// the underlying WalShipper to Backlog mode (rule 1: context
// BEFORE StartSession). If WalShipper.StartSession errors, the
// emit context has already been updated — caller's defer of
// EndSession will restore it.
func (r *RecoverySink) StartSession(fromLSN uint64) error {
	r.e.updateWalShipperEmitContext(r.replicaID, r.sessionConn, r.sessionLineage, r.sessionProfile)
	return r.e.WalShipperFor(r.replicaID).StartSession(fromLSN)
}

// DrainBacklog delegates to WalShipper.DrainBacklog. Any emits
// from the drain loop run under the session emit context set by
// StartSession.
func (r *RecoverySink) DrainBacklog(ctx context.Context) error {
	return r.e.WalShipperFor(r.replicaID).DrainBacklog(ctx)
}

// EndSession transitions the underlying WalShipper back to
// Realtime FIRST, then restores the steady emit context (rule 2:
// restore AFTER EndSession). The order matters: any in-flight
// emit triggered by the EndSession path completes under session
// context before the restore lands.
func (r *RecoverySink) EndSession() {
	// Set sealed FIRST so any in-flight NotifyAppend that races
	// past the sealMu check sees the sealed=true gate before the
	// WalShipper transitions and emit context is restored. This is
	// the analogue of senderBacklogSink.flushAndSeal's atomic seal
	// boundary.
	r.sealMu.Lock()
	r.sealed = true
	r.sealMu.Unlock()

	r.e.WalShipperFor(r.replicaID).EndSession()
	r.e.updateWalShipperEmitContext(r.replicaID, r.steadyConn, r.steadyLineage, r.steadyProfile)
}

// NotifyAppend delegates to WalShipper.NotifyAppend during the
// session. In Backlog mode the call updates the lag sample; the
// drain loop picks up the entry from substrate. In Realtime mode
// it ships directly under the session emit context (dual-lane port
// + frameWALEntry).
//
// After EndSession seals the sink, NotifyAppend returns ErrSinkSealed
// — matches senderBacklogSink's post-flushAndSeal contract so
// callers (PushLiveWrite) get a clean "session is closing" signal.
// Without this, a NotifyAppend racing in after EndSession would hit
// the WalShipper with a steady (nil) emit context and produce a
// confusing "no conn" error.
func (r *RecoverySink) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	r.sealMu.Lock()
	sealed := r.sealed
	r.sealMu.Unlock()
	if sealed {
		return ErrSinkSealed
	}
	return r.e.WalShipperFor(r.replicaID).NotifyAppend(lba, lsn, data)
}

// WriteMu returns the per-replica entry's writeMu so recovery.Sender
// (which writes SessionStart / BaseBlock / BaseDone / BarrierReq on
// the same dual-lane conn) can serialize its writes against the
// WalShipper's EmitFunc writes. Without sharing this mutex, the two
// writers race on conn.Write and can interleave header/payload of
// different frames.
//
// recovery.Sender duck-types this method via an optional sub-interface;
// when present, it uses the returned mutex for its writeFrame in
// place of its own private mutex. Returns nil if no entry exists yet —
// caller falls back to its own mutex.
//
// Implements §6.8 #1 SINGLE-SERIALIZER (mechanical) — one mutex per
// (volume, replicaID) entry serializes all conn writes.
func (r *RecoverySink) WriteMu() *sync.Mutex {
	return r.e.WalShipperWriteMu(r.replicaID)
}

// SetPostEmitHook installs the callback recovery.Sender provides at
// session start: typically `func(lsn uint64) { coord.RecordShipped(replicaID, lsn) }`.
// Fires after each successful WalShipper emit (Backlog scan or Live
// NotifyAppend) so per-peer shipCursor advances in lockstep with
// emit.
//
// recovery.Sender duck-types this method via an optional sub-interface;
// when present, it calls SetPostEmitHook at session start (with the
// RecordShipped callback) and SetPostEmitHook(nil) at session end so
// post-session emits don't call into a torn-down coord state.
func (r *RecoverySink) SetPostEmitHook(hook func(lsn uint64)) {
	r.e.SetWalShipperPostEmitHook(r.replicaID, hook)
}

// ProbeBarrierEligibility surfaces the §IV.2.1 PrimaryWalLegOk
// observation tuple from the per-replica WalShipper, taken under the
// shipper's serializer lock so the snapshot is consistent. Returns
// (false, false, false, 0, 0) when no shipper exists for the replica
// (defensive — shouldn't happen on a live session but the executor's
// registry is map-backed so a probe before WalShipperFor is theoretically
// possible).
//
// recovery.Sender's barrierEligibilityProbe interface duck-types this
// method; the dual-lane production path uses it to populate the
// `barrier prepare` / `barrier handshake` markers per plan §8.2.6.
// The bridging-sink path (senderBacklogSink) does NOT implement this
// interface — pre-§IV.2.1 markers are dual-lane-only.
//
// Slice marker-only stage: probe is observation, not gating.
func (r *RecoverySink) ProbeBarrierEligibility() (debtZero, liveTail, walLegOk bool, cursor, head uint64) {
	shipper := r.e.WalShipperFor(r.replicaID)
	if shipper == nil {
		return false, false, false, 0, 0
	}
	return shipper.ProbeBarrierEligibility()
}
