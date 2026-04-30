package transport

import (
	"context"
	"net"
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
	sessionConn    net.Conn
	sessionLineage RecoveryLineage

	// Pre-session / steady context: restored AFTER EndSession
	// (rule 2). nil conn / zero lineage is honest — it just
	// means "no steady emit context active before this session".
	steadyConn    net.Conn
	steadyLineage RecoveryLineage
}

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
	_ = e.WalShipperFor(replicaID) // ensure entry exists
	return &RecoverySink{
		e:              e,
		replicaID:      replicaID,
		sessionConn:    sessionConn,
		sessionLineage: sessionLineage,
		steadyConn:     steadyConn,
		steadyLineage:  steadyLineage,
	}
}

// StartSession installs the session emit context, then transitions
// the underlying WalShipper to Backlog mode (rule 1: context
// BEFORE StartSession). If WalShipper.StartSession errors, the
// emit context has already been updated — caller's defer of
// EndSession will restore it.
func (r *RecoverySink) StartSession(fromLSN uint64) error {
	r.e.updateWalShipperEmitContext(r.replicaID, r.sessionConn, r.sessionLineage)
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
	r.e.WalShipperFor(r.replicaID).EndSession()
	r.e.updateWalShipperEmitContext(r.replicaID, r.steadyConn, r.steadyLineage)
}

// NotifyAppend delegates to WalShipper.NotifyAppend. In Backlog
// mode (during the session) the call updates the lag sample;
// the actual emit happens via the drain loop. In Realtime mode
// (after EndSession) it ships directly under whatever emit
// context is current — session context until restore, steady
// context after.
func (r *RecoverySink) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	return r.e.WalShipperFor(r.replicaID).NotifyAppend(lba, lsn, data)
}
