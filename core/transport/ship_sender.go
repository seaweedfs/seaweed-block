package transport

import (
	"fmt"
	"log"
	"net"
	"time"
)

// shipWriteDeadline bounds Ship's TCP write so a dead replica can't
// wedge the caller on the ~120s TCP retransmission timeout.
// Matches V2 WALShipper.Ship (wal_shipper.go:240) verbatim.
const shipWriteDeadline = 3 * time.Second

// shipDialTimeout bounds the lazy-dial window inside Ship when an
// already-registered session has no attached conn yet. Matches V2
// WALShipper.ensureDataConn (wal_shipper.go:636) verbatim.
const shipDialTimeout = 3 * time.Second

// Ship sends one WAL entry live to the named replica over the session's
// attached data connection. Fire-and-forget: no per-entry ack at this
// layer — durability closure is T4b concern (Barrier / sync_all).
//
// Session contract: the session for lineage.SessionID MUST already be
// registered (e.g., from a prior Probe / StartCatchUp / StartRebuild /
// future steady-state attach). Ship does NOT auto-register from the
// incoming lineage — doing so would weaken the accepted-lineage model
// that the epoch fence depends on. If the session exists but has no
// attached conn yet, Ship lazy-dials e.replicaAddr and attaches. This
// preserves the V2 transport-muscle lazy-dial seam (wal_shipper.go:632
// ensureDataConn) at the V3 V2-faithful location (§0-B Stability-
// Locality rule).
//
// On dial or write failure, Ship returns error. The peer layer
// (ReplicaPeer.ShipEntry, T4a-3) translates the error to peer-state
// Degraded + Invalidate(reason). This is the architect-revised seam:
// V3 BlockExecutor does not own ReplicaState, so V2's silent
// return-nil-after-markDegraded cannot be literal-ported without a
// concrete executor→peer failure callback — error return is the
// correct V3-layering substitute (see v3-phase-15-t4a-2-g1-v2-read.md
// §6 architect review).
//
// Called by: ReplicaPeer.ShipEntry (T4a-3) on behalf of
// ReplicationVolume.OnLocalWrite fan-out.
// Owns: per-write deadline on the session's conn; lazy dial + attach
// of the session's data conn when missing.
// Borrows: session (by SessionID lookup); data slice — caller retains,
// Ship does not mutate and does not retain past return.
//
// Invariants preserved from V2 WALShipper.Ship:
//   - Lazy dial on missing conn (wal_shipper.go:632-643): same trigger,
//     same 3s DialTimeout; attach-under-mu races resolved by "first
//     winner keeps" semantics.
//   - Epoch-== silent drop (L1 §2.1 invariant #2, wal_shipper.go:217-221):
//     entries whose lineage.Epoch does not match the session's accepted
//     epoch return nil without writing anything to the wire.
//   - 3s write deadline (wal_shipper.go:240): byte-identical bound.
func (e *BlockExecutor) Ship(replicaID string, lineage RecoveryLineage, lba uint32, lsn uint64, data []byte) error {
	e.mu.Lock()
	session, ok := e.sessions[lineage.SessionID]
	if !ok || session == nil {
		e.mu.Unlock()
		return fmt.Errorf("transport: ship: no session for replica=%s sessionID=%d",
			replicaID, lineage.SessionID)
	}
	// Epoch-== fence. V2 wal_shipper.go:217-221 silent drop.
	// Source of epoch differs from V2 (V2: epochFn() callback; V3:
	// session.lineage.Epoch from registered session), but semantic is
	// identical — stale-epoch entries never reach the wire.
	if lineage.Epoch != session.lineage.Epoch {
		sessionEpoch := session.lineage.Epoch
		e.mu.Unlock()
		log.Printf("transport: ship: dropping LSN=%d replica=%s stale epoch %d (session epoch %d)",
			lsn, replicaID, lineage.Epoch, sessionEpoch)
		return nil
	}
	conn := session.conn
	e.mu.Unlock()

	// Lazy dial if the registered session has no conn attached yet.
	// Done outside e.mu so a slow dial doesn't wedge other sessions.
	if conn == nil {
		dialed, err := net.DialTimeout("tcp", e.replicaAddr, shipDialTimeout)
		if err != nil {
			return fmt.Errorf("transport: ship: dial replica=%s sessionID=%d: %w",
				replicaID, lineage.SessionID, err)
		}
		// Re-acquire lock to attach. Handle three races:
		//   1. Session invalidated between release+reacquire → drop our conn.
		//   2. Another Ship already dialed and attached → use theirs, drop ours.
		//   3. We win → install our conn.
		e.mu.Lock()
		current, stillActive := e.sessions[lineage.SessionID]
		switch {
		case !stillActive || current != session:
			e.mu.Unlock()
			_ = dialed.Close()
			return fmt.Errorf("transport: ship: session %d invalidated during dial",
				lineage.SessionID)
		case session.conn != nil:
			conn = session.conn
			e.mu.Unlock()
			_ = dialed.Close()
		default:
			session.conn = dialed
			conn = dialed
			e.mu.Unlock()
		}
	}

	payload := EncodeShipEntry(ShipEntry{
		Lineage: lineage,
		LBA:     lba,
		LSN:     lsn,
		Data:    data,
	})

	_ = conn.SetWriteDeadline(time.Now().Add(shipWriteDeadline))
	err := WriteMsg(conn, MsgShipEntry, payload)
	_ = conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("transport: ship: write failed replica=%s lsn=%d: %w",
			replicaID, lsn, err)
	}
	return nil
}

// HasSession reports whether a session with the given SessionID is
// currently registered on the executor. Diagnostic / test accessor —
// used by core/replication tests to assert session lifecycle (e.g.,
// that a peer's Close invalidates its session). Not intended for
// production flow-control.
//
// Called by: core/replication tests (Opt-3 executor-teardown fence).
// Owns: read-only snapshot under e.mu.
// Borrows: nothing.
func (e *BlockExecutor) HasSession(sessionID uint64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.sessions[sessionID]
	return ok
}

// RegisterLiveShipSession registers a session for steady-state live
// shipping. The session is registered without a conn — Ship will
// lazy-dial replicaAddr on first call. This is the V3 "steady-state
// attach" lifecycle entry point; recovery commands use StartCatchUp
// / StartRebuild instead.
//
// Called by: ReplicaPeer constructor (core/replication, T4a-3).
// Owns: the new activeSession entry in the executor's sessions map.
// Borrows: lineage — caller retains. Caller is responsible for a
// matching InvalidateSession on peer teardown.
func (e *BlockExecutor) RegisterLiveShipSession(lineage RecoveryLineage) error {
	if _, err := e.registerSession(lineage); err != nil {
		return err
	}
	return nil
}

// attachShipSession registers a session + attaches the given conn for
// Ship dispatch. Test-only helper — production code registers sessions
// via the normal lifecycle (RegisterLiveShipSession / StartCatchUp /
// StartRebuild) and relies on Ship's lazy dial for the conn-attach
// step.
func (e *BlockExecutor) attachShipSession(lineage RecoveryLineage, conn net.Conn) error {
	sess, err := e.registerSession(lineage)
	if err != nil {
		return err
	}
	if err := e.attachConn(sess, conn); err != nil {
		return err
	}
	return nil
}

// registerShipSessionForTest is a test-only helper that registers a
// session WITHOUT attaching any conn. Lets tests exercise Ship's
// lazy-dial path against a registered session whose conn is nil.
func (e *BlockExecutor) registerShipSessionForTest(lineage RecoveryLineage) error {
	_, err := e.registerSession(lineage)
	return err
}
