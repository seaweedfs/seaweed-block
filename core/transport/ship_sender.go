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

// Ship sends one WAL entry live to the named replica over the session's
// attached data connection. Fire-and-forget: no per-entry ack at this
// layer — durability closure is T4b concern (Barrier / sync_all).
//
// The session for lineage.SessionID must already be registered and have
// an attached conn (via StartCatchUp / StartRebuild / future steady-state
// attach). Ship does NOT lazy-dial — by design, per v3-phase-15-t4a-2-
// g1-v2-read.md §6.1: V3 lifts dial responsibility to the peer layer
// (ReplicaPeer.ShipEntry), preserving V2's transparent-reconnect property
// via "degrade + catch-up" at the caller, not inside Ship.
//
// Called by: ReplicaPeer.ShipEntry (T4a-3) on behalf of
// ReplicationVolume.OnLocalWrite fan-out.
// Owns: per-write deadline on the session's conn; nothing persistent.
// Borrows: session (by SessionID lookup); data slice — caller retains,
// Ship does not mutate and does not retain past return.
//
// Invariants preserved from V2 WALShipper.Ship:
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

	if conn == nil {
		return fmt.Errorf("transport: ship: session %d for replica=%s has no attached conn",
			lineage.SessionID, replicaID)
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

// attachShipSession registers a session + attaches the given conn for
// Ship dispatch. Exposed only to tests and to ReplicaPeer (T4a-3) via
// the executor's session lifecycle. Kept unexported; higher layers call
// StartCatchUp/StartRebuild or (future) a dedicated steady-state attach
// method to establish sessions.
//
// This helper lets tests drive Ship without going through StartCatchUp
// (which runs a streaming catch-up that's not what we want to test).
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
