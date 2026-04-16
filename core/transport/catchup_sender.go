package transport

import (
	"fmt"
	"log"
	"net"
	"sort"
	"time"
)

// StartCatchUp ships entries from the primary to the replica when
// the replica is behind but still within the primary's retained
// window. Runs asynchronously — calls OnSessionClose when done.
//
// This is one of the two byte-movement entry points of the data-sync
// institution; the other is StartRebuild. Both share session
// registration, conn lifecycle, and the barrier exchange, but each
// owns its own ship loop so the two protocols stay distinct.
func (e *BlockExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	lineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		TargetLSN:       targetLSN,
	}
	session, err := e.registerSession(lineage)
	if err != nil {
		return err
	}

	go func() {
		achieved, err := e.doCatchUp(replicaID, session, targetLSN)
		e.finishSession(replicaID, session, achieved, err)
	}()
	return nil
}

// doCatchUp performs the catch-up byte-movement path:
//
//  1. dial the replica
//  2. ship every primary block as MsgShipEntry at the engine's targetLSN
//  3. send MsgBarrierReq, read the typed BarrierResponse back
//
// Lineage travels with each ShipEntry so the replica fails closed on
// stale traffic. The achieved-frontier from the barrier is returned
// verbatim — this method MUST NOT widen the engine's target.
//
// NOTE: today catch-up ships every block in the primary store, not
// just the WAL window. Bounded incremental WAL streaming belongs to
// later execution-lifecycle work (P10) and is intentionally out of
// the data-sync institution's current scope.
func (e *BlockExecutor) doCatchUp(replicaID string, session *activeSession, targetLSN uint64) (uint64, error) {
	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		return 0, fmt.Errorf("catch-up dial: %w", err)
	}
	if err := e.attachConn(session, conn); err != nil {
		_ = conn.Close()
		return 0, err
	}
	defer func() {
		e.detachConn(session, conn)
		_ = conn.Close()
	}()
	e.signalSessionStart(replicaID, session.lineage.SessionID)

	blocks := e.primaryStore.AllBlocks()
	lbas := make([]int, 0, len(blocks))
	for lba := range blocks {
		lbas = append(lbas, int(lba))
	}
	sort.Ints(lbas)
	for _, lbaInt := range lbas {
		select {
		case <-session.cancel:
			return 0, errSessionInvalidated
		default:
		}
		lba := uint32(lbaInt)
		entry := EncodeShipEntry(ShipEntry{
			Lineage: session.lineage,
			LBA:     lba,
			LSN:     targetLSN,
			Data:    blocks[lba],
		})
		if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
			return 0, fmt.Errorf("catch-up set deadline: %w", err)
		}
		if err := WriteMsg(conn, MsgShipEntry, entry); err != nil {
			return 0, fmt.Errorf("catch-up ship LBA %d: %w", lba, err)
		}
		if e.stepDelay > 0 {
			time.Sleep(e.stepDelay)
		}
	}

	select {
	case <-session.cancel:
		return 0, errSessionInvalidated
	default:
	}
	if err := sendBarrierReq(conn, session.lineage, recoveryConnTimeout); err != nil {
		return 0, fmt.Errorf("catch-up barrier: %w", err)
	}
	resp, err := recvBarrierResp(conn, recoveryConnTimeout)
	if err != nil {
		return 0, fmt.Errorf("catch-up barrier resp: %w", err)
	}
	log.Printf("executor: catch-up complete, replica synced to %d", resp.AchievedLSN)
	return resp.AchievedLSN, nil
}
