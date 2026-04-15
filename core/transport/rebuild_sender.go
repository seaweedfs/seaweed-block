package transport

import (
	"fmt"
	"log"
	"net"
	"sort"
	"time"
)

// StartRebuild streams every base block from primary to replica when
// the replica is too far behind for a WAL-window catch-up. Runs
// asynchronously — calls OnSessionClose when done.
//
// This is the second byte-movement entry point of the data-sync
// institution. The wire protocol differs from catch-up:
// MsgRebuildBlock per LBA, then a terminal MsgRebuildDone whose
// BarrierResponse carries the replica's actually achieved frontier.
func (e *BlockExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
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
		achieved, err := e.doRebuild(session, targetLSN)
		e.finishSession(replicaID, session, achieved, err)
	}()
	return nil
}

// doRebuild performs the rebuild byte-movement path:
//
//  1. dial the replica
//  2. ship every primary block as MsgRebuildBlock — the replica
//     applies each with lineage.TargetLSN as the LSN, so a future
//     LSN-aware ApplyEntry guard still treats rebuild data as current
//  3. send MsgRebuildDone carrying the lineage; the replica advances
//     its frontier to lineage.TargetLSN, syncs, and replies with a
//     BarrierResponse holding the actually achieved frontier
//
// The achieved frontier is returned verbatim to the engine. The
// executor MUST NOT silently widen lineage.TargetLSN — that would
// violate the engine's contract that recovery completion equals the
// frozen target, not whatever the executor decided to do.
func (e *BlockExecutor) doRebuild(session *activeSession, targetLSN uint64) (uint64, error) {
	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		return 0, fmt.Errorf("rebuild dial: %w", err)
	}
	if err := e.attachConn(session, conn); err != nil {
		_ = conn.Close()
		return 0, err
	}
	defer func() {
		e.detachConn(session, conn)
		_ = conn.Close()
	}()

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
		payload := EncodeRebuildBlock(session.lineage, lba, blocks[lba])
		if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
			return 0, fmt.Errorf("rebuild set deadline: %w", err)
		}
		if err := WriteMsg(conn, MsgRebuildBlock, payload); err != nil {
			return 0, fmt.Errorf("rebuild block LBA %d: %w", lba, err)
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
	if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
		return 0, fmt.Errorf("rebuild set deadline: %w", err)
	}
	if err := WriteMsg(conn, MsgRebuildDone, EncodeLineage(session.lineage)); err != nil {
		return 0, fmt.Errorf("rebuild done: %w", err)
	}
	resp, err := recvBarrierResp(conn, recoveryConnTimeout)
	if err != nil {
		return 0, fmt.Errorf("rebuild ack resp: %w", err)
	}
	log.Printf("executor: rebuild complete, sent %d blocks (targetLSN=%d)", len(blocks), targetLSN)
	return resp.AchievedLSN, nil
}
