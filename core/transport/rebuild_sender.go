package transport

import (
	"context"
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
	if e.dualLane != nil {
		return e.startRebuildDualLane(replicaID, sessionID, epoch, endpointVersion, targetLSN)
	}
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

	// G6 §2 #5 observability: emit a deterministic START marker BEFORE
	// the goroutine spawns so QA's wait_until_rebuild_dispatched
	// helper has a fixed log string to scrape against — independent
	// of how long the actual rebuild takes on the wire (the
	// pre-existing "executor: rebuild complete" line only fires
	// AFTER completion, which the G5-5C scenario D 5s scrape window
	// missed for sustained-write workloads).
	//
	// Pinned by: INV-G6-WALRECYCLE-DISPATCHES-REBUILD (test pointer)
	// + QA `wait_until_rebuild_dispatched` helper (D-scenario harness).
	log.Printf("executor: rebuild start replica=%s sessionID=%d epoch=%d EV=%d targetLSN=%d",
		replicaID, sessionID, epoch, endpointVersion, targetLSN)

	go func() {
		achieved, err := e.doRebuild(replicaID, session, targetLSN)
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
func (e *BlockExecutor) doRebuild(replicaID string, session *activeSession, targetLSN uint64) (uint64, error) {
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

// startRebuildDualLane is the wiring entry into the dual-lane recovery
// package. It dials the replica's separate dual-lane port (per
// docs/recovery-wiring-plan.md §4 Option A) and delegates to the
// PrimaryBridge captured in e.dualLane.Bridge.
//
// Lifecycle: this method is async (returns immediately after dialing
// + StartRebuildSession is kicked off). The bridge's onClose callback
// (set in NewBlockExecutorWithDualLane) fires the executor's
// OnSessionClose when the session goroutine completes — which now
// happens autonomously: sender.Run barriers as soon as sink.DrainBacklog
// returns (P2c-slice B-2 lifecycle; no explicit FinishLiveWrites needed).
//
// Legacy semantics — no live writes during rebuild — are preserved by
// the absence of any PushLiveWrite call here. The bridge's WalShipperSink
// buffer remains empty; flushAndSeal ships zero entries; barrier converges.
func (e *BlockExecutor) startRebuildDualLane(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	dl := e.dualLane
	conn, err := net.DialTimeout("tcp", dl.DialAddr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dual-lane dial %s: %w", dl.DialAddr, err)
	}
	// Same observability marker as the legacy path so QA helpers
	// scrape it identically across modes (INV-G6-WALRECYCLE-DISPATCHES-
	// REBUILD).
	log.Printf("executor: rebuild start replica=%s sessionID=%d epoch=%d EV=%d targetLSN=%d (dual-lane)",
		replicaID, sessionID, epoch, endpointVersion, targetLSN)
	// fromLSN=0 for full rebuild matches the legacy doRebuild semantic
	// (base lane covers everything; no catch-up tail). When a future
	// caller wants partial rebuild from a specific LSN, the
	// StartRebuild signature would need fromLSN added — out of scope.
	if err := dl.Bridge.StartRebuildSession(context.Background(), conn, dl.ReplicaID, sessionID, 0, targetLSN); err != nil {
		_ = conn.Close()
		return fmt.Errorf("dual-lane start: %w", err)
	}
	return nil
}
