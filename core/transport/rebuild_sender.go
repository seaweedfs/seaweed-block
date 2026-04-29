package transport

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"sort"
	"time"
)

// streamRebuildBlocks writes MsgRebuildBlock frames for each LBA in `blocks`
// in sorted order. Used twice by doRebuild — once for the initial snapshot,
// once for the delta pass that picks up writes which landed during the
// initial stream.
func streamRebuildBlocks(e *BlockExecutor, conn net.Conn, session *activeSession, blocks map[uint32][]byte) error {
	lbas := make([]int, 0, len(blocks))
	for lba := range blocks {
		lbas = append(lbas, int(lba))
	}
	sort.Ints(lbas)
	for _, lbaInt := range lbas {
		select {
		case <-session.cancel:
			return errSessionInvalidated
		default:
		}
		lba := uint32(lbaInt)
		payload := EncodeRebuildBlock(session.lineage, lba, blocks[lba])
		if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
			return fmt.Errorf("rebuild set deadline: %w", err)
		}
		if err := WriteMsg(conn, MsgRebuildBlock, payload); err != nil {
			return fmt.Errorf("rebuild block LBA %d: %w", lba, err)
		}
		if e.stepDelay > 0 {
			time.Sleep(e.stepDelay)
		}
	}
	return nil
}

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

	// Two-phase rebuild stream — closes the live-ship-during-rebuild gap
	// surfaced by G7 §2 #5 canonical run on da858ea (498 mismatches at
	// concurrent-write LBAs). The replica's lineage gate rejects MsgShipEntry
	// (live-ship lineage) while a rebuild lineage is active, so writes that
	// land in the primary's WAL AFTER the first AllBlocks snapshot have no
	// path onto the replica until rebuild concludes — but the post-rebuild
	// catch-up's lineage also fails the gate. Without a delta pass those
	// LBAs end up zero on the replica.
	//
	// Phase 1 streams the first snapshot; Phase 2 takes a second snapshot
	// at end-of-stream and ships LBAs whose bytes changed (or appeared) in
	// the interim. Bounded under quiescent workloads (which all G7 tests
	// reach because the harness's concurrent-writes phase completes before
	// the verify step). For sustained writes the loop would need to iterate
	// to convergence — out of scope here; documented as carry on the
	// rebuild contract.
	first := e.primaryStore.AllBlocks()
	if err := streamRebuildBlocks(e, conn, session, first); err != nil {
		return 0, err
	}
	second := e.primaryStore.AllBlocks()
	delta := make(map[uint32][]byte)
	for lba, data := range second {
		if firstData, ok := first[lba]; !ok || !bytes.Equal(firstData, data) {
			delta[lba] = data
		}
	}
	if len(delta) > 0 {
		if err := streamRebuildBlocks(e, conn, session, delta); err != nil {
			return 0, err
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
	// Marker contract: hardware harness's `wait_until_rebuild_complete`
	// helper greps for "executor: rebuild complete, sent <n> blocks
	// (targetLSN=<n>)". `<n>` is the total block count across phase 1 +
	// delta. Don't add fields between "blocks" and "(targetLSN=" without
	// updating QA's regex (G7 §harness-notes). Delta size is logged
	// separately for diagnostics.
	totalBlocks := len(first) + len(delta)
	log.Printf("executor: rebuild complete, sent %d blocks (targetLSN=%d)", totalBlocks, targetLSN)
	if len(delta) > 0 {
		log.Printf("executor: rebuild delta-pass picked up %d additional blocks (writes during base-lane stream)", len(delta))
	}
	return resp.AchievedLSN, nil
}
