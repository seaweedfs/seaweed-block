package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"time"

	"github.com/seaweedfs/seaweed-block/core/recovery"
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
	// Legacy wrapper: older adapter/engine surfaces only provide one
	// number. Treat it as both the base snapshot pin and the frontier
	// hint. New transport code that has a distinct base pin should call
	// StartRebuildPinned.
	return e.StartRebuildPinned(replicaID, sessionID, epoch, endpointVersion, targetLSN, targetLSN)
}

// StartRebuildPinned is the explicit rebuild entry point: basePinLSN is
// the WAL point through which the base snapshot is considered durable;
// frontierHint is the session lineage's fourth wire slot and compatibility
// band. Keeping them separate prevents the recovery feeder from using a
// close/band value as the WAL rewind point by accident.
func (e *BlockExecutor) StartRebuildPinned(replicaID string, sessionID, epoch, endpointVersion, basePinLSN, frontierHint uint64) error {
	if e.dualLane != nil {
		return e.startRebuildDualLane(replicaID, sessionID, epoch, endpointVersion, basePinLSN, frontierHint)
	}
	if basePinLSN != frontierHint {
		return fmt.Errorf("legacy rebuild path requires basePinLSN == frontierHint (basePinLSN=%d frontierHint=%d)", basePinLSN, frontierHint)
	}
	lineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		FrontierHint:    frontierHint,
		TargetLSN:       frontierHint,
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
	log.Printf("executor: rebuild start replica=%s sessionID=%d epoch=%d EV=%d basePinLSN=%d frontierHint=%d targetLSN=%d",
		replicaID, sessionID, epoch, endpointVersion, basePinLSN, frontierHint, frontierHint)

	go func() {
		achieved, err := e.doRebuild(replicaID, session, frontierHint)
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
// P2d (architect 2026-04-30): the WAL pump routes through the
// resident per-replica WalShipper via a transport.RecoverySink adapter.
// Steady (legacy port) emit context is snapshotted before constructing
// the sink so EndSession's rule-2 restore lands the right values.
// During session, the WalShipper's emit profile is DualLaneWALFrame
// (writes frameWALEntry on the dual-lane conn); after EndSession the
// shipper's emit context is restored to the steady snapshot.
//
// Lifecycle: this method is async (returns immediately after dialing
// + StartRebuildSessionWithSink is kicked off). The bridge's onClose
// callback (set in NewBlockExecutorWithDualLane) fires the executor's
// OnSessionClose when the session goroutine completes — happens
// autonomously: sender.Run barriers as soon as sink.DrainBacklog
// returns (P2c-slice B-2 lifecycle).
func (e *BlockExecutor) startRebuildDualLane(replicaID string, sessionID, epoch, endpointVersion, basePinLSN, frontierHint uint64) error {
	dl := e.dualLane

	// Phase 0 Fix #1 — INV-RID-UNIFIED-PER-SESSION:
	// the engine's arg `replicaID` is the SINGLE source of truth for keying
	// coord / WalShipper / sink / bridge for this session. The
	// construction-time `dl.ReplicaID` exists for the test accessor
	// (DualLanePrimaryBridge) and any cluster-wiring sanity, but it is not
	// re-keyed per session. If the two diverge:
	//
	//   - sink.WriteMu()/post-emit hook keys under arg `replicaID`
	//     → WalShipper writeMu + RecordShipped advance under arg
	//   - bridge.StartRebuildSessionWithSink(... dl.ReplicaID ...) keys
	//     coord.PinFloor under dl.ReplicaID
	//   → PinFloor and shipCursor live under different IDs; the rebuild
	//     never reconciles, but no error fires until much later (frontier
	//     never converges, watchdog times out).
	//
	// Fix: (a) assert agreement at session entry — fail closed before we
	// touch the wire — and (b) thread the arg through to the bridge so
	// keying is uniform from this point on.
	argRID := recovery.ReplicaID(replicaID)
	if dl.ReplicaID != "" && dl.ReplicaID != argRID {
		return fmt.Errorf("dual-lane replicaID drift: arg=%q dl=%q (must agree per INV-RID-UNIFIED-PER-SESSION)",
			replicaID, string(dl.ReplicaID))
	}

	conn, err := net.DialTimeout("tcp", dl.DialAddr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dual-lane dial %s: %w", dl.DialAddr, err)
	}
	// Same observability marker as the legacy path so QA helpers
	// scrape it identically across modes (INV-G6-WALRECYCLE-DISPATCHES-
	// REBUILD).
	log.Printf("executor: rebuild start replica=%s sessionID=%d epoch=%d EV=%d basePinLSN=%d frontierHint=%d targetLSN=%d (dual-lane)",
		replicaID, sessionID, epoch, endpointVersion, basePinLSN, frontierHint, frontierHint)

	// P2d wiring: build the RecoverySink that brackets the resident
	// WalShipper around this session.
	//
	// Snapshot the steady emit context (legacy port + MsgShipEntry)
	// before installing session context, so EndSession's rule-2
	// restore lands the correct values. SnapshotEmitContext returns
	// (nil, zero, SteadyMsgShip) when no Ship has run yet — that's
	// the honest "no steady context to restore" signal.
	steadyConn, steadyLineage, steadyProfile := e.SnapshotEmitContext(replicaID)
	sessionLineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		FrontierHint:    frontierHint,
		TargetLSN:       frontierHint,
	}
	sink := NewRecoverySinkWithProfiles(e, replicaID,
		conn, sessionLineage, EmitProfileDualLaneWALFrame,
		steadyConn, steadyLineage, steadyProfile,
	)

	// basePinLSN is the snapshot pin — the LSN through which the base
	// lane's extent snapshot is durable. WAL drain ships entries after
	// this pin. frontierHint is carried in the lineage for compatibility
	// and diagnostics, but it is not allowed to silently pick the WAL
	// rewind point.
	//
	// Without this translation, passing fromLSN=0 to WalShipper.StartSession
	// makes DrainBacklog's ScanLBAs(0) hit the recycle gate
	// (`fromLSN <= floor=checkpointLSN`) and fail-fast — wal goroutine
	// errors before barrier emission, hardware silently stalls.
	// Hardware-validated 2026-05-02 (g7-debug trail captured the
	// recycle error pre-fix).
	//
	sessionFromLSN := basePinLSN
	log.Printf("g7-debug: startRebuildDualLane calling Bridge.StartRebuildSessionWithSink replica=%s sessionID=%d basePinLSN=%d frontierHint=%d", replicaID, sessionID, sessionFromLSN, frontierHint)
	if err := dl.Bridge.StartRebuildSessionWithSink(
		context.Background(), conn, argRID, sessionID, sessionFromLSN, frontierHint, sink,
	); err != nil {
		log.Printf("g7-debug: startRebuildDualLane Bridge.StartRebuildSessionWithSink err replica=%s err=%v", replicaID, err)
		_ = conn.Close()
		return fmt.Errorf("dual-lane start: %w", err)
	}
	log.Printf("g7-debug: startRebuildDualLane returned ok replica=%s sessionID=%d", replicaID, sessionID)
	return nil
}
