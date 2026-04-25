package transport

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
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

// doCatchUp performs the T4c-2 catch-up muscle path:
//
//  1. dial the replica
//  2. stream the substrate's retained-WAL window via ScanLBAs starting
//     at the replica's flushed frontier; each entry ships as
//     MsgShipEntry carrying the engine's targetLSN
//  3. send MsgBarrierReq + read typed BarrierResponse — the barrier's
//     AchievedLSN is the caller-facing "did we reach target" answer
//     (per G-1 §4.2: barrier-as-terminator collapses V2's separate
//     MsgCatchupDone marker into the existing barrier response, with
//     INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-ACHIEVED-LSN inscribed)
//
// Substrate sub-mode is reported via storage.RecoveryMode (memo §5.1)
// in the success log line so operators can tell at a glance whether
// catch-up ran wal_replay (walstore) or state_convergence (smartwal).
//
// Error classes (G-1 §3 row #8 + INV-REPL-CATCHUP-TARGET-NOT-REACHED-
// VS-RECYCLED-DISTINGUISHED):
//   - storage.ErrWALRecycled wrapped → tier-class change; engine
//     SessionClose handler maps to peer NeedsRebuild (G-1 §4.3
//     Option B)
//   - target-not-reached after clean scan → stream-level error;
//     engine retries until RecoveryRuntimePolicy.MaxRetries (G-1 §4.1)
//   - other stream errors → same retry path
//
// Hidden invariants honored:
//   - INV-REPL-CATCHUP-CALLBACK-RETURN-NIL-CONTINUES — callback skips
//     entries past targetLSN by `return nil`, not by erroring out
//   - INV-REPL-CATCHUP-LASTSENT-MONOTONIC — `lastSent` only advances
//     on successful frame write, so partial-progress achievedLSN is
//     surfaceable to the caller
//   - INV-REPL-CATCHUP-DEADLINE-PER-CALL-SCOPE — conn deadline is set
//     at entry, cleared at exit; subsequent steady-state ships start
//     fresh
//
// Called by: BlockExecutor.StartCatchUp goroutine after registerSession
// returns. Owns: per-call conn deadline; ScanLBAs callback closure.
// Borrows: primaryStore (substrate handle for ScanLBAs); session
// (cancel channel + lineage).
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
		// Clear the deadline so subsequent steady-state ships on the
		// same conn don't inherit it — INV-REPL-CATCHUP-DEADLINE-PER-
		// CALL-SCOPE.
		_ = conn.SetDeadline(time.Time{})
		_ = conn.Close()
	}()
	e.signalSessionStart(replicaID, session.lineage.SessionID)

	// Per-call deadline. T4c-2 sources from RecoveryRuntimePolicy in
	// the post-pre-B path; for the legacy StartCatchUp wrapper path
	// we use recoveryConnTimeout as the documented default.
	if err := conn.SetDeadline(time.Now().Add(recoveryConnTimeout)); err != nil {
		return 0, fmt.Errorf("catch-up set deadline: %w", err)
	}

	// Replica's flushed LSN is what catch-up streams from. The
	// scan starts at fromLSN+1 (entries STRICTLY AFTER the replica's
	// frontier). T4b probe handshake delivered the replica's R; the
	// engine's StartCatchUp command embeds H as the ship target.
	// V2 used `runCatchUpTo(replicaFlushedLSN, targetLSN)` (line 845);
	// V3's executor doesn't yet thread replicaFlushedLSN through
	// StartCatchUp — for muscle parity, we scan from 0 and let the
	// substrate decide what's retained (BlockStore returns nothing
	// at fromLSN >= H; walstore + smartwal return ErrWALRecycled at
	// retention boundary; substrate-internal filtering handles the
	// rest). This works because the replica's `acceptMutationLineage`
	// is idempotent on already-applied LSNs. T4c-3 integration tests
	// will cover the replicaFlushedLSN threading at the StartCatchUp
	// command boundary if needed.
	var lastSent uint64
	scanErr := e.primaryStore.ScanLBAs(0, func(entry storage.RecoveryEntry) error {
		select {
		case <-session.cancel:
			return errSessionInvalidated
		default:
		}
		// Per-entry target cap: skip entries past targetLSN by
		// returning nil (continues the scan) — INV-REPL-CATCHUP-
		// CALLBACK-RETURN-NIL-CONTINUES.
		if targetLSN > 0 && entry.LSN > targetLSN {
			return nil
		}
		shipPayload := EncodeShipEntry(ShipEntry{
			Lineage: session.lineage,
			LBA:     entry.LBA,
			LSN:     entry.LSN,
			Data:    entry.Data,
		})
		if err := WriteMsg(conn, MsgShipEntry, shipPayload); err != nil {
			return fmt.Errorf("ship LBA %d: %w", entry.LBA, err)
		}
		// Only advance lastSent AFTER the write succeeds —
		// INV-REPL-CATCHUP-LASTSENT-MONOTONIC.
		lastSent = entry.LSN
		if e.stepDelay > 0 {
			time.Sleep(e.stepDelay)
		}
		return nil
	})

	if scanErr != nil {
		// Distinguish error classes — INV-REPL-CATCHUP-TARGET-NOT-
		// REACHED-VS-RECYCLED-DISTINGUISHED. ErrWALRecycled is a
		// tier-class change; other errors are stream-level.
		if errors.Is(scanErr, storage.ErrWALRecycled) {
			return lastSent, fmt.Errorf("catch-up: WAL recycled: %w", scanErr)
		}
		if errors.Is(scanErr, errSessionInvalidated) {
			return lastSent, errSessionInvalidated
		}
		return lastSent, fmt.Errorf("catch-up: stream error: %w", scanErr)
	}

	select {
	case <-session.cancel:
		return lastSent, errSessionInvalidated
	default:
	}
	if err := sendBarrierReq(conn, session.lineage, recoveryConnTimeout); err != nil {
		return lastSent, fmt.Errorf("catch-up barrier: %w", err)
	}
	resp, err := recvBarrierResp(conn, recoveryConnTimeout)
	if err != nil {
		return lastSent, fmt.Errorf("catch-up barrier resp: %w", err)
	}

	// Barrier-as-terminator (G-1 §4.2 architect Option B):
	// `BarrierResponse.AchievedLSN` carries the same information
	// V2's MsgCatchupDone marker provided. INV-REPL-CATCHUP-COMPLETION-
	// FROM-BARRIER-ACHIEVED-LSN: completion is determined by comparing
	// `resp.AchievedLSN` against `targetLSN`; partial-progress is
	// surfaceable via the returned (lastSent, error) tuple.
	if targetLSN > 0 && resp.AchievedLSN < targetLSN {
		return lastSent, fmt.Errorf("catch-up: target %d not reached (achieved=%d)",
			targetLSN, resp.AchievedLSN)
	}

	// Mode label surfacing (memo §5.1). For now we report based on
	// substrate type via a runtime check; T4c-3 integration tests
	// will cover both substrate sub-modes through the matrix.
	mode := storage.RecoveryModeStateConvergence
	if _, ok := e.primaryStore.(interface{ CheckpointLSN() uint64 }); ok {
		// walstore exposes CheckpointLSN — distinguishes it from
		// BlockStore + smartwal. Provisional probe; T4c-3 will
		// migrate this to a substrate-reported mode method.
		mode = storage.RecoveryModeWALReplay
	}
	log.Printf("executor: catch-up complete replica=%s recovery_mode=%s achieved=%d target=%d last_sent=%d",
		replicaID, mode, resp.AchievedLSN, targetLSN, lastSent)
	return resp.AchievedLSN, nil
}
