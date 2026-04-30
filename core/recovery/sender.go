package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Sender is the primary-side driver of one rebuild session. It runs
// the base lane and the WAL lane on top of the same primary WAL via
// a single forward cursor: the cursor is initialized to fromLSN at
// session start (the one rewind), advances monotonically per
// ScanLBAs callback, exits when caught up with primary head AND an
// idle window has elapsed.
//
// Per v3-recovery-unified-wal-stream-kickoff.md (architect 2026-04-29):
// 单队列交织 = [pinLSN, head) 上的滑动光标，≠ 两段物理 phase. The earlier
// POC's two-phase shape (streamBacklog → drainAndSeal → SessionLive)
// is gone; sender now ships from one source — the primary's WAL —
// from fromLSN forward to head. The Kind byte (Backlog / SessionLive)
// flips once when the cursor catches up with head; this is
// observability only, not a state machine boundary.
//
// Sender owns:
//   - The wire `conn` writer side (frame writes serialize through `writerMu`).
//
// Sender borrows:
//   - `primaryStore` for reads (base + WAL pump).
//   - `coordinator` for ship-cursor recording and routing decisions.
//
// Sender does NOT own: receiver-side state, retry policy, lineage
// minting, or session lifecycle decisions (those come from upstream).
type Sender struct {
	primaryStore storage.LogicalStorage
	coordinator  *PeerShipCoordinator
	conn         io.ReadWriter
	replicaID    ReplicaID

	// Session contract — set at Run() entry, immutable thereafter.
	sessionID uint64
	fromLSN   uint64
	targetLSN uint64

	writerMu sync.Mutex // serializes conn writes (frames are atomic)

	// idleWindow guards against barrier-mid-burst: pump exits only if
	// the cursor has caught head AND no new appends arrived for this
	// duration. Default 100ms; configurable via NewSenderWithOptions.
	// Per kickoff §3.2: this is NOT a convergence proof — only barrier
	// + AchievedLSN ≥ targetLSN proves convergence. The idle window is
	// purely a "don't barrier in a millisecond-long lull" guard.
	idleWindow time.Duration

	// barrierRespCh delivers the inbound BarrierResp (or any wire /
	// protocol failure surfaced by the reader goroutine) to the
	// main Run goroutine at barrier-wait time. Buffered=1 so the
	// reader can push exactly once without blocking on a consumer.
	barrierRespCh chan ackOrResult
}

// ackOrResult is the channel payload from the reader goroutine to
// the main Run goroutine: either the achieved LSN from BarrierResp
// or a typed *Failure from any wire / protocol / pin-update error.
type ackOrResult struct {
	achieved uint64
	err      error
}

// kindFlipEpsilon is the LSN distance at which the sender flips
// frameWALEntry's Kind byte from Backlog to SessionLive (kickoff
// Q1 ratified default = 8). Inline constant; observability-only.
const kindFlipEpsilon uint64 = 8

// defaultIdleWindow is the production default for the pump's exit
// idle guard (kickoff Q11 ratified default).
const defaultIdleWindow = 100 * time.Millisecond

// NewSender constructs a sender bound to a primary store + coordinator.
// `conn` is the wire — typically a *net.TCPConn in production, or a
// net.Pipe end in tests. Sender does not close `conn`; caller does.
func NewSender(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID) *Sender {
	return NewSenderWithOptions(primaryStore, coordinator, conn, replicaID, defaultIdleWindow)
}

// NewSenderWithOptions constructs a sender with an explicit idle-window
// duration (test convenience for tighter timing).
func NewSenderWithOptions(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID, idleWindow time.Duration) *Sender {
	if idleWindow <= 0 {
		idleWindow = defaultIdleWindow
	}
	return &Sender{
		primaryStore:  primaryStore,
		coordinator:   coordinator,
		conn:          conn,
		replicaID:     replicaID,
		idleWindow:    idleWindow,
		barrierRespCh: make(chan ackOrResult, 1),
	}
}

// Run drives the entire session end-to-end and blocks until barrier
// ack returns. Returns the AchievedLSN reported by the replica on
// success; returns an error on any wire / session failure.
//
// PRECONDITION: caller already invoked coord.StartSession.
//
// Sequence (post-§3.2 #3):
//  1. Send frameSessionStart.
//  2. Spawn readerLoop (inbound demux for BaseBatchAck / BarrierResp).
//  3. Base lane: ship every LBA's current data via frameBaseBlock.
//  4. Send frameBaseDone; coordinator.MarkBaseDone.
//  5. WAL pump: streamUntilHead — cursor walks (fromLSN, head] with
//     monotonic LSN, kind flips Backlog→SessionLive once at catch-up,
//     exits when cursor == head AND idleWindow elapsed.
//  6. Send frameBarrierReq, await frameBarrierResp.
//  7. Verify achieved ≥ targetLSN via coordinator.CanEmitSessionComplete.
//
// Defer: coordinator.EndSession.
//
// Caller MUST call Run exactly once per Sender instance.
//
// Caller MUST have already invoked
// `coordinator.StartSession(replicaID, sessionID, fromLSN, targetLSN)`.
//
// Lifecycle / cancellation: pass a Context whose cancellation signals
// "abort this session". A typical pattern is to cancel the context
// when the receiver-side goroutine fails so the sender unblocks.
func (s *Sender) Run(ctx context.Context, sessionID, fromLSN, targetLSN uint64) (achievedLSN uint64, err error) {
	s.sessionID = sessionID
	s.fromLSN = fromLSN
	s.targetLSN = targetLSN

	defer s.coordinator.EndSession(s.replicaID)

	// Step 1: SessionStart frame.
	numBlocks := s.primaryStore.NumBlocks()
	if err := s.writeFrame(frameSessionStart, encodeSessionStart(sessionStartPayload{
		SessionID: sessionID, FromLSN: fromLSN, TargetLSN: targetLSN, NumBlocks: numBlocks,
	})); err != nil {
		return 0, newFailure(FailureWire, PhaseSendStart, err)
	}

	// Step 2: spawn the inbound demux reader.
	go s.readerLoop()

	// Step 3: base lane.
	if err := s.streamBase(numBlocks); err != nil {
		return 0, err
	}

	// Step 4: BaseDone signal.
	if err := s.writeFrame(frameBaseDone, nil); err != nil {
		return 0, newFailure(FailureWire, PhaseBaseDone, err)
	}
	if err := s.coordinator.MarkBaseDone(s.replicaID); err != nil {
		return 0, newFailure(FailureContract, PhaseBaseDone, err)
	}

	// Step 5: WAL pump — rewind to fromLSN, walk forward to head + idle.
	if err := s.streamUntilHead(ctx); err != nil {
		return 0, err
	}

	// Step 6: barrier request.
	if err := s.writeFrame(frameBarrierReq, nil); err != nil {
		return 0, newFailure(FailureWire, PhaseBarrierReq, err)
	}
	var achieved uint64
	select {
	case res := <-s.barrierRespCh:
		if res.err != nil {
			return 0, res.err
		}
		achieved = res.achieved
	case <-ctx.Done():
		return 0, newFailure(FailureCancelled, PhaseBarrierResp, ctx.Err())
	}

	// Step 7: convergence check.
	if !s.coordinator.CanEmitSessionComplete(s.replicaID, achieved) {
		return achieved, newFailure(FailureContract, PhaseBarrierResp,
			fmt.Errorf("achieved=%d < target=%d", achieved, targetLSN))
	}
	return achieved, nil
}

// readerLoop is the sender's inbound demux. Reads frames until
// BarrierResp arrives or any wire / protocol / pin failure surfaces.
//
// BaseBatchAck handling:
//  1. Decode payload.
//  2. Validate SessionID matches the active session.
//  3. Fetch primary's S boundary.
//  4. Call coord.SetPinFloor(replicaID, AcknowledgedLSN, S).
//  5. On error (incl. PinUnderRetention), abort.
//
// Single-shot: pushes exactly one ackOrResult to barrierRespCh.
func (s *Sender) readerLoop() {
	for {
		ft, payload, err := readFrame(s.conn)
		if err != nil {
			s.barrierRespCh <- ackOrResult{err: newFailure(FailureWire, PhaseBarrierResp, err)}
			return
		}
		switch ft {
		case frameBaseBatchAck:
			p, decErr := decodeBaseBatchAck(payload)
			if decErr != nil {
				s.barrierRespCh <- ackOrResult{err: newFailure(FailureProtocol, PhasePinUpdate, decErr)}
				return
			}
			if p.SessionID != s.sessionID {
				s.barrierRespCh <- ackOrResult{err: newFailure(FailureProtocol, PhasePinUpdate,
					fmt.Errorf("ack sessionID=%d != active=%d", p.SessionID, s.sessionID))}
				return
			}
			_, primaryS, _ := s.primaryStore.Boundaries()
			if updateErr := s.coordinator.SetPinFloor(s.replicaID, p.AcknowledgedLSN, primaryS); updateErr != nil {
				s.barrierRespCh <- ackOrResult{err: updateErr}
				return
			}
			// continue reading
		case frameBarrierResp:
			achieved, decErr := decodeBarrierResp(payload)
			if decErr != nil {
				s.barrierRespCh <- ackOrResult{err: newFailure(FailureProtocol, PhaseBarrierResp, decErr)}
				return
			}
			s.barrierRespCh <- ackOrResult{achieved: achieved}
			return
		default:
			s.barrierRespCh <- ackOrResult{err: newFailure(FailureProtocol, PhaseBarrierResp,
				fmt.Errorf("unexpected frame type %d", ft))}
			return
		}
	}
}

// streamBase sends every LBA's current data as a base block. Dense
// for POC; sparse omit is a later optimization.
func (s *Sender) streamBase(numBlocks uint32) error {
	for lba := uint32(0); lba < numBlocks; lba++ {
		data, err := s.primaryStore.Read(lba)
		if err != nil {
			return newFailure(FailureSubstrate, PhaseBaseLane,
				fmt.Errorf("read lba=%d: %w", lba, err))
		}
		if err := s.writeFrame(frameBaseBlock, encodeBaseBlock(lba, data)); err != nil {
			return newFailure(FailureWire, PhaseBaseLane, err)
		}
	}
	return nil
}

// streamUntilHead is the §3.2 #3 unified WAL pump: one cursor that
// rewinds to fromLSN at session start, walks forward via ScanLBAs,
// kind-flips Backlog→SessionLive once at catch-up, exits on
// (cursor caught head AND idleWindow elapsed).
//
// Per v3-recovery-unified-wal-stream-kickoff.md §3.1.
//
// Termination conditions:
//   - cursor == head AND no append in idleWindow → return nil (barrier ready)
//   - ctx cancelled → FailureCancelled
//   - ScanLBAs returns ErrWALRecycled → FailureWALRecycled
//   - ScanLBAs returns other error → FailureSubstrate
//   - frame write fails → FailureWire
//   - coord.RecordShipped fails → FailureContract
func (s *Sender) streamUntilHead(ctx context.Context) error {
	cursor := s.fromLSN
	kind := WALKindBacklog

	for {
		// Pump one scan cycle from the current cursor.
		var wireErr, contractErr error
		scanErr := s.primaryStore.ScanLBAs(cursor, func(e storage.RecoveryEntry) error {
			// fromLSN is exclusive — substrate may emit an entry at
			// LSN == cursor (when cursor was set to that LSN by a
			// prior callback or initialized at fromLSN). Skip.
			if e.LSN <= cursor {
				return nil
			}
			if err := s.writeFrame(frameWALEntry, encodeWALEntry(kind, e.LBA, e.LSN, e.Data)); err != nil {
				wireErr = err
				return err
			}
			cursor = e.LSN
			if err := s.coordinator.RecordShipped(s.replicaID, cursor); err != nil {
				contractErr = err
				return err
			}
			// Kind flip: monotonic, one-way (Backlog → SessionLive).
			if kind == WALKindBacklog {
				_, _, head := s.primaryStore.Boundaries()
				if head > cursor && head-cursor < kindFlipEpsilon {
					kind = WALKindSessionLive
				} else if head <= cursor {
					// Already caught up at this entry; flip immediately.
					kind = WALKindSessionLive
				}
			}
			return nil
		})
		if wireErr != nil {
			return newFailure(FailureWire, PhaseBacklog, wireErr)
		}
		if contractErr != nil {
			return newFailure(FailureContract, PhaseBacklog, contractErr)
		}
		if scanErr != nil {
			if errors.Is(scanErr, storage.ErrWALRecycled) {
				return newFailure(FailureWALRecycled, PhaseBacklog, scanErr)
			}
			return newFailure(FailureSubstrate, PhaseBacklog, scanErr)
		}

		// Scan exhausted at this snapshot. Decide: loop again or barrier?
		_, _, head := s.primaryStore.Boundaries()
		if cursor < head {
			// More entries appeared mid-scan. Loop immediately —
			// don't sleep before re-scanning a known-non-empty range.
			continue
		}
		// cursor == head: caught up at this moment. Verify idle window.
		select {
		case <-time.After(s.idleWindow):
			_, _, headAgain := s.primaryStore.Boundaries()
			if cursor == headAgain {
				return nil // barrier ready
			}
			// head moved during idle window; loop again.
		case <-ctx.Done():
			return newFailure(FailureCancelled, PhaseBacklog, ctx.Err())
		}
	}
}

func (s *Sender) writeFrame(t frameType, payload []byte) error {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	return writeFrame(s.conn, t, payload)
}
