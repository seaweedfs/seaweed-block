package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Sender is the primary-side driver of one rebuild session. It runs
// the base lane, the backlog lane, and (during the same session)
// accepts post-target live writes through PushLiveWrite, all sharing
// one ordered ship queue per peer (spec §3.2 #3).
//
// POC scope:
//   - Base lane: dense — ships every LBA in [0, NumBlocks()) by reading
//     primary's current state via LogicalStorage.Read. (sparse omit
//     and substrate basement-clearing are a later milestone; see
//     INV-BASE-SPARSE-REQUIRES-SUBSTRATE-CLOSURE.)
//   - Backlog lane: replays primary's WAL via ScanLBAs(fromLSN, ...)
//     up to (and including) targetLSN. Entries past targetLSN are
//     skipped at this lane — they are post-target traffic that
//     either rides the same session lane (if injected via
//     PushLiveWrite while DrainingHistorical) or routes to the
//     steady-live path (after coordinator transitions).
//   - Single goroutine drives the wire writer; concurrent producers
//     enqueue items via a mutex-protected slice. No backpressure /
//     bounded buffer in the POC — the sender drives the slice
//     synchronously.
//   - No baseBatchAck pin advancement during the session: pin floor
//     stays at the session's fromLSN until EndSession releases it.
//     Adding incremental pin advancement is a layer-2 enhancement.
//
// Sender owns:
//   - The wire `conn` writer side (frame writes serialize through
//     `writerMu`).
//   - The ordered ship queue (`queue`).
//
// Sender borrows:
//   - `primaryStore` for reads (base + backlog).
//   - `coordinator` for ship-phase transitions and routing decisions.
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

	// queueMu protects the live-write queue + sealed flag. Backlog
	// and base lanes run on the Run() goroutine and write directly
	// without queueing because they are sequential by construction.
	// Live writes from PushLiveWrite append here; the final drain
	// happens AFTER closeCh is closed, atomic with sealing.
	queueMu   sync.Mutex
	liveQueue []walItem
	sealed    bool // set by drainAndSeal; PushLiveWrite errors after this

	// closeCh signals "no more live writes coming; safe to drain
	// queue, transition phase, and run barrier". Caller closes via
	// Sender.Close(). Run blocks on this channel after the backlog
	// scan finishes.
	closeCh   chan struct{}
	closeOnce sync.Once
}

type walItem struct {
	lba  uint32
	lsn  uint64
	data []byte
}

// NewSender constructs a sender bound to a primary store + coordinator.
// `conn` is the wire — typically a *net.TCPConn in production, or a
// net.Pipe end in tests. Sender does not close `conn`; caller does.
func NewSender(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID) *Sender {
	return &Sender{
		primaryStore: primaryStore,
		coordinator:  coordinator,
		conn:         conn,
		replicaID:    replicaID,
		closeCh:      make(chan struct{}),
	}
}

// Close signals the sender that no more PushLiveWrite calls will
// arrive for this session, so it may safely drain the live queue,
// transition phase, and run the barrier round-trip. Idempotent.
//
// Caller pattern:
//
//	go sender.Run(...)
//	// ... PushLiveWrite as the WAL shipper produces local writes ...
//	sender.Close()  // session may now barrier
//	// wait for Run to return; coordinator phase will be Idle
//
// PushLiveWrite calls that race with Close (arrive concurrently) are
// either accepted (queued before drainAndSeal) OR rejected with an
// error (sealed already). The atomic-seal contract guarantees no
// queued write is lost between drain and barrier.
func (s *Sender) Close() {
	s.closeOnce.Do(func() { close(s.closeCh) })
}

// PushLiveWrite hands the sender one local-write event for this peer.
// Caller (the WAL shipper integration) must already have consulted
// `coordinator.RouteLocalWrite(replicaID, lsn)` and confirmed the
// routing is RouteSessionLane; this method does not double-check.
//
// Live writes are buffered until the backlog drain phase finishes,
// then flushed in LSN order before barrier. POC simplification —
// in production the sender would interleave them with backlog by
// LSN order in real time. Spec §3.2 #3 (single ordered queue) is
// preserved at the level of "post-backlog drain, before barrier".
func (s *Sender) PushLiveWrite(lba uint32, lsn uint64, data []byte) error {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.sealed {
		return errors.New("recovery: PushLiveWrite after sender sealed (session is closing)")
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	s.liveQueue = append(s.liveQueue, walItem{lba: lba, lsn: lsn, data: cp})
	return nil
}

// Run drives the entire session end-to-end and blocks until barrier
// ack returns. Returns the AchievedLSN reported by the replica on
// success; returns an error on any wire / session failure.
//
// PRECONDITION: caller already invoked coord.StartSession.
//
// Sequence:
//  1. Send frameSessionStart.
//  2. Base lane: ship every LBA's current data via frameBaseBlock.
//  3. Send frameBaseDone; coordinator.MarkBaseDone.
//  4. Backlog lane: ScanLBAs(fromLSN, ...); ship entries with
//     LSN ∈ (fromLSN, targetLSN] via frameWALEntry; record each
//     shipped LSN via coordinator.RecordShipped.
//  5. Wait for caller's Close() OR ctx cancellation.
//  6. Drain live queue + atomic seal; ship buffered live writes.
//  7. coordinator.TryAdvanceToSteadyLive (§3.2 transition).
//  8. Send frameBarrierReq, read frameBarrierResp.
//  9. Verify achieved ≥ targetLSN via coordinator.CanEmitSessionComplete
//     (§5.2, CHK-BARRIER-BEFORE-CLOSE).
//
// Defer always: set sealed=true; coordinator.EndSession.
//
// Caller MUST call Run exactly once per Sender instance.
//
// Caller MUST have already invoked
// `coordinator.StartSession(replicaID, sessionID, fromLSN, targetLSN)`
// synchronously before spawning the goroutine that calls Run. This
// keeps "session is registered" observable from the call site that
// scheduled Run, so concurrent code reading `coord.RouteLocalWrite`
// after the spawn point sees a non-Idle phase deterministically.
//
// Run's defer always calls coordinator.EndSession, regardless of the
// exit path (success, ctx-cancel, wire-error). Sealed is also set in
// the defer so any post-Run PushLiveWrite gets an explicit error
// instead of silently appending to a dead queue.
//
// Lifecycle / cancellation: pass a Context whose cancellation signals
// "abort this session". A typical pattern is to cancel the context
// when the receiver-side goroutine fails, so a sender blocked on
// closeCh wakes up and returns ctx.Err().
func (s *Sender) Run(ctx context.Context, sessionID, fromLSN, targetLSN uint64) (achievedLSN uint64, err error) {
	s.sessionID = sessionID
	s.fromLSN = fromLSN
	s.targetLSN = targetLSN

	// Defer cleanup runs on every exit path.
	defer func() {
		s.queueMu.Lock()
		s.sealed = true
		s.queueMu.Unlock()
		s.coordinator.EndSession(s.replicaID)
	}()

	// Step 2: SessionStart frame.
	numBlocks := s.primaryStore.NumBlocks()
	if err := s.writeFrame(frameSessionStart, encodeSessionStart(sessionStartPayload{
		SessionID: sessionID, FromLSN: fromLSN, TargetLSN: targetLSN, NumBlocks: numBlocks,
	})); err != nil {
		return 0, newFailure(FailureWire, PhaseSendStart, err)
	}

	// Step 3: base lane.
	if err := s.streamBase(numBlocks); err != nil {
		return 0, err // streamBase wraps its own Failure
	}

	// Step 4: BaseDone signal.
	if err := s.writeFrame(frameBaseDone, nil); err != nil {
		return 0, newFailure(FailureWire, PhaseBaseDone, err)
	}
	if err := s.coordinator.MarkBaseDone(s.replicaID); err != nil {
		return 0, newFailure(FailureContract, PhaseBaseDone, err)
	}

	// Step 5: backlog lane.
	if err := s.streamBacklog(); err != nil {
		return 0, err // streamBacklog wraps its own Failure
	}

	// Step 6: wait for caller's "no more live writes" signal OR ctx
	// cancellation. Until Close() is called, PushLiveWrite continues
	// to accept new entries. This is the dual-lane invariant: while
	// session is active, every primary-side WAL append routes through
	// the session lane, and the sender is the gate that decides when
	// barrier may begin.
	//
	// Ctx cancellation is the abort path: typically fired by the
	// caller when the receiver goroutine fails so the sender unblocks
	// and returns rather than waiting forever.
	select {
	case <-s.closeCh:
	case <-ctx.Done():
		return 0, newFailure(FailureCancelled, PhaseAwaitClose, ctx.Err())
	}

	// Step 7: drain live queue + atomic seal. Any PushLiveWrite that
	// races concurrently is either captured before sealing or
	// rejected with an error (caller sees the error and knows the
	// session is closing). No queued write can be lost between drain
	// and barrier because seal is atomic with the take.
	if err := s.drainAndSeal(); err != nil {
		return 0, err // drainAndSeal wraps its own Failure
	}

	// Step 8: phase transition. Per architect ruling, this is purely
	// a publication-permission flag — RouteLocalWrite still returns
	// SessionLane until EndSession (because the session is still
	// open until barrier completes).
	if !s.coordinator.TryAdvanceToSteadyLive(s.replicaID) {
		// Defensive: if this fails after we've shipped backlog and
		// marked base done, something is structurally wrong.
		st, _ := s.coordinator.Status(s.replicaID)
		return 0, newFailure(FailureContract, PhaseTransition,
			fmt.Errorf("TryAdvanceToSteadyLive rejected: %+v", st))
	}

	// Step 8 + 9: barrier round-trip.
	if err := s.writeFrame(frameBarrierReq, nil); err != nil {
		return 0, newFailure(FailureWire, PhaseBarrierReq, err)
	}
	ft, payload, err := readFrame(s.conn)
	if err != nil {
		return 0, newFailure(FailureWire, PhaseBarrierResp, err)
	}
	if ft != frameBarrierResp {
		return 0, newFailure(FailureProtocol, PhaseBarrierResp,
			fmt.Errorf("expected barrierResp, got frame type %d", ft))
	}
	achieved, err := decodeBarrierResp(payload)
	if err != nil {
		return 0, newFailure(FailureProtocol, PhaseBarrierResp, err)
	}
	if !s.coordinator.CanEmitSessionComplete(s.replicaID, achieved) {
		return achieved, newFailure(FailureContract, PhaseBarrierResp,
			fmt.Errorf("achieved=%d < target=%d", achieved, targetLSN))
	}
	return achieved, nil
}

// streamBase sends every LBA's current data as a base block. Dense
// for POC; sparse omit is a later optimization paired with a
// substrate basement-clearing INV.
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

// streamBacklog replays the primary's WAL window (fromLSN, targetLSN]
// via ScanLBAs and ships each entry on the WAL lane. Records each
// shipped LSN with the coordinator so BacklogDrained becomes true
// once the highest LSN ≤ targetLSN is shipped.
func (s *Sender) streamBacklog() error {
	stop := errors.New("scan stop sentinel")
	var wireErr, contractErr error // surfaced from inside the callback
	scanErr := s.primaryStore.ScanLBAs(s.fromLSN, func(e storage.RecoveryEntry) error {
		// Skip entries past frozen target — those belong on the
		// steady-live path or get fed back via PushLiveWrite if the
		// session is still DrainingHistorical.
		if e.LSN > s.targetLSN {
			return stop
		}
		// fromLSN is exclusive in our contract; ScanLBAs may emit
		// LSN == fromLSN if the substrate's retention starts there.
		// Skip the boundary entry.
		if e.LSN <= s.fromLSN {
			return nil
		}
		if err := s.writeFrame(frameWALEntry, encodeWALEntry(WALKindBacklog, e.LBA, e.LSN, e.Data)); err != nil {
			wireErr = err
			return err
		}
		if err := s.coordinator.RecordShipped(s.replicaID, e.LSN); err != nil {
			contractErr = err
			return err
		}
		return nil
	})
	if wireErr != nil {
		return newFailure(FailureWire, PhaseBacklog, wireErr)
	}
	if contractErr != nil {
		return newFailure(FailureContract, PhaseBacklog, contractErr)
	}
	if scanErr != nil && !errors.Is(scanErr, stop) {
		// ScanLBAs returns storage.ErrWALRecycled when fromLSN is
		// below retention. Tier-class escalation: caller must pick
		// a fresher anchor.
		if errors.Is(scanErr, storage.ErrWALRecycled) {
			return newFailure(FailureWALRecycled, PhaseBacklog, scanErr)
		}
		return newFailure(FailureSubstrate, PhaseBacklog, scanErr)
	}
	return nil
}

// drainAndSeal atomically takes the live queue and forbids further
// PushLiveWrite calls (sealed=true under queueMu), then ships every
// item in LSN order. The atomic take + seal guarantees no concurrent
// PushLiveWrite can land an entry that misses the drain — either the
// push happens before the lock and is captured, or after and gets a
// "sender sealed" error.
func (s *Sender) drainAndSeal() error {
	s.queueMu.Lock()
	queue := s.liveQueue
	s.liveQueue = nil
	s.sealed = true
	s.queueMu.Unlock()

	// Stable sort by LSN ascending.
	for i := 1; i < len(queue); i++ {
		j := i
		for j > 0 && queue[j-1].lsn > queue[j].lsn {
			queue[j-1], queue[j] = queue[j], queue[j-1]
			j--
		}
	}

	for _, item := range queue {
		if err := s.writeFrame(frameWALEntry, encodeWALEntry(WALKindSessionLive, item.lba, item.lsn, item.data)); err != nil {
			return newFailure(FailureWire, PhaseDrainSeal, err)
		}
		if err := s.coordinator.RecordShipped(s.replicaID, item.lsn); err != nil {
			return newFailure(FailureContract, PhaseDrainSeal, err)
		}
	}
	return nil
}

func (s *Sender) writeFrame(t frameType, payload []byte) error {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	return writeFrame(s.conn, t, payload)
}
