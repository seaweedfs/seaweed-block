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

	// barrierRespCh delivers the inbound BarrierResp (or any wire /
	// protocol failure surfaced by the reader goroutine) to the
	// main Run goroutine at barrier-wait time. Buffered=1 so the
	// reader can push exactly once without blocking on a consumer.
	//
	// Per docs/recovery-pin-floor-wire.md §7: the reader goroutine
	// is required because BaseBatchAck arrives mid-stream during
	// base lane while Run is busy writing frames. Pre-#3 there was
	// no reader; only one inline readFrame at barrier time.
	barrierRespCh chan ackOrResult

	// sink — Run() delegates the WAL pump (StartSession / DrainBacklog
	// / EndSession) to this sink. After P2c-slice A sink is mandatory:
	// every Sender has one (transport WalShipper in production, or
	// senderBacklogSink in the bridging path until P2d). Set by
	// NewSenderWithSink / NewSenderWithBacklogRelay.
	sink WalShipperSink
}

// ackOrResult is the channel payload from the reader goroutine to
// the main Run goroutine: either the achieved LSN from BarrierResp
// or a typed *Failure from any wire / protocol / pin-update error.
type ackOrResult struct {
	achieved uint64
	err      error
}

type walItem struct {
	lba  uint32
	lsn  uint64
	data []byte
}

// WalShipperSink is the abstract WAL-pump surface that Sender
// delegates to. Run() invokes the session bracket:
//
//   1. StartSession(fromLSN) — before any WAL emission begins
//   2. DrainBacklog(ctx)     — runs until cursor catches head
//   3. EndSession()          — defer-fired regardless of exit path
//
// Plus the live-write API for RouteSessionLane traffic during the
// session (P2c-slice B-1 — single live path):
//
//   4. NotifyAppend(lba, lsn, data) — primary's WAL append handed
//      to the sink. Sink decides whether to ship directly (production
//      WalShipper Realtime mode) or buffer for ordered drain (bridging
//      senderBacklogSink). Sender.PushLiveWrite routes here.
//
// Implementations live OUTSIDE this package (transport.WalShipper
// satisfies it via duck typing — no import cycle since the
// interface lives here). The contract intentionally avoids exposing
// wire format, lineage, or coord state — those are sink-side
// concerns. Sink decides where bytes go (legacy port MsgShipEntry
// vs dual-lane port frameWALEntry); Sender just orchestrates the
// session bracket and routes live writes through.
//
// Architect P1 review (2026-04-29) integration rules — must hold at
// the layer that constructs the sink:
//
//   - The sink's emit context (e.g., conn + lineage) MUST be set
//     BEFORE StartSession is called.
//   - After EndSession, the emit context MUST be restored to the
//     steady-state value (else next steady emit ships under stale
//     recovery lineage).
//
// These are caller obligations; Sender does not enforce them.
type WalShipperSink interface {
	StartSession(fromLSN uint64) error
	DrainBacklog(ctx context.Context) error
	EndSession()
	NotifyAppend(lba uint32, lsn uint64, data []byte) error
}

// newSender is the private builder used by both sink-installing
// constructors below. After P2c-slice A there is no public Sender
// constructor without a sink — sink is mandatory; legacy NewSender
// path was deleted. `conn` is the wire — typically a *net.TCPConn in
// production, or a net.Pipe end in tests. Sender does not close
// `conn`; caller does.
func newSender(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID) *Sender {
	return &Sender{
		primaryStore:  primaryStore,
		coordinator:   coordinator,
		conn:          conn,
		replicaID:     replicaID,
		closeCh:       make(chan struct{}),
		barrierRespCh: make(chan ackOrResult, 1),
	}
}

// NewSenderWithSink constructs a sender that delegates the WAL
// pump to the provided sink. Run() calls sink.StartSession after
// BaseDone, sink.DrainBacklog to run the pump, and sink.EndSession
// in defer. There is no legacy in-package WAL pump anymore — sink is
// the only path to the wire (mini-plan §4 INV-NO-DOUBLE-LIVE row).
//
// After DrainBacklog succeeds, Run still waits on closeCh and runs
// drainAndSeal so PushLiveWrite session-lane entries converge until
// P2c-slice B routes live traffic through WalShipper.
//
// `sink` MUST be non-nil — passing nil is a programmer error and
// panics at construction (P2c-slice A: sink mandatory).
func NewSenderWithSink(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID, sink WalShipperSink) *Sender {
	if sink == nil {
		panic("recovery: NewSenderWithSink: sink is required (P2c-slice A) — use NewSenderWithBacklogRelay if no transport WalShipper is wired yet")
	}
	s := newSender(primaryStore, coordinator, conn, replicaID)
	s.sink = sink
	return s
}

// senderBacklogSink adapts Sender's legacy streamBacklog pump to
// WalShipperSink. P2b uses it to migrate e2e tests onto the sink
// branch without changing recovery wire format (receiver still expects
// frameWALEntry). Production P2d replaces this with transport.WalShipper
// once dual-lane + MsgShipEntry context is wired.
//
// StartSession / EndSession are no-ops: session bounds live on Sender
// (fromLSN/targetLSN set at Run entry); rewind semantics belong on the
// real WalShipper sink in transport.
//
// NotifyAppend (P2c-slice B-1) buffers live writes in the sender's
// liveQueue under queueMu — production discipline (architect rule 2)
// because streamBacklog runs as a one-shot ScanLBAs scan and direct
// wire emit during the scan would interleave LSNs and break the
// receiver's monotonic check. drainAndSeal flushes the queue in LSN
// order after backlog drain. When P2d swaps in a real transport
// WalShipper sink, NotifyAppend will route to WalShipper.NotifyAppend
// (Backlog mode lag-tracking; Realtime direct ship under shipMu) and
// the buffer becomes redundant (slice B-2 deletion).
type senderBacklogSink struct {
	s *Sender
}

func (w senderBacklogSink) StartSession(fromLSN uint64) error {
	_ = fromLSN
	return nil
}

func (w senderBacklogSink) DrainBacklog(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return w.s.streamBacklog()
}

func (w senderBacklogSink) EndSession() {}

func (w senderBacklogSink) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	return w.s.bufferLiveWriteLocked(lba, lsn, data)
}

// NewSenderWithBacklogRelay constructs a Sender whose WalShipperSink
// replays WAL via streamBacklog using the in-package backlog relay
// adapter. This is the bridging constructor used by tests and the
// production primary bridge until P2d wires a real transport
// WalShipper adapter behind NewSenderWithSink.
func NewSenderWithBacklogRelay(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID) *Sender {
	s := newSender(primaryStore, coordinator, conn, replicaID)
	s.sink = senderBacklogSink{s: s}
	return s
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
// P2c-slice B-1: routes through the sink's NotifyAppend so the live
// path is uniform with the sink's other emit surfaces. The sink owns
// buffering / ordering discipline:
//
//   - Bridging path (senderBacklogSink): buffer in liveQueue,
//     drainAndSeal flushes in LSN order after backlog completes.
//     Production discipline per architect rule 2 — required until
//     P2d aligns dual-lane wire format.
//   - Real transport WalShipper sink (P2d): NotifyAppend routes
//     directly to WalShipper.NotifyAppend; ordering comes from
//     shipMu + Backlog/Realtime mode design.
func (s *Sender) PushLiveWrite(lba uint32, lsn uint64, data []byte) error {
	return s.sink.NotifyAppend(lba, lsn, data)
}

// bufferLiveWriteLocked is the senderBacklogSink-side buffer entry.
// Acquires queueMu, rejects after seal, copies data, appends to
// liveQueue. drainAndSeal consumes it in LSN order after backlog.
//
// Lives on Sender (not senderBacklogSink) so the buffer state — same
// queueMu, sealed, liveQueue, walItem — stays co-located with the
// drainAndSeal flush. When the bridging sink is replaced by a real
// transport WalShipper sink (P2d), this method goes with the queue.
func (s *Sender) bufferLiveWriteLocked(lba uint32, lsn uint64, data []byte) error {
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
//  4. WAL pump via sink: sink.StartSession(fromLSN); sink.DrainBacklog(ctx).
//     The sink owns wire-format encoding and decides whether bytes go
//     to the legacy port (frameWALEntry) or dual-lane port (P2d).
//  5. Wait for caller's Close() OR ctx cancellation.
//  6. Drain live queue + atomic seal; ship buffered live writes.
//  7. Send frameBarrierReq, read frameBarrierResp.
//  8. Verify achieved ≥ targetLSN via coordinator.CanEmitSessionComplete
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

	// Step 2b: spawn the inbound demux reader. From this point onward
	// any frame from the receiver (BaseBatchAck during base lane,
	// BarrierResp at end) flows through readerLoop. Reader exits on
	// BarrierResp delivery, on any wire error, or on conn close by
	// caller's defer.
	go s.readerLoop()

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

	// Step 5–7: WAL pump via sink.
	//
	// Sink delegates StartSession + DrainBacklog to its implementation
	// (transport WalShipper or in-package senderBacklogSink). One emit
	// path = INV-NO-DOUBLE-LIVE at recovery layer (mini-plan §4).
	//
	// After DrainBacklog, Run still waits on closeCh and runs
	// drainAndSeal so PushLiveWrite session-lane tails ship before
	// barrier — slice B will route live traffic through WalShipper
	// and remove this tail.
	//
	// Defer EndSession FIRST so it fires on every exit path after this
	// point — DrainBacklog error, BarrierReq error, barrier-resp error.
	defer s.sink.EndSession()

	if err := s.sink.StartSession(fromLSN); err != nil {
		return 0, newFailure(FailureContract, PhaseBacklog,
			fmt.Errorf("sink.StartSession: %w", err))
	}
	if err := s.sink.DrainBacklog(ctx); err != nil {
		// Classify substrate vs cancellation vs other.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, newFailure(FailureCancelled, PhaseBacklog, err)
		}
		return 0, newFailure(FailureSubstrate, PhaseBacklog,
			fmt.Errorf("sink.DrainBacklog: %w", err))
	}
	// Coord's TryAdvanceToSteadyLive is collapsed (kickoff Q3 ratified
	// Phase enum collapse to {Idle, Active}). Barrier-ack is the only
	// authoritative completion signal.
	select {
	case <-s.closeCh:
	case <-ctx.Done():
		return 0, newFailure(FailureCancelled, PhaseAwaitClose, ctx.Err())
	}
	if err := s.drainAndSeal(); err != nil {
		return 0, err
	}

	// Step 8 + 9: barrier round-trip — reader goroutine delivers the
	// inbound BarrierResp via barrierRespCh (or any wire / protocol /
	// pin-update error surfaced earlier).
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
	if !s.coordinator.CanEmitSessionComplete(s.replicaID, achieved) {
		return achieved, newFailure(FailureContract, PhaseBarrierResp,
			fmt.Errorf("achieved=%d < target=%d", achieved, targetLSN))
	}
	return achieved, nil
}

// readerLoop is the sender's inbound demux. It reads frames until
// BarrierResp arrives (success path) or any wire / protocol / pin
// failure surfaces. Result is delivered to barrierRespCh exactly once.
//
// BaseBatchAck handling:
//   1. Decode payload.
//   2. Validate SessionID matches the active session.
//   3. Fetch primary's S boundary (`Boundaries().S`).
//   4. Call coord.SetPinFloor(replicaID, AcknowledgedLSN, S).
//   5. On error (incl. PinUnderRetention), abort by pushing to
//      barrierRespCh and return.
//
// Reader exits on:
//   - BarrierResp delivered → push achieved, return.
//   - readFrame error (peer closed, etc.) → push Wire failure, return.
//   - any decode / validation error → push typed failure, return.
//   - SetPinFloor error → push the typed failure, return.
//
// Single-shot: pushes exactly one ackOrResult to barrierRespCh.
// barrierRespCh buffer=1 so push always succeeds without consumer.
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
				// SetPinFloor returns *Failure already typed
				// (FailurePinUnderRetention or wrapped); pass through.
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
