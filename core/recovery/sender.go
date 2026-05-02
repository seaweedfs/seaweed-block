package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// barrierEligibilityProbe is the optional sink-side hook that exposes
// the §IV.2.1 PrimaryWalLegOk observation tuple. Implemented by the
// dual-lane RecoverySink (transport package) which has access to the
// per-replica WalShipper; bridging-sink (senderBacklogSink) does NOT
// implement this — only the dual-lane production path emits the
// `barrier prepare` / `barrier handshake` markers per plan §8.2.6.
//
// Returning values:
//
//	debtZero — cursor == head observed under serializer lock at probe
//	liveTail — at least one EmitKindLive frame in this session
//	walLegOk — debtZero ∨ liveTail
//	cursor   — last-emitted LSN
//	head     — primary's head at probe
//
// Slice marker-only stage: probe is observation; the values do NOT
// gate BarrierReq emission. A-class wave (post-G0) replaces the
// recover(a,b) `walApplied >= target` predicate with this tuple.
type barrierEligibilityProbe interface {
	ProbeBarrierEligibility() (debtZero, liveTail, walLegOk bool, cursor, head uint64)
}

// Sender is the primary-side driver of one rebuild session. It runs
// the base lane, then delegates the WAL pump (and live-write traffic
// during the session) to its WalShipperSink, and finally runs the
// barrier round-trip.
//
// POC scope:
//   - Base lane: dense — ships every LBA in [0, NumBlocks()) by reading
//     primary's current state via LogicalStorage.Read.
//   - WAL pump: delegated to sink. The bridging sink (senderBacklogSink)
//     scans the substrate's WAL via ScanLBAs(fromLSN, ...) up to (and
//     including) targetLSN. Live writes during the session arrive via
//     PushLiveWrite → sink.NotifyAppend; the bridging sink owns the
//     buffering / atomic-seal / LSN-ordered flush discipline.
//   - No baseBatchAck pin advancement during the session: pin floor
//     stays at the session's fromLSN until EndSession releases it.
//     Adding incremental pin advancement is a layer-2 enhancement.
//
// Sender owns:
//   - The wire `conn` writer side (frame writes serialize through
//     `writerMu`).
//
// Sender borrows:
//   - `primaryStore` for reads (base + bridging-sink backlog scan).
//   - `coordinator` for ship-phase transitions and routing decisions.
//   - The injected sink for the entire WAL emit path.
//
// Sender does NOT own: receiver-side state, retry policy, lineage
// minting, session lifecycle decisions, or any live-write buffer.
type Sender struct {
	primaryStore storage.LogicalStorage
	coordinator  *PeerShipCoordinator
	conn         io.ReadWriter
	replicaID    ReplicaID

	// Session contract — set at Run() entry, immutable thereafter.
	sessionID uint64
	fromLSN   uint64
	targetLSN uint64

	writerMu     sync.Mutex  // own mutex; serializes conn writes (frames are atomic)
	sharedWriter *sync.Mutex // optional; non-nil if sink exposes WriteMu() (C1 §6.8 #1)

	// barrierRespCh delivers the inbound BarrierResp (or any wire /
	// protocol failure surfaced by the reader goroutine) to the
	// main Run goroutine at barrier-wait time. Buffered=1 so the
	// reader can push exactly once without blocking on a consumer.
	//
	// Per docs/recovery-pin-floor-wire.md §7: the reader goroutine
	// is required because BaseBatchAck arrives mid-stream during
	// base lane while Run is busy writing frames.
	barrierRespCh chan ackOrResult

	// sink — Run() delegates the WAL pump (StartSession / DrainBacklog
	// / EndSession / NotifyAppend) to this sink. After P2c-slice A sink
	// is mandatory: every Sender has one (transport WalShipper in
	// production, or senderBacklogSink in the bridging path until P2d).
	// Set by NewSenderWithSink / NewSenderWithBacklogRelay.
	sink WalShipperSink
}

// ackOrResult is the channel payload from the reader goroutine to
// the main Run goroutine: either the achieved LSN from BarrierResp
// or a typed *Failure from any wire / protocol / pin-update error.
type ackOrResult struct {
	achieved uint64
	err      error
}

// WalShipperSink is the abstract WAL-pump surface that Sender
// delegates to. Run() invokes the session bracket:
//
//   1. StartSession(fromLSN) — before any WAL emission begins
//   2. DrainBacklog(ctx)     — runs until cursor catches head;
//                              for the bridging sink, also flushes
//                              buffered live writes and seals.
//   3. EndSession()          — defer-fired regardless of exit path;
//                              MUST be idempotent and MUST seal the
//                              sink against further NotifyAppend so
//                              post-Run pushes get an explicit error.
//
// Plus the live-write API for RouteSessionLane traffic during the
// session:
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
// constructors below. Sink is mandatory; legacy NewSender path is gone.
// `conn` is the wire — typically a *net.TCPConn in production, or a
// net.Pipe end in tests. Sender does not close `conn`; caller does.
func newSender(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID) *Sender {
	return &Sender{
		primaryStore:  primaryStore,
		coordinator:   coordinator,
		conn:          conn,
		replicaID:     replicaID,
		barrierRespCh: make(chan ackOrResult, 1),
	}
}

// NewSenderWithSink constructs a sender that delegates the WAL
// pump to the provided sink. Run() calls sink.StartSession after
// BaseDone, sink.DrainBacklog to run the pump (which also handles
// live-write flush + seal in the bridging path), and sink.EndSession
// in defer.
//
// `sink` MUST be non-nil — passing nil is a programmer error and
// panics at construction (P2c-slice A).
func NewSenderWithSink(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID, sink WalShipperSink) *Sender {
	if sink == nil {
		panic("recovery: NewSenderWithSink: sink is required (P2c-slice A) — use NewSenderWithBacklogRelay if no transport WalShipper is wired yet")
	}
	s := newSender(primaryStore, coordinator, conn, replicaID)
	s.sink = sink
	// C1 §6.8 #1: if the sink exposes a shared write mutex (the
	// per-replica entry's writeMu), use it so Sender.writeFrame and
	// the WalShipper's EmitFunc serialize on the same conn. Sinks
	// that don't share (bridging senderBacklogSink) leave sharedWriter
	// nil; Sender falls back to its own writerMu (uncontended).
	if wms, ok := sink.(interface{ WriteMu() *sync.Mutex }); ok {
		s.sharedWriter = wms.WriteMu()
	}
	return s
}

// walItem is the per-entry record buffered by senderBacklogSink while
// streamBacklog is in flight. P2c-slice B-2 moved this into the sink:
// the buffer is a sink-internal artifact of the bridging path, not a
// Sender field. When P2d swaps in a real transport WalShipper sink,
// this type goes with the bridging sink.
type walItem struct {
	lba  uint32
	lsn  uint64
	data []byte
}

// senderBacklogSink is the in-package WalShipperSink that bridges
// recovery.Sender's emit path to the legacy frameWALEntry wire format.
// Production P2d replaces this with a transport.WalShipper adapter
// once dual-lane vs MsgShipEntry context is wired.
//
// Lifecycle (P2c-slice B-2):
//   - StartSession is a no-op: session bounds live on Sender (fromLSN /
//     targetLSN set at Run entry); rewind semantics belong on the real
//     WalShipper sink in transport.
//   - NotifyAppend buffers live writes under sinkMu — the bridging
//     path's "production discipline" (architect rule 2). streamBacklog
//     is a one-shot ScanLBAs scan; direct wire emit during the scan
//     would interleave LSNs and break the receiver's monotonic check.
//   - DrainBacklog runs streamBacklog (ships LSN ≤ targetLSN entries)
//     and then flushAndSeal — sorts buffered live writes (LSN >
//     targetLSN) by LSN and ships them as WALKindSessionLive frames,
//     atomically seal=true so subsequent NotifyAppend rejects.
//   - EndSession is the defer-safety net: idempotent seal so any post-
//     Run NotifyAppend gets an explicit error. Buffer entries that
//     weren't flushed (e.g., DrainBacklog errored mid-scan) are
//     dropped — the wire is presumed unsalvageable in error paths.
//
// When P2d wires the real transport.WalShipper sink, NotifyAppend will
// route to WalShipper.NotifyAppend (Backlog mode lag-tracking; Realtime
// direct ship under shipMu) — the buffer + flushAndSeal scaffolding
// here goes away with the bridging sink.
type senderBacklogSink struct {
	s *Sender

	sinkMu sync.Mutex
	queue  []walItem
	sealed bool
}

func (w *senderBacklogSink) StartSession(fromLSN uint64) error {
	_ = fromLSN
	return nil
}

func (w *senderBacklogSink) DrainBacklog(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := w.s.streamBacklog(); err != nil {
		return err
	}
	return w.flushAndSeal()
}

// EndSession is the defer-safety seal. If DrainBacklog ran successfully,
// the sink is already sealed and this is a no-op. If Run errored
// before DrainBacklog (e.g., StartSession failed) or DrainBacklog
// errored mid-flight, this guarantees no post-Run NotifyAppend silently
// buffers into a dead session.
func (w *senderBacklogSink) EndSession() {
	w.sinkMu.Lock()
	w.sealed = true
	// Drop any unflushed entries — error paths shouldn't try to ship
	// to a presumed-broken wire. The atomic-seal contract holds for
	// the success path (DrainBacklog → flushAndSeal); error paths
	// surface failures back to the caller via Run's return value.
	w.queue = nil
	w.sinkMu.Unlock()
}

func (w *senderBacklogSink) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	w.sinkMu.Lock()
	defer w.sinkMu.Unlock()
	if w.sealed {
		return errors.New("recovery: NotifyAppend after sink sealed (session is closing)")
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	w.queue = append(w.queue, walItem{lba: lba, lsn: lsn, data: cp})
	return nil
}

// flushAndSeal atomically takes the buffered live-write queue and
// forbids further NotifyAppend (sealed=true under sinkMu), then ships
// every item in LSN order on the same wire as streamBacklog. The
// atomic take + seal guarantees no concurrent NotifyAppend can land
// an entry that misses the flush — the push either lands before
// sinkMu acquisition (captured in queue) or after (sealed → error).
func (w *senderBacklogSink) flushAndSeal() error {
	w.sinkMu.Lock()
	if w.sealed {
		// Idempotent: DrainBacklog called us once; EndSession defer
		// would also call us. After first seal, no further work.
		w.sinkMu.Unlock()
		return nil
	}
	queue := w.queue
	w.queue = nil
	w.sealed = true
	w.sinkMu.Unlock()

	// Stable sort by LSN ascending.
	for i := 1; i < len(queue); i++ {
		j := i
		for j > 0 && queue[j-1].lsn > queue[j].lsn {
			queue[j-1], queue[j] = queue[j], queue[j-1]
			j--
		}
	}

	for _, item := range queue {
		if err := w.s.writeFrame(frameWALEntry, encodeWALEntry(WALKindSessionLive, item.lba, item.lsn, item.data)); err != nil {
			return newFailure(FailureWire, PhaseDrainSeal, err)
		}
		if err := w.s.coordinator.RecordShipped(w.s.replicaID, item.lsn); err != nil {
			return newFailure(FailureContract, PhaseDrainSeal, err)
		}
	}
	return nil
}

// NewSenderWithBacklogRelay constructs a Sender whose WalShipperSink
// replays WAL via streamBacklog and owns the live-write buffer +
// flushAndSeal discipline. This is the bridging constructor used by
// tests and the production primary bridge until P2d wires a real
// transport WalShipper adapter behind NewSenderWithSink.
func NewSenderWithBacklogRelay(primaryStore storage.LogicalStorage, coordinator *PeerShipCoordinator, conn io.ReadWriter, replicaID ReplicaID) *Sender {
	s := newSender(primaryStore, coordinator, conn, replicaID)
	s.sink = &senderBacklogSink{s: s}
	return s
}

// PushLiveWrite hands the sender one local-write event for this peer.
// Caller (the WAL shipper integration) must already have consulted
// `coordinator.RouteLocalWrite(replicaID, lsn)` and confirmed the
// routing is RouteSessionLane; this method does not double-check.
//
// P2c-slice B-1 / B-2: routes through the sink's NotifyAppend; the
// sink owns buffering / ordering discipline:
//
//   - Bridging path (senderBacklogSink): buffer in sink-internal
//     queue under sinkMu, flushed in LSN order during DrainBacklog's
//     flushAndSeal. Production discipline per architect rule 2.
//   - Real transport WalShipper sink (P2d): NotifyAppend routes
//     directly to WalShipper.NotifyAppend; ordering comes from
//     shipMu + Backlog/Realtime mode design.
//
// After the sink seals (DrainBacklog completes or EndSession defer
// fires), NotifyAppend returns an explicit error so post-session
// callers learn the session is closing.
func (s *Sender) PushLiveWrite(lba uint32, lsn uint64, data []byte) error {
	return s.sink.NotifyAppend(lba, lsn, data)
}

// Run drives the entire session end-to-end and blocks until barrier
// ack returns. Returns the AchievedLSN reported by the replica on
// success; returns an error on any wire / session failure.
//
// PRECONDITION: caller already invoked
// `coordinator.StartSession(replicaID, sessionID, fromLSN, targetLSN)`
// synchronously before spawning the goroutine that calls Run. This
// keeps "session is registered" observable from the call site that
// scheduled Run, so concurrent code reading `coord.RouteLocalWrite`
// after the spawn point sees a non-Idle phase deterministically.
//
// Sequence:
//  1. Send frameSessionStart.
//  2. Base lane: ship every LBA's current data via frameBaseBlock.
//  3. Send frameBaseDone; coordinator.MarkBaseDone.
//  4. WAL pump via sink: sink.StartSession(fromLSN); sink.DrainBacklog(ctx).
//     The sink owns wire-format encoding AND (in the bridging path)
//     the live-write buffer flush + seal that previously lived on
//     Sender as drainAndSeal.
//  5. Send frameBarrierReq, read frameBarrierResp.
//  6. Verify achieved ≥ targetLSN via coordinator.CanEmitSessionComplete
//     (§5.2, CHK-BARRIER-BEFORE-CLOSE).
//
// Defer always: sink.EndSession (idempotent seal) + coordinator.EndSession.
//
// Caller MUST call Run exactly once per Sender instance.
//
// Lifecycle / cancellation: pass a Context whose cancellation signals
// "abort this session". DrainBacklog observes ctx and returns ctx.Err()
// on cancellation; sink.EndSession defer fires regardless.
func (s *Sender) Run(ctx context.Context, sessionID, fromLSN, targetLSN uint64) (achievedLSN uint64, err error) {
	s.sessionID = sessionID
	s.fromLSN = fromLSN
	s.targetLSN = targetLSN

	log.Printf("g7-debug: Sender.Run entry replica=%s sessionID=%d fromLSN=%d targetLSN=%d sinkType=%T",
		s.replicaID, sessionID, fromLSN, targetLSN, s.sink)

	// Defer cleanup runs on every exit path. EndSession on the sink
	// is the seal-defense — guarantees post-Run NotifyAppend (e.g.,
	// PushLiveWrite via PrimaryBridge) gets an explicit error rather
	// than silently buffering into a dead session.
	defer s.coordinator.EndSession(s.replicaID)
	defer s.sink.EndSession()

	// C1 §6.8 accounting: install a post-emit hook on the sink (if it
	// supports it) so each successful WalShipper-routed emit advances
	// coord.shipCursor via RecordShipped. Without this, the dual-lane
	// path silently leaves shipCursor at fromLSN. Cleared at session
	// end so post-session emits don't call into a teardown coord.
	type postEmitHookSetter interface {
		SetPostEmitHook(hook func(lsn uint64))
	}
	if h, ok := s.sink.(postEmitHookSetter); ok {
		h.SetPostEmitHook(func(lsn uint64) {
			_ = s.coordinator.RecordShipped(s.replicaID, lsn)
		})
		defer h.SetPostEmitHook(nil)
	}

	// Step 1: SessionStart frame.
	numBlocks := s.primaryStore.NumBlocks()
	log.Printf("g7-debug: Sender.Run writing frameSessionStart replica=%s numBlocks=%d", s.replicaID, numBlocks)
	if err := s.writeFrame(frameSessionStart, encodeSessionStart(sessionStartPayload{
		SessionID: sessionID, FromLSN: fromLSN, TargetLSN: targetLSN, NumBlocks: numBlocks,
	})); err != nil {
		return 0, newFailure(FailureWire, PhaseSendStart, err)
	}
	log.Printf("g7-debug: Sender.Run frameSessionStart written ok replica=%s", s.replicaID)

	// Step 1b: spawn the inbound demux reader. From this point onward
	// any frame from the receiver (BaseBatchAck during base lane,
	// BarrierResp at end) flows through readerLoop. Reader exits on
	// BarrierResp delivery, on any wire error, or on conn close by
	// caller's defer.
	go s.readerLoop()

	// Step 2 + 3 + 4: BASE ∥ WAL — run base lane and WAL pump as
	// concurrent goroutines (§6.8 #6 / P6 / G3 BASE ∥ WAL wall-clock
	// overlap). Both write through the shared writeMu (C1) — frame
	// integrity preserved by mutex-bounded interleaving on the wire.
	//
	// Base goroutine:  streamBase → frameBaseDone → coord.MarkBaseDone.
	// WAL goroutine:   sink.StartSession → sink.DrainBacklog.
	//
	// errgroup waits both; on any error from either, ctx cancel fires
	// to unblock the other. Barrier writes only after both complete.
	groupCtx, groupCancel := context.WithCancel(ctx)
	defer groupCancel()
	baseErr := make(chan error, 1)
	walErr := make(chan error, 1)

	go func() {
		log.Printf("g7-debug: base goroutine entry replica=%s numBlocks=%d", s.replicaID, numBlocks)
		// Base lane.
		if err := s.streamBase(numBlocks); err != nil {
			log.Printf("g7-debug: base goroutine streamBase err replica=%s err=%v", s.replicaID, err)
			groupCancel()
			baseErr <- err
			return
		}
		log.Printf("g7-debug: base goroutine streamBase done replica=%s", s.replicaID)
		// BaseDone signal.
		if err := s.writeFrame(frameBaseDone, nil); err != nil {
			log.Printf("g7-debug: base goroutine BaseDone write err replica=%s err=%v", s.replicaID, err)
			groupCancel()
			baseErr <- newFailure(FailureWire, PhaseBaseDone, err)
			return
		}
		if err := s.coordinator.MarkBaseDone(s.replicaID); err != nil {
			log.Printf("g7-debug: base goroutine MarkBaseDone err replica=%s err=%v", s.replicaID, err)
			groupCancel()
			baseErr <- newFailure(FailureContract, PhaseBaseDone, err)
			return
		}
		log.Printf("g7-debug: base goroutine exit ok replica=%s", s.replicaID)
		baseErr <- nil
	}()

	go func() {
		log.Printf("g7-debug: wal goroutine entry replica=%s fromLSN=%d", s.replicaID, fromLSN)
		if err := s.sink.StartSession(fromLSN); err != nil {
			log.Printf("g7-debug: wal goroutine sink.StartSession err replica=%s err=%v", s.replicaID, err)
			groupCancel()
			walErr <- newFailure(FailureContract, PhaseBacklog,
				fmt.Errorf("sink.StartSession: %w", err))
			return
		}
		log.Printf("g7-debug: wal goroutine sink.StartSession done replica=%s", s.replicaID)
		if err := s.sink.DrainBacklog(groupCtx); err != nil {
			log.Printf("g7-debug: wal goroutine sink.DrainBacklog err replica=%s err=%v", s.replicaID, err)
			groupCancel()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				walErr <- newFailure(FailureCancelled, PhaseBacklog, err)
				return
			}
			walErr <- newFailure(FailureSubstrate, PhaseBacklog,
				fmt.Errorf("sink.DrainBacklog: %w", err))
			return
		}
		log.Printf("g7-debug: wal goroutine exit ok replica=%s", s.replicaID)
		walErr <- nil
	}()

	// Wait both. On any error, propagate first non-nil; ctx cancel
	// fired by failing goroutine ensures the other unblocks.
	bErr := <-baseErr
	log.Printf("g7-debug: Sender.Run base goroutine returned replica=%s err=%v", s.replicaID, bErr)
	wErr := <-walErr
	log.Printf("g7-debug: Sender.Run wal goroutine returned replica=%s err=%v", s.replicaID, wErr)
	if bErr != nil {
		return 0, bErr
	}
	if wErr != nil {
		return 0, wErr
	}

	// Step 5 + 6: barrier round-trip — reader goroutine delivers the
	// inbound BarrierResp via barrierRespCh (or any wire / protocol /
	// pin-update error surfaced earlier).
	//
	// §IV.2.1 / §8.2.6 marker-only stage (G0 closed; Tier-1 predicate
	// replacement = A-class wave, not this commit). Before writing
	// frameBarrierReq, snapshot the PrimaryWalLegOk inputs from the
	// sink (if it implements barrierEligibilityProbe) and emit the
	// 5-field `barrier prepare` log line per plan §8.2.6 hardware
	// oracle contract. The values are real (debtZero, liveTail,
	// walLegOk computed from current shipper state) but do NOT yet
	// gate the emission — observability bridge for QA.
	//
	// cutID source per architect Option B (plan §8.2.6 + §6 v3.27):
	// per-session monotonic counter from coord.NextBarrierCut. Pre-
	// §IV.2.4 wire change, this is a coordinator-local sequence;
	// post-C-class it becomes the wire CheckpointCutSeq with no
	// log-prefix change (still `cut=CCS:<n>`).
	var (
		barrierCutID    uint64
		probeDebtZero   bool
		probeLiveTail   bool
		probeWalLegOk   bool
		probeCursor     uint64
		probeHead       uint64
		probeAvailable  bool
	)
	if probe, ok := s.sink.(barrierEligibilityProbe); ok {
		probeDebtZero, probeLiveTail, probeWalLegOk, probeCursor, probeHead = probe.ProbeBarrierEligibility()
		probeAvailable = true
	}
	if cut, err := s.coordinator.NextBarrierCut(s.replicaID); err == nil {
		barrierCutID = cut
	}
	// §IV.2.1 / recover-semantics-adjustment-plan §1 A-class:
	// record the PrimaryWalLegOk witness in coord state BEFORE
	// writing frameBarrierReq, so CanEmitSessionComplete sees it as
	// part of the close conjunct. Dual-lane only — bridging sink
	// path (no probe) leaves the witness unset and the predicate
	// collapses to legacy. Idle-peer error is impossible here
	// (StartSession has run); silently swallowed for safety.
	if probeAvailable {
		_ = s.coordinator.RecordBarrierWalLegOk(s.replicaID, probeWalLegOk)
	}
	if probeAvailable && barrierCutID > 0 {
		log.Printf("replication: barrier prepare replica=%s sessionID=%d cut=CCS:%d PrimaryDebtZero=%t LiveTail=%t WalLegOk=%t cursor=%d head=%d",
			s.replicaID, sessionID, barrierCutID,
			probeDebtZero, probeLiveTail, probeWalLegOk,
			probeCursor, probeHead)
	}

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

	// §IV.2.1 / §8.2.6 marker-only stage: emit the `barrier handshake`
	// log line with the SAME cutID as the prepare line (Option B
	// round-trip). Pre-C-class wire (no CheckpointCutSeq on the
	// BarrierResp payload), `match=true` is unconditional — the same
	// cutID flows from prepare to handshake by construction. Post-
	// C-class, match becomes the real wire-vs-engine compare; a
	// `match=false` line in hardware logs is the RED scenario per
	// §8.2.6.
	if probeAvailable && barrierCutID > 0 {
		log.Printf("replication: barrier handshake cut=CCS:%d achieved=%d match=true",
			barrierCutID, achieved)
	}
	// §IV.2.1 / FS-1 / Gate G0 — Tier 1 completion-authority site.
	// The CanEmitSessionComplete check below + FailureContract on
	// `achieved < target` is the historic recover(a,b) Run-success
	// predicate; per consensus §I P8, this is NOT the recover(a)
	// completion authority. Migration target (per
	// `sw-block/design/recover-semantics-adjustment-plan.md` §1 +
	// `learn/2026-05-01-recover-target-audit.md` Tier 1) replaces
	// this with `baseDone (replica) ∧ PrimaryWalLegOk(P) (primary
	// WalShipper) ∧ BarrierHandshake`; FailureContract is rebound
	// to "barrier-pre violation / lifecycle contract violation"
	// rather than "did not reach Y". NO behavior change pre-Gate G0.
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
	const progressEvery uint32 = 4096
	for lba := uint32(0); lba < numBlocks; lba++ {
		data, err := s.primaryStore.Read(lba)
		if err != nil {
			return newFailure(FailureSubstrate, PhaseBaseLane,
				fmt.Errorf("read lba=%d: %w", lba, err))
		}
		if err := s.writeFrame(frameBaseBlock, encodeBaseBlock(lba, data)); err != nil {
			return newFailure(FailureWire, PhaseBaseLane, err)
		}
		if (lba+1)%progressEvery == 0 {
			log.Printf("g7-debug: streamBase progress replica=%s lba=%d/%d", s.replicaID, lba+1, numBlocks)
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
		// session-lane buffer (via NotifyAppend) or the steady-live
		// path; streamBacklog only ships pre-target history.
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

func (s *Sender) writeFrame(t frameType, payload []byte) error {
	// C1 (§6.8 #1 mechanical SINGLE-SERIALIZER): when the sink exposes
	// WriteMu() (transport.RecoverySink does), share that mutex with
	// the WalShipper's EmitFunc so concurrent emits don't interleave
	// header+payload on the same conn. Falls back to own mutex when
	// the sink doesn't share (bridging path; legacy single-writer).
	if s.sharedWriter != nil {
		s.sharedWriter.Lock()
		defer s.sharedWriter.Unlock()
	} else {
		s.writerMu.Lock()
		defer s.writerMu.Unlock()
	}
	return writeFrame(s.conn, t, payload)
}
