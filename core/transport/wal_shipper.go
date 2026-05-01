package transport

// WalShipper is the per-replica, single-cursor WAL emit scheduler on
// the primary side. One instance per `(volume, replica)`. Implementation
// of `v3-recovery-wal-shipper-spec.md` §2-§7.
//
// Invariants pinned (`INV-*` from spec §3):
//   - INV-SINGLE: at most one execution context emits for a given peer
//     at a time; serialized by `shipMu`.
//   - INV-MONOTONIC-CURSOR: `cursor` is monotonically non-decreasing
//     except for the single rewind at session entry (StartSession).
//   - INV-SUBSET: emitted set is exactly `(fromLSN, cursor]` of the
//     substrate's WAL when in Realtime; or `(fromLSN, cursor]` of the
//     scan range when in Backlog.
//   - INV-NO-GAP-R1: when transitioning Backlog→Realtime, no LSN with
//     `cursor < L ≤ headAtTransition` is missed; gated by R1 §5.
//   - INV-NO-DOUBLE-LIVE: at any moment, exactly one delivery path is
//     live (mutex-enforced).
//
// Boundary (per spec §1, §8):
//   - WalShipper does NOT know wire frame format. Emits via injected
//     `EmitFunc(lba, lsn, data) error`. Production wraps SWRP
//     `MsgShipEntry` write; test uses a recording mock.
//   - WalShipper does NOT own base lane / extent dump (separate
//     transient component during recovery session).
//   - WalShipper does NOT own barrier / coord pin advancement.
//   - WalShipper consumes `head` from a `HeadSource`; reads substrate
//     via `BacklogScanner` for the backlog drain loop.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// EmitKind tags one emit with its source lane semantic. Passed by
// WalShipper to EmitFunc; the EmitFunc uses it only when the wire
// profile cares (e.g., dual-lane recovery WAL frame's WALKind tag);
// the legacy MsgShipEntry profile ignores it.
//
// EmitKindLive: NotifyAppend in Realtime mode (live writes from the
// primary's local apply path).
// EmitKindBacklog: DrainBacklog scan loop emits (substrate ScanLBAs
// catch-up during recovery session backlog phase).
//
// P2d (architect 2026-04-30): kind plus the entry's emit profile
// determines the wire frame: SteadyMsgShip + any kind → MsgShipEntry;
// DualLaneWALFrame + Live → frameWALEntry(WALKindSessionLive);
// DualLaneWALFrame + Backlog → frameWALEntry(WALKindBacklog).
type EmitKind int

const (
	EmitKindLive EmitKind = iota
	EmitKindBacklog
)

// EmitFunc is the wire-side delivery surface. WalShipper hands every
// emit through this function. In production: BlockExecutor.Ship's
// WriteMsg. In test: a mock that records LSN sequence + asserts
// invariants.
//
// On error: WalShipper propagates the error to its caller (NotifyAppend
// or DrainBacklog return value) and aborts the current emit. WalShipper
// does NOT itself flip mode or take degraded action — that decision
// belongs to the executor / engine layer (e.g., mark peer Degraded,
// schedule retry, fail-close session). Mode stays unchanged on emit
// error; cursor is NOT advanced for the failed emit.
type EmitFunc func(kind EmitKind, lba uint32, lsn uint64, data []byte) error

// HeadSource provides primary's current head LSN. WalShipper queries
// this in R1's double-check and on each timer tick. The substrate's
// `LogicalStorage.Boundaries()` already returns `(R, S, H)` — H is
// head; this minimal interface lets test mock head cleanly.
type HeadSource interface {
	Head() uint64
}

// boundariesHeadSource adapts a `storage.LogicalStorage` to HeadSource
// by reading H from Boundaries().
type boundariesHeadSource struct {
	store storage.LogicalStorage
}

func (b boundariesHeadSource) Head() uint64 {
	_, _, h := b.store.Boundaries()
	return h
}

// HeadSourceFromStorage wraps a substrate as a HeadSource.
func HeadSourceFromStorage(store storage.LogicalStorage) HeadSource {
	return boundariesHeadSource{store: store}
}

// WalShipperMode is the per-replica scheduler state.
//
// Idle: no replica activity (e.g., not yet probed); NotifyAppend drops
// silently. Used at construction; transitions to Realtime via Activate
// or to Backlog via StartSession.
//
// Realtime: cursor == head; NotifyAppend emits immediately under
// shipMu. Steady-state shipping path.
//
// Backlog: cursor < head (replica is catching up); NotifyAppend just
// updates the head cache and advances will be picked up by the
// backlog drain loop. Triggered at session start (rewind to fromLSN).
type WalShipperMode int

const (
	ModeIdle WalShipperMode = iota
	ModeRealtime
	ModeBacklog
)

func (m WalShipperMode) String() string {
	switch m {
	case ModeIdle:
		return "Idle"
	case ModeRealtime:
		return "Realtime"
	case ModeBacklog:
		return "Backlog"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// WalShipper config knobs (defaults are spec §6.3 R2 + sane production
// values). Test code uses NewWalShipperWithOptions for tighter timing.
type WalShipperConfig struct {
	// IdleSleep is how long the backlog drain loop sleeps between
	// scan iterations when the substrate emits no new entries (avoid
	// hot-spin without mode flip yet). Default 5ms.
	IdleSleep time.Duration

	// SaturationThreshold is the lag (head - cursor) above which
	// `OnSaturation` fires once. Default 100,000 LSN (R2 spec §6.2).
	// Zero disables R2 entirely.
	SaturationThreshold uint64

	// OnSaturation is the R2 hook. Called from a goroutine-safe
	// context once when lag crosses the threshold; engine adapter
	// decides what to do (escalate / log / etc.). nil = no hook.
	OnSaturation func(replicaID string, lag uint64)

	// StrictRealtimeOrdering, if true, makes the tail-emit path FAIL
	// CLOSED on `lsn != cursor + 1` when cursor == head (no debt).
	// Default false: still fails closed (per consensus §6.3 — input
	// payload MUST NOT replace a missing LSN), unless
	// LegacyOutOfOrderEmit is also set. The flag now functions as
	// "fail-closed at the tail-emit gate" — same behavior strict and
	// non-strict, kept for production-side wiring expressivity.
	//
	// NOTE (2026-04-30 §6.3 / §6.9 collapse): Pre-§6.3 default behavior
	// was "log warning + emit input bytes anyway" on a tail gap. That
	// bypass-the-gap behavior is now opt-in via LegacyOutOfOrderEmit —
	// see that field. StrictRealtimeOrdering's old role (toggling
	// fail-closed) is subsumed by §6.3's tail-gap contract; both
	// values now fail closed by default.
	StrictRealtimeOrdering bool

	// LegacyOutOfOrderEmit, if true, restores pre-§6.3 behavior on a
	// tail gap (cursor == head, input.lsn != cursor+1): log a warning
	// and emit input bytes anyway. DEPRECATED — exists only as a
	// short-lived migration knob for tests / staging that exercised
	// the silent-fill path. Callers MUST plan to remove this flag.
	//
	// Production wiring MUST leave this false. With this flag false,
	// a tail-gap surfaces as an error from the dispatch path; the
	// engine MUST treat the error as a CursorGap signal and escalate
	// to rebuild (consensus §6.3 + §6.9 NEGATIVE-EQUITY ban: the
	// shipper does NOT self-heal a tail gap by ad-hoc emit).
	//
	// NOTE: this flag does NOT affect the debt-fill path (cursor <
	// head). When debt exists, drive() always scans substrate first
	// (CASE A) regardless of LegacyOutOfOrderEmit — debt-fill via
	// substrate is the §6.3 default, not negotiable.
	LegacyOutOfOrderEmit bool

	// DisableTimerDrain, if true, suppresses the C2 §6.8 #4 self-
	// driving Backlog drain goroutine.
	//
	// PRODUCTION CALLERS MUST LEAVE THIS false. §6.8 #4 is a MUST:
	// "primary-idle starvation forbidden" — backlog cannot depend
	// solely on new appends. Setting DisableTimerDrain=true in
	// production violates the architect contract.
	//
	// Test-only invariant when set true:
	//   "the WalShipper MUST NOT linger in cursor < head without
	//    an alternative drain source" — i.e., the test itself drives
	//    DrainBacklog explicitly, OR the test asserts on a
	//    transient-debt state and tears the shipper down before
	//    the assertion would observe starvation.
	//
	// Today's only callers are core/transport's R2 saturation tests
	// (TestWalShipper_R2_LagSignalFires / NoSpuriousSignal): they
	// build lag by writing without a manual DrainBacklog call,
	// observe OnSaturation fires, then tear down. Within the test
	// scope, debt is intentional and bounded.
	DisableTimerDrain bool
}

// DefaultWalShipperConfig returns production defaults.
//
// MIGRATION NOTE (slice 1 of §6.3 collapse): LegacyOutOfOrderEmit
// defaults to TRUE here as a transitional bridge — preserves
// pre-§6.3 "log + emit on tail gap" semantics so existing tests +
// production callers don't observe a behavior flip in this commit.
// Slice 2 will flip the default to false (§6.3 normative fail-closed
// on tail gap), once production callers have been audited /
// migrated. Tests that explicitly want §6.3 strict behavior must
// set LegacyOutOfOrderEmit=false in their config.
func DefaultWalShipperConfig() WalShipperConfig {
	return WalShipperConfig{
		IdleSleep:            5 * time.Millisecond,
		SaturationThreshold:  100_000,
		OnSaturation:         nil,
		LegacyOutOfOrderEmit: true, // slice-1 transitional; flip in slice-2
	}
}

// WalShipper is the per-replica WAL emit scheduler. Construct one per
// `(volume, replica)` via `NewWalShipper`; the executor's registry
// enforces INV-SINGLE.
type WalShipper struct {
	replicaID string
	head      HeadSource
	substrate storage.LogicalStorage
	emit      EmitFunc

	cfg WalShipperConfig

	// shipMu serializes all state mutations + emit calls. INV-SINGLE
	// derives from this single mutex: any code path that emits or
	// transitions must hold shipMu.
	shipMu sync.Mutex
	mode   WalShipperMode
	cursor uint64 // last-emitted LSN (or fromLSN if no emit yet in session)

	// Session state — present iff StartSession was called and
	// EndSession has not run yet. EndSession sets sessionActive=false
	// and transitions mode to Realtime (NOT Idle). ModeIdle is the
	// initial state at NewWalShipper construction; there is no path
	// from any non-Idle mode back to Idle in current API.
	sessionActive bool
	fromLSN       uint64 // session entry watermark (post-rewind cursor anchor)

	// Saturation observability (R2). lagSample updated on every emit
	// and timer; saturationFired is single-shot per session to avoid
	// spam.
	lagSample       atomic.Uint64
	saturationFired atomic.Bool

	// C2 §6.8 #4 self-driving drain. timerLoop runs as a background
	// goroutine for the shipper's lifetime; on each `IdleSleep` tick
	// it wakes and (under shipMu) checks `cursor < head`. If true,
	// runs one ScanLBAs emit cycle from cursor — pushing backlog
	// without depending on NotifyAppend arrivals (forbids
	// primary-idle starvation).
	//
	// nudgeCh is a 1-deep buffered signal: NotifyAppend in Realtime
	// with `cursor < head` (debt detected) sends a non-blocking nudge
	// to wake the timer goroutine immediately rather than waiting
	// for the next IdleSleep tick. Idempotent — multiple nudges
	// collapse to one wake.
	//
	// stopCh is closed by Stop() to terminate the timer goroutine.
	// Idempotent via stopOnce.
	nudgeCh   chan struct{}
	stopCh    chan struct{}
	stopOnce  sync.Once
	timerDone chan struct{}
}

// NewWalShipper constructs a WalShipper bound to a substrate +
// emit sink. Mode starts at Idle; caller must call StartSession or
// Activate to begin emission.
//
// `replicaID` is the peer identity. `emit` is the wire delivery
// callback. `head` provides the primary's head LSN; in production,
// pass `HeadSourceFromStorage(substrate)`. `substrate` is the WAL
// substrate (for ScanLBAs in backlog mode).
func NewWalShipper(replicaID string, head HeadSource, substrate storage.LogicalStorage, emit EmitFunc) *WalShipper {
	return NewWalShipperWithOptions(replicaID, head, substrate, emit, DefaultWalShipperConfig())
}

// NewWalShipperWithOptions is NewWalShipper with explicit config.
// Starts the C2 self-driving drain goroutine; it idles in ModeIdle
// (no emit) and only does work once Activate / StartSession run AND
// the executor installs a non-nil emit context (conn). Stop the
// goroutine via (*WalShipper).Stop().
func NewWalShipperWithOptions(replicaID string, head HeadSource, substrate storage.LogicalStorage, emit EmitFunc, cfg WalShipperConfig) *WalShipper {
	if cfg.IdleSleep <= 0 {
		cfg.IdleSleep = 5 * time.Millisecond
	}
	s := &WalShipper{
		replicaID: replicaID,
		head:      head,
		substrate: substrate,
		emit:      emit,
		cfg:       cfg,
		mode:      ModeIdle,
		nudgeCh:   make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		timerDone: make(chan struct{}),
	}
	if cfg.DisableTimerDrain {
		// Tests that need manual DrainBacklog control set this. The
		// timerDone channel must be closed so Stop() doesn't block.
		close(s.timerDone)
	} else {
		go s.timerLoop()
	}
	return s
}

// Stop terminates the self-driving drain goroutine. Idempotent.
// Calling Stop after Stop is a no-op. Caller blocks until the
// goroutine exits.
//
// In production the executor manages WalShipper lifecycle per
// (volume, replicaID). For tests, defer Stop() to avoid goroutine
// leaks across test cases.
func (s *WalShipper) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	<-s.timerDone
}

// timerLoop is the C2 self-driving drain goroutine. On every
// IdleSleep tick, OR when nudged by NotifyAppend, it attempts one
// ScanLBAs cycle if `cursor < head` and a session is active or
// shipper is in Backlog mode.
//
// §6.8 #4 normative: "MUST implement a periodic ShipOpportunity
// (timer or shipper-internal loop equivalent) that attempts
// emit-from-cursor so backlog cannot depend solely on new appends
// (Primary idle starvation forbidden)."
func (s *WalShipper) timerLoop() {
	defer close(s.timerDone)

	ticker := time.NewTicker(s.cfg.IdleSleep)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.drainOpportunity()
		case <-s.nudgeCh:
			s.drainOpportunity()
		}
	}
}

// driveInput is the unified payload to the §6.3 single-dispatch
// drive() function. has=false is a timer/nudge ∅-tick (no incoming
// append); has=true is a NotifyAppend caller offering an Append.
//
// Per consensus §6.3 there is ONE mailbox: both inputs converge on
// drive(). The "two-mailbox" anti-pattern (separate Backlog vs
// Realtime queues) is forbidden by §6.9 HOPE-SHIPPER-MONOTONIC.
type driveInput struct {
	has  bool
	lba  uint32
	lsn  uint64
	data []byte
}

// drive is the §6.3 normative single-dispatch entry. It implements:
//
//	CASE A — debt (cursor < head):
//	  Substrate is canonical. Scan from cursor, emit each entry,
//	  advance cursor; loop until cursor == head OR scan exits with
//	  no progress. Per §13 alignment, this is the BACKLOG-DRAIN
//	  path even when invoked from a NotifyAppend caller — debt-first
//	  means substrate, NOT input. The reader of this code who is
//	  worried about "Realtime is reading substrate?": no, we are
//	  observing debt and switching to backlog semantics for this
//	  call; ModeRealtime as a name is not load-bearing for dispatch.
//
//	CASE B — caught up + fresh tail (cursor == head, input.has,
//	  input.lsn == cursor+1):
//	  Emit input directly; advance cursor by 1.
//
//	CASE C — caught up + ∅ input: noop.
//
//	Tail-gap (cursor == head, input.lsn != cursor+1, input.lsn > cursor):
//	  Per §6.3 / §6.9 NEGATIVE-EQUITY ban — input payload MUST NOT
//	  replace a missing LSN. Default: return an explicit error so
//	  the engine escalates to rebuild. LegacyOutOfOrderEmit=true
//	  (transitional) restores pre-§6.3 log+emit behavior.
//
//	Idle (mode == ModeIdle): drop everything (no replica context).
//
// Concurrency: drive holds shipMu for the whole call, including the
// CASE A scan loop. INV-SINGLE: at most one drive() executing per
// peer at any time. EmitFunc internally takes its own writeMu (per
// walShipperEntry) for the wire-write atomic envelope; that lock is
// shorter-scoped and orthogonal to shipMu. Concurrent drive callers
// queue on shipMu — this is the §6 / P2 "single serializer per peer"
// rule, not optional.
//
// CONVENTION on cursor semantics: this implementation tracks cursor
// as LAST-EMITTED LSN. Spec §6.3 uses cursor = NEXT-TO-EMIT LSN.
// Translation: spec.cursor == internal.cursor + 1. The check
// `lsn == cursor + 1` here is identical to spec's `lsn == cursor`.
func (s *WalShipper) drive(in driveInput) error {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()

	if s.mode == ModeIdle {
		return nil
	}

	head := s.head.Head()

	// FAST-PATH tail emit: input arrived for exactly the next LSN
	// AND substrate has nothing beyond it. "The debt is exactly the
	// input" — no queue jumping, input bytes are canonical (caller
	// already persisted to substrate; bytes equal). Preserves
	// EmitKindLive on the steady-state ship hot path so wire
	// observability tags Realtime tail emits as Live.
	//
	// This is NOT a §6.3 violation. §6.3 forbids using input to fill
	// a GAP (input.lsn > cursor+1, missing intermediate LSNs). Here
	// input.lsn == cursor+1 AND head == input.lsn, so there is no
	// gap — input IS the entire next-step debt.
	if in.has && in.lsn == s.cursor+1 && head <= in.lsn {
		if err := s.emit(EmitKindLive, in.lba, in.lsn, in.data); err != nil {
			return err
		}
		s.cursor = in.lsn
		s.updateLagLocked()
		return nil
	}

	// CASE A — debt-first substrate fill. Reaches here when there's
	// a gap larger than input alone can fill, OR substrate is ahead
	// of input. Loops internally until cursor catches head OR scan
	// terminates. Even invoked from NotifyAppend, this is BACKLOG-
	// CLASS work (consensus §13 alignment): the wire kind tag is
	// EmitKindBacklog because we're filling debt from substrate.
	if err := s.driveCaseADebtFillLocked(); err != nil {
		return err
	}

	// CASE C — ∅ input + caught up.
	if !in.has {
		return nil
	}

	// Idempotent skip: input.lsn already past cursor (substrate scan
	// in CASE A may have advanced cursor over it).
	if in.lsn <= s.cursor {
		return nil
	}

	// CASE B (post-CASE-A residual) — caught up tail emit. Requires
	// lsn == cursor+1 exactly. A gap here is a producer contract
	// violation; do NOT use input bytes to fill (§6.3 / §6.9
	// NEGATIVE-EQUITY).
	if in.lsn != s.cursor+1 {
		if s.cfg.LegacyOutOfOrderEmit {
			log.Printf("WALSHIPPER-OUT-OF-ORDER replica=%s lsn=%d cursor=%d (expected cursor+1=%d; LegacyOutOfOrderEmit=true → emitting; DEPRECATED, see §6.3)",
				s.replicaID, in.lsn, s.cursor, s.cursor+1)
		} else {
			return fmt.Errorf("walshipper: tail-emit cursor gap: replica=%s lsn=%d cursor=%d (expected lsn==cursor+1; engine MUST escalate to rebuild per §6.3 / §6.9)",
				s.replicaID, in.lsn, s.cursor)
		}
	}
	if err := s.emit(EmitKindLive, in.lba, in.lsn, in.data); err != nil {
		return err
	}
	s.cursor = in.lsn
	s.updateLagLocked()
	return nil
}

// driveCaseADebtFillLocked implements §6.3 CASE A: scan substrate
// from cursor, emit each entry as backlog kind, advance cursor, loop
// until cursor == head OR scan returns no progress / error. Caller
// MUST hold shipMu.
//
// The emit kind is EmitKindBacklog regardless of which caller path
// invoked drive() — when CASE A fires, by definition we are filling
// debt from substrate, which IS backlog semantics on the wire. Any
// future code that needs to distinguish "live tail gap fill" vs
// "session backlog drain" should split the kind tag here, not at
// the dispatch level.
//
// On R1 caught-up (cursor reaches head) under ModeBacklog session,
// the assertCaughtUpAndEnableTailShipLocked transition runs as part
// of this call's tail.
func (s *WalShipper) driveCaseADebtFillLocked() error {
	head := s.head.Head()
	if s.cursor >= head {
		return nil
	}

	// Scan from current cursor. Substrate-specific behavior:
	// memorywal exclusive lower; smartwal inclusive lower with
	// ErrWALRecycled past retention. Callback runs UNDER our
	// already-held shipMu — does NOT re-acquire (deadlock).
	var hardErr error
	scanErr := s.substrate.ScanLBAs(s.cursor, func(e storage.RecoveryEntry) error {
		if s.mode == ModeIdle {
			return errStopScan
		}
		if e.LSN <= s.cursor {
			return nil // already-emitted boundary
		}
		if err := s.emit(EmitKindBacklog, e.LBA, e.LSN, e.Data); err != nil {
			hardErr = fmt.Errorf("walshipper: drive CASE A emit lsn=%d: %w", e.LSN, err)
			return err
		}
		s.cursor = e.LSN
		s.updateLagLocked()
		return nil
	})
	if hardErr != nil {
		return hardErr
	}
	if scanErr != nil {
		if errors.Is(scanErr, errStopScan) {
			// Mode flipped to Idle mid-scan — clean stop.
			return nil
		}
		if errors.Is(scanErr, storage.ErrWALRecycled) {
			// Transient at this layer; engine handles recycle via
			// session re-anchor. Do NOT propagate as fatal here.
			return nil
		}
		return fmt.Errorf("walshipper: drive CASE A scan: %w", scanErr)
	}

	// R1 transition opportunity: if a session is active and we caught
	// up to head, the spec §5 R1 procedure flips ModeBacklog →
	// ModeRealtime under shipMu (we already hold it).
	if s.mode == ModeBacklog {
		head2 := s.head.Head()
		if s.cursor >= head2 {
			_ = s.assertCaughtUpAndEnableTailShipLocked(head2)
		}
	}
	return nil
}

// drainOpportunity is the timer/nudge entry into drive(). Wraps
// drive(driveInput{has: false}) — see drive() doc for §6.3 CASE A/B/C
// dispatch.
func (s *WalShipper) drainOpportunity() {
	_ = s.drive(driveInput{has: false})
}

// errStopScan is the sentinel returned from ScanLBAs callbacks to
// stop iteration cleanly (mode changed mid-scan).
var errStopScan = errors.New("walshipper: stop scan")

// nudgeDrain wakes the timer goroutine immediately. Caller MUST be
// holding shipMu when invoking — it's a single-shot non-blocking
// signal used by NotifyAppend Realtime when debt is detected.
func (s *WalShipper) nudgeDrainLocked() {
	select {
	case s.nudgeCh <- struct{}{}:
	default:
		// Already nudged; collapse multiple wakes into one.
	}
}

// Activate transitions an Idle WalShipper into Realtime mode (steady-
// state; no session). Called by the executor once a replica's conn
// is established and steady ship is permitted.
//
// `cursor` is the highest LSN already known-applied on the replica
// (typically primary head at the moment, since this is steady-state
// catching up zero entries). If unknown, pass primary's current head.
func (s *WalShipper) Activate(cursor uint64) error {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()
	if s.mode != ModeIdle {
		return fmt.Errorf("walshipper: Activate called in mode %s (want Idle)", s.mode)
	}
	s.cursor = cursor
	s.mode = ModeRealtime
	return nil
}

// StartSession transitions WalShipper into Backlog mode for a recovery
// session. Resets cursor to fromLSN (the one allowed rewind per
// INV-MONOTONIC-CURSOR). Caller (recovery.Sender) MUST then call
// DrainBacklog to actually pump entries.
//
// fromLSN is the session lower bound (engine-determined; equals pin
// at session admission per consensus §2). After StartSession, the
// next legal frame on the wire has LSN == fromLSN+1 (matching
// receiver's checkMonotonic expected = fromLSN+1).
func (s *WalShipper) StartSession(fromLSN uint64) error {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()
	if s.sessionActive {
		return fmt.Errorf("walshipper: StartSession on already-active session (replica %s)", s.replicaID)
	}
	s.cursor = fromLSN // the one rewind, per INV-MONOTONIC-CURSOR
	s.fromLSN = fromLSN
	s.sessionActive = true
	s.mode = ModeBacklog
	s.saturationFired.Store(false)
	return nil
}

// EndSession returns the shipper to Realtime (post-session steady).
// Cursor stays where it is (typically caught up to head via
// DrainBacklog success). Called from recovery.Sender's defer or on
// session failure.
func (s *WalShipper) EndSession() {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()
	s.sessionActive = false
	// Stay at whatever cursor we reached. If DrainBacklog succeeded,
	// we're at head (Realtime). If it failed mid-way, we're somewhere
	// in (fromLSN, ...); next session can restart with appropriate
	// fromLSN. Mode Realtime is safe because steady-state callers
	// (NotifyAppend) check shipMu + cursor independently.
	s.mode = ModeRealtime
}

// NotifyAppend is the §6.3 single-mailbox entry for "primary just
// committed an LSN; here are the bytes". Per §6.3 / §6.9: this
// thin wrapper offers the input to drive() — the actual dispatch
// (debt-fill via substrate vs tail emit vs idempotent skip vs gap
// error) lives there.
//
// Behavior contract (§6.3):
//
//   - cursor < head (debt): drive runs CASE A first — substrate
//     canonical, scan-and-emit until caught up. The input bytes
//     are NOT used to fill any LSN gap (§6.9 NEGATIVE-EQUITY ban).
//     If, after CASE A, input.lsn <= new cursor → idempotent skip
//     (substrate scan covered it).
//   - cursor == head + lsn == cursor+1: CASE B — tail emit.
//   - cursor == head + lsn != cursor+1: tail-gap. Default fail-
//     closed; engine MUST escalate to rebuild. LegacyOutOfOrderEmit
//     restores pre-§6.3 log+emit behavior (transitional only).
//
// On emit error, mode stays unchanged; caller / executor decides
// degraded handling.
func (s *WalShipper) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	return s.drive(driveInput{has: true, lba: lba, lsn: lsn, data: data})
}

// DrainBacklog runs the backlog-mode pull loop until either:
//   - Cursor catches head AND R1 transition succeeds (returns nil),
//   - ctx is cancelled (returns ctx.Err() wrapped),
//   - Substrate ScanLBAs errors (returns wrapped error),
//   - EmitFunc errors (returns wrapped error).
//
// On success, mode is Realtime and subsequent NotifyAppend calls
// emit directly. On error, mode stays Backlog (caller may EndSession
// to fail-close, or retry DrainBacklog).
//
// Caller invariant: only call this between StartSession and
// EndSession. WalShipper does NOT itself coordinate session
// lifecycle.
//
// Per spec §5: R1 (AssertCaughtUpAndEnableTailShip) is the only
// path Backlog→Realtime; double-check inside shipMu after each
// scan exhausts.
func (s *WalShipper) DrainBacklog(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("walshipper: DrainBacklog cancelled: %w", ctx.Err())
		default:
		}

		// Verify mode under shipMu before each scan iteration.
		s.shipMu.Lock()
		if s.mode != ModeBacklog {
			s.shipMu.Unlock()
			if !s.sessionActive {
				return errors.New("walshipper: DrainBacklog without active session")
			}
			// Mode flipped to Realtime by another path (e.g., R1 in
			// a previous iteration). Done.
			return nil
		}
		cursor := s.cursor
		s.shipMu.Unlock()

		// Pull-and-ship one scan cycle. ScanLBAs iterates entries
		// with LSN > cursor; emit each under shipMu (so concurrent
		// NotifyAppend serializes naturally and INV-NO-DOUBLE-LIVE
		// holds).
		var emitted bool
		var hardErr error
		scanErr := s.substrate.ScanLBAs(cursor, func(e storage.RecoveryEntry) error {
			s.shipMu.Lock()
			defer s.shipMu.Unlock()
			if s.mode != ModeBacklog || e.LSN <= s.cursor {
				return nil // mode flipped or already-emitted; skip
			}
			if err := s.emit(EmitKindBacklog, e.LBA, e.LSN, e.Data); err != nil {
				hardErr = fmt.Errorf("walshipper: backlog emit lsn=%d: %w", e.LSN, err)
				return err
			}
			s.cursor = e.LSN
			s.updateLagLocked()
			emitted = true
			return nil
		})
		if hardErr != nil {
			return hardErr
		}
		if scanErr != nil {
			if errors.Is(scanErr, storage.ErrWALRecycled) {
				return fmt.Errorf("walshipper: backlog scan: WAL recycled past cursor: %w", scanErr)
			}
			return fmt.Errorf("walshipper: backlog scan: %w", scanErr)
		}

		// Try R1 transition.
		s.shipMu.Lock()
		head := s.head.Head()
		if s.cursor >= head {
			if s.assertCaughtUpAndEnableTailShipLocked(head) {
				s.shipMu.Unlock()
				return nil // transitioned; DrainBacklog done
			}
			// R1 said "no" — head moved during the check. Loop and
			// re-scan; the next iteration will emit the new entries.
		}
		s.shipMu.Unlock()

		if !emitted && s.cfg.IdleSleep > 0 {
			// No progress this iteration; brief sleep to avoid spin.
			// Ctx-respecting sleep.
			select {
			case <-ctx.Done():
				return fmt.Errorf("walshipper: DrainBacklog cancelled: %w", ctx.Err())
			case <-time.After(s.cfg.IdleSleep):
			}
		}
	}
}

// assertCaughtUpAndEnableTailShipLocked is the §5 R1 procedure.
// Caller MUST hold shipMu. Returns true on transition Backlog→Realtime
// (caller observes mode == ModeRealtime); false if a write raced
// (caller stays in Backlog and re-scans).
//
// `headObservedByCaller` is what the caller already read inside
// shipMu just before calling. If head moved between then and now,
// we treat the caller's read as stale and return false — i.e., we
// require that head has not advanced under the caller's observation
// window, which is the spec §5.2 double-check.
func (s *WalShipper) assertCaughtUpAndEnableTailShipLocked(headObservedByCaller uint64) bool {
	if s.mode != ModeBacklog {
		return false
	}
	// Re-read head atomically. If a Write landed between caller's
	// observation and this re-read, head will be > observed; in that
	// case stay in Backlog so the next loop iteration emits the new
	// entry.
	headNow := s.head.Head()
	if headNow != headObservedByCaller {
		return false
	}
	if s.cursor < headNow {
		return false
	}
	// cursor == head, double-check confirmed. Flip to Realtime.
	// From now on, NotifyAppend takes the Realtime path under shipMu.
	s.mode = ModeRealtime
	return true
}

// updateLagLocked refreshes lag sample + fires R2 hook if threshold
// crossed. Caller holds shipMu.
func (s *WalShipper) updateLagLocked() {
	head := s.head.Head()
	var lag uint64
	if head > s.cursor {
		lag = head - s.cursor
	}
	s.lagSample.Store(lag)
	if s.cfg.SaturationThreshold > 0 && lag >= s.cfg.SaturationThreshold {
		// Single-shot per session to avoid hook spam. Reset on
		// StartSession.
		if !s.saturationFired.Swap(true) && s.cfg.OnSaturation != nil {
			// Call hook outside shipMu to avoid hook code blocking
			// the hot path. We're under shipMu now; capture and
			// release before calling.
			rid := s.replicaID
			hook := s.cfg.OnSaturation
			go hook(rid, lag)
		}
	}
}

// Mode returns the current scheduler mode (diagnostic).
func (s *WalShipper) Mode() WalShipperMode {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()
	return s.mode
}

// Cursor returns the last-emitted LSN (diagnostic).
func (s *WalShipper) Cursor() uint64 {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()
	return s.cursor
}

// Lag returns the most recent (head - cursor) sample (diagnostic).
// Read with no lock (atomic); may be slightly stale.
func (s *WalShipper) Lag() uint64 {
	return s.lagSample.Load()
}

// SessionActive reports whether a session is currently active
// (diagnostic / test).
func (s *WalShipper) SessionActive() bool {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()
	return s.sessionActive
}
