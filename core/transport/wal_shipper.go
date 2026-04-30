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
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
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
type EmitFunc func(lba uint32, lsn uint64, data []byte) error

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
}

// DefaultWalShipperConfig returns production defaults.
func DefaultWalShipperConfig() WalShipperConfig {
	return WalShipperConfig{
		IdleSleep:           5 * time.Millisecond,
		SaturationThreshold: 100_000,
		OnSaturation:        nil,
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
func NewWalShipperWithOptions(replicaID string, head HeadSource, substrate storage.LogicalStorage, emit EmitFunc, cfg WalShipperConfig) *WalShipper {
	if cfg.IdleSleep <= 0 {
		cfg.IdleSleep = 5 * time.Millisecond
	}
	return &WalShipper{
		replicaID: replicaID,
		head:      head,
		substrate: substrate,
		emit:      emit,
		cfg:       cfg,
		mode:      ModeIdle,
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

// NotifyAppend is called by primary's write path for every committed
// LSN. In Realtime mode: emits immediately via EmitFunc, advances
// cursor. In Backlog mode: just updates lag sample (the backlog drain
// loop will pick the new entry via its next ScanLBAs cycle). In
// Idle mode: no-op (replica isn't active).
//
// Returns whatever EmitFunc returns in Realtime mode; nil otherwise.
// On emit error, mode stays Realtime (caller / executor decides
// degraded handling).
func (s *WalShipper) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	s.shipMu.Lock()
	defer s.shipMu.Unlock()

	switch s.mode {
	case ModeIdle:
		return nil
	case ModeBacklog:
		// Update lag sample; backlog loop will pick up via ScanLBAs.
		s.updateLagLocked()
		return nil
	case ModeRealtime:
		if lsn <= s.cursor {
			// Idempotent: write already accounted for (e.g. retry).
			return nil
		}
		if err := s.emit(lba, lsn, data); err != nil {
			return err
		}
		s.cursor = lsn
		s.updateLagLocked()
		return nil
	default:
		return fmt.Errorf("walshipper: NotifyAppend in unknown mode %s", s.mode)
	}
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
			if err := s.emit(e.LBA, e.LSN, e.Data); err != nil {
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
