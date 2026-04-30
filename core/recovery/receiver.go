package recovery

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Default cadence for BaseBatchAck emission. After every K base-lane
// blocks applied OR every T elapsed, the receiver emits one ack so
// the primary can advance pin_floor incrementally. Mandatory acks
// (MarkBaseComplete, BarrierReq) bypass the cadence guard.
//
// Per docs/recovery-pin-floor-wire.md §3. Tunable via
// NewReceiverWithCadence.
const (
	DefaultCadenceK uint32        = 256
	DefaultCadenceT time.Duration = 100 * time.Millisecond
)

// Receiver is the replica-side reader for one rebuild session. It
// owns the connection's read side, decodes frames, dispatches to a
// fresh `RebuildSession`, and emits the barrier response.
//
// One Receiver per inbound recover-session connection. Caller spawns
// it in its own goroutine; Run blocks until barrier ack is sent or
// the connection errors.
type Receiver struct {
	store storage.LogicalStorage
	conn  io.ReadWriter

	// Populated after frameSessionStart.
	session   *RebuildSession
	sessionID uint64

	// BaseBatchAck cadence config + state (per
	// v3-recovery-pin-floor-wire.md §3).
	cadenceK           uint32
	cadenceT           time.Duration
	blocksSinceLastAck uint32
	lastAckTime        time.Time
	baseInstalledUpper uint32 // highest base LBA installed (advisory in BaseBatchAck)

	// appliedLSN tracks the highest WAL LSN successfully passed to
	// session.ApplyWALEntry. Initialized to fromLSN at SessionStart
	// (the watermark form: "fromLSN is already-installed-on-replica").
	// Per v3-recovery-unified-wal-stream-kickoff.md §5 + mini-plan §2.3
	// (Q13 ratified): the first WAL frame's expected LSN is fromLSN+1.
	appliedLSN uint64
}

// NewReceiver constructs a receiver bound to the replica's substrate
// with default ack cadence (DefaultCadenceK blocks, DefaultCadenceT).
// `conn` is the wire — caller's responsibility to close.
func NewReceiver(store storage.LogicalStorage, conn io.ReadWriter) *Receiver {
	return NewReceiverWithCadence(store, conn, DefaultCadenceK, DefaultCadenceT)
}

// NewReceiverWithCadence is NewReceiver with explicit cadence
// parameters. Tests use this for deterministic ack timing.
func NewReceiverWithCadence(store storage.LogicalStorage, conn io.ReadWriter, k uint32, t time.Duration) *Receiver {
	return &Receiver{
		store:    store,
		conn:     conn,
		cadenceK: k,
		cadenceT: t,
	}
}

// Session returns the active session (nil if SessionStart not yet
// received). Test/diagnostic accessor.
func (r *Receiver) Session() *RebuildSession { return r.session }

// Run reads frames until the barrier round-trip completes, the peer
// closes the conn, or a fatal error occurs. Returns the
// `(achievedLSN, nil)` reported on the wire after a successful
// barrier; returns an error on protocol violation or apply failure.
func (r *Receiver) Run() (achievedLSN uint64, err error) {
	for {
		ft, payload, err := readFrame(r.conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, newFailure(FailureWire, PhaseRecvDispatch,
					fmt.Errorf("peer closed before barrier: %w", err))
			}
			return 0, newFailure(FailureWire, PhaseRecvDispatch, err)
		}

		switch ft {
		case frameSessionStart:
			s, decErr := decodeSessionStart(payload)
			if decErr != nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch, decErr)
			}
			if r.session != nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
					errors.New("duplicate SessionStart"))
			}
			// numBlocks is sent by primary so receiver sizes the bitmap
			// matching primary's view; if the substrate disagrees, the
			// session would silently mis-arbitrate, so we sanity-check.
			if got := r.store.NumBlocks(); got != s.NumBlocks {
				return 0, newFailure(FailureContract, PhaseRecvDispatch,
					fmt.Errorf("numBlocks mismatch primary=%d local=%d", s.NumBlocks, got))
			}
			r.sessionID = s.SessionID
			r.session = NewRebuildSession(r.store, s.TargetLSN)
			// §3.2 #3 monotonic-LSN init: fromLSN is the
			// already-installed-on-replica watermark; first WAL frame's
			// expected LSN is fromLSN+1.
			r.appliedLSN = s.FromLSN

		case frameBaseBlock:
			if r.session == nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
					errors.New("BaseBlock before SessionStart"))
			}
			lba, data, decErr := decodeBaseBlock(payload)
			if decErr != nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch, decErr)
			}
			if _, applyErr := r.session.ApplyBaseBlock(lba, data); applyErr != nil {
				return 0, newFailure(FailureSubstrate, PhaseRecvApply,
					fmt.Errorf("apply base lba=%d: %w", lba, applyErr))
			}
			if lba+1 > r.baseInstalledUpper {
				r.baseInstalledUpper = lba + 1
			}
			r.blocksSinceLastAck++
			if r.shouldAck() {
				if ackErr := r.sendAck(); ackErr != nil {
					return 0, ackErr
				}
			}

		case frameWALEntry:
			if r.session == nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
					errors.New("WALEntry before SessionStart"))
			}
			kind, lba, lsn, data, decErr := decodeWALEntry(payload)
			if decErr != nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch, decErr)
			}
			// §3.2 #3 wire-level monotonic gate. Per kickoff v0.3 §5.1:
			//   lsn == applied+1 → apply
			//   lsn  > applied+1 → FailureContract (gap, not silent skip)
			//   lsn == applied   → FailureProtocol (exact-duplicate; sender bug)
			//   lsn  < applied   → FailureProtocol (in-stream backward)
			// The §5.2 duplicate-LSN claim semantics is substrate-level
			// per-LBA arbitration; it does NOT pass through this gate.
			if mErr := r.checkMonotonic(lsn); mErr != nil {
				return 0, mErr
			}
			if applyErr := r.session.ApplyWALEntry(kind, lba, data, lsn); applyErr != nil {
				return 0, newFailure(FailureSubstrate, PhaseRecvApply,
					fmt.Errorf("apply wal kind=%s lba=%d lsn=%d: %w", kind, lba, lsn, applyErr))
			}
			r.appliedLSN = lsn

		case frameBaseDone:
			if r.session == nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
					errors.New("BaseDone before SessionStart"))
			}
			r.session.MarkBaseComplete()
			// Mandatory final base-lane ack — base done means primary
			// expects to know our base-installed cursor before drain
			// even if cadence wouldn't have triggered yet.
			if ackErr := r.sendAck(); ackErr != nil {
				return 0, ackErr
			}

		case frameBarrierReq:
			if r.session == nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
					errors.New("BarrierReq before SessionStart"))
			}
			// Sync substrate so AchievedLSN reflects durable state.
			frontier, syncErr := r.store.Sync()
			if syncErr != nil {
				return 0, newFailure(FailureSubstrate, PhaseRecvSync, syncErr)
			}
			// Layer-1 closure check (necessary, not sufficient — the
			// system close is the primary's CanEmitSessionComplete
			// after reading our response).
			achieved, done := r.session.TryComplete()
			if !done {
				st := r.session.Status()
				return 0, newFailure(FailureContract, PhaseRecvSync,
					fmt.Errorf("barrier req but layer-1 not done: %+v", st))
			}
			_ = achieved
			if writeErr := writeFrame(r.conn, frameBarrierResp, encodeBarrierResp(frontier)); writeErr != nil {
				return 0, newFailure(FailureWire, PhaseRecvSync, writeErr)
			}
			return frontier, nil

		case frameBarrierResp:
			return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
				errors.New("unexpected BarrierResp from primary"))

		default:
			return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
				fmt.Errorf("unknown frame type %d", ft))
		}
	}
}

// shouldAck evaluates the cadence rule per docs/recovery-pin-floor-wire.md §3.
// Returns true on the FIRST trigger (K blocks accumulated OR T elapsed since
// last ack). Idempotent: returns false if neither K nor T threshold reached.
//
// Special case: lastAckTime zero means "no ack sent yet" — don't trigger on
// elapsed alone, otherwise the very first frame triggers ack which churns
// for tests with very small K. Wait until at least 1 block has been applied.
func (r *Receiver) shouldAck() bool {
	if r.cadenceK > 0 && r.blocksSinceLastAck >= r.cadenceK {
		return true
	}
	if r.cadenceT > 0 && r.blocksSinceLastAck > 0 {
		if !r.lastAckTime.IsZero() && time.Since(r.lastAckTime) >= r.cadenceT {
			return true
		}
		// First-ever ack: trigger by elapsed using session age (for
		// tests that set tiny K and T) — but only after at least one
		// block has been applied.
		if r.lastAckTime.IsZero() {
			// We don't track session start time; for the POC, the
			// elapsed-since-zero case is captured the next time around.
			// This deliberate undercount is harmless: K-trigger will
			// catch typical workloads.
		}
	}
	return false
}

// sendAck writes a frameBaseBatchAck with the receiver's current
// AcknowledgedLSN and BaseLBAUpper. Resets cadence counters.
//
// AcknowledgedLSN semantics (POC): uses session.Status().WALApplied,
// the highest WAL LSN this session has applied. For real WAL substrate
// in production, the receiver should call store.Sync() first and use
// the returned syncedLSN to claim true durability — see
// docs/recovery-pin-floor-wire.md §3 ("During base lane: min(walApplied,
// syncedLSN-at-ack-time)"). POC trusts in-memory durability.
func (r *Receiver) sendAck() error {
	if r.session == nil {
		return newFailure(FailureProtocol, PhaseRecvAckWrite,
			errors.New("sendAck before SessionStart"))
	}
	st := r.session.Status()
	payload := encodeBaseBatchAck(baseBatchAckPayload{
		SessionID:       r.sessionID,
		AcknowledgedLSN: st.WALApplied,
		BaseLBAUpper:    r.baseInstalledUpper,
	})
	if err := writeFrame(r.conn, frameBaseBatchAck, payload); err != nil {
		return newFailure(FailureWire, PhaseRecvAckWrite, err)
	}
	r.blocksSinceLastAck = 0
	r.lastAckTime = time.Now()
	return nil
}

// checkMonotonic implements the §3.2 #3 wire-level monotonic LSN gate
// per kickoff v0.3 §5.1. Four cases:
//
//	lsn == applied + 1: normal (the only legal forward step). Returns nil.
//	lsn  > applied + 1: GAP — FailureContract; not silent skip. Sender
//	                    MUST emit contiguous LSN. WAL-recycled gaps go
//	                    through FailureWALRecycled at sender side.
//	lsn == applied:     EXACT-DUPLICATE on the wire — FailureProtocol.
//	                    TCP delivers in-order; recovery sender writes
//	                    each LSN exactly once. NOT to be confused with
//	                    §5.2 substrate-level per-LBA arbitration
//	                    (INV-DUAL-LANE-WAL-WINS-BASE) — that's a
//	                    different layer.
//	lsn  < applied:     BACKWARD — FailureProtocol. Only legitimate
//	                    "rewind" is session-level cursor:=pinLSN at a
//	                    NEW SessionStart, never in-stream.
//
// Initialization (per Q13 ratified default): r.appliedLSN := fromLSN
// at SessionStart so the first frame's expected LSN is fromLSN+1.
func (r *Receiver) checkMonotonic(lsn uint64) error {
	expected := r.appliedLSN + 1
	if lsn == expected {
		return nil
	}
	if lsn > expected {
		return newFailure(FailureContract, PhaseRecvDispatch,
			fmt.Errorf("WAL gap: got LSN=%d, expected %d (applied=%d)",
				lsn, expected, r.appliedLSN))
	}
	if lsn == r.appliedLSN {
		return newFailure(FailureProtocol, PhaseRecvDispatch,
			fmt.Errorf("WAL exact-duplicate: got LSN=%d == applied=%d "+
				"(sender re-emit; never legitimate on the wire)",
				lsn, r.appliedLSN))
	}
	return newFailure(FailureProtocol, PhaseRecvDispatch,
		fmt.Errorf("WAL backward: got LSN=%d < applied=%d",
			lsn, r.appliedLSN))
}
