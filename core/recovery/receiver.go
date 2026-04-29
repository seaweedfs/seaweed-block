package recovery

import (
	"errors"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweed-block/core/storage"
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
}

// NewReceiver constructs a receiver bound to the replica's substrate.
// `conn` is the wire — caller's responsibility to close.
func NewReceiver(store storage.LogicalStorage, conn io.ReadWriter) *Receiver {
	return &Receiver{store: store, conn: conn}
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

		case frameWALEntry:
			if r.session == nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
					errors.New("WALEntry before SessionStart"))
			}
			kind, lba, lsn, data, decErr := decodeWALEntry(payload)
			if decErr != nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch, decErr)
			}
			if applyErr := r.session.ApplyWALEntry(kind, lba, data, lsn); applyErr != nil {
				return 0, newFailure(FailureSubstrate, PhaseRecvApply,
					fmt.Errorf("apply wal kind=%s lba=%d lsn=%d: %w", kind, lba, lsn, applyErr))
			}

		case frameBaseDone:
			if r.session == nil {
				return 0, newFailure(FailureProtocol, PhaseRecvDispatch,
					errors.New("BaseDone before SessionStart"))
			}
			r.session.MarkBaseComplete()

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
