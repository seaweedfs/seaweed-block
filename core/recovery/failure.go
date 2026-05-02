package recovery

import (
	"errors"
	"fmt"
)

// FailureKind classifies a recovery-session failure for the
// integration shim's retry decision. Each value documents whether
// the engine should retry the same session, escalate to a fresh
// lineage, or surface to the operator.
//
// Mapping to core/engine/RecoveryFailureKind happens at the
// integration boundary; this package keeps its own taxonomy so it
// can be unit-tested without an engine dependency.
type FailureKind int

const (
	// FailureUnknown — uncategorized error path; treat as
	// non-retryable until classified. Should never appear in well-
	// formed error returns (assert in CI).
	FailureUnknown FailureKind = iota

	// FailureWire — connection-level error: read/write returned an
	// error, peer closed mid-frame, frame size cap exceeded.
	// RETRYABLE: the engine may dial a fresh connection, mint a new
	// sessionID, and try again. The substrate is not at fault; the
	// transport state is dirty but recoverable.
	FailureWire

	// FailureProtocol — well-formed bytes that violate the wire
	// contract: duplicate SessionStart, unexpected frame type,
	// kind byte ∉ {Backlog, SessionLive}, decode error on payload.
	// NOT RETRYABLE on the same peer's same code: indicates
	// version mismatch or a bug. Engine should surface to operator.
	FailureProtocol

	// FailureSubstrate — LogicalStorage operation failed mid-session
	// (Read for base lane, ApplyEntry for either lane, Sync at
	// barrier). RETRYABLE: most disk errors are transient (EAGAIN,
	// transient EIO); persistent errors will fail the next attempt
	// the same way and the engine's retry budget will exhaust,
	// surfacing to operator.
	FailureSubstrate

	// FailureContract — session-level invariant violation that the
	// recovery package detected after the wire round-trip:
	// missing/false PrimaryWalLegOk witness, receiver TryComplete
	// returned !done at BarrierReq (caller fired barrier prematurely),
	// or legacy/bridging close explicitly opted into the old band
	// oracle and the observed achieved frontier did not satisfy it.
	// RETRYABLE with caveat: the engine should re-evaluate the
	// recovery decision (R/S/H may have shifted) before retrying.
	// Same fromLSN/frontier hint may yield the same result.
	FailureContract

	// FailureCancelled — caller cancelled the context. NOT a
	// failure of the session itself; the caller decided to abort.
	// RETRY DECISION belongs to the caller, not the engine.
	FailureCancelled

	// FailureSingleFlight — coordinator rejected StartSession
	// because the peer already has an active session
	// (INV-SINGLE-FLIGHT-PER-REPLICA). NOT RETRYABLE without first
	// tearing down the existing session via EndSession or
	// invalidation.
	FailureSingleFlight

	// FailureWALRecycled — substrate's ScanLBAs returned the
	// retention-boundary sentinel (fromLSN below S). Tier-class
	// escalation: catch-up is impossible, must rebuild from a
	// fresher pin. NOT RETRYABLE at the same fromLSN; engine must
	// pick a new anchor and start a new lineage.
	//
	// Distinct from FailurePinUnderRetention (cold-start scan
	// failure vs mid-session contract violation; see that doc).
	FailureWALRecycled

	// FailurePinUnderRetention — mid-session contract violation:
	// `coord.SetPinFloor(replicaID, floor)` was called with a
	// `floor` value below the primary's current S boundary. The
	// session was streaming fine but primary's WAL retention has
	// since advanced past where this session committed. Different
	// from WALRecycled (which fires before any data ships) — this
	// is an Invariant breach noticed during incremental ack.
	//
	// NOT RETRYABLE at the same lineage. The engine MUST invalidate
	// this session and start a new lineage with a fromLSN ≥ current
	// S. Operator alert: indicates retention policy is too
	// aggressive vs the slowest replica's progress, OR the slowest
	// replica is stalled. See INV-PIN-COMPATIBLE-WITH-RETENTION,
	// docs/recovery-pin-floor-wire.md §5.
	//
	// Architect ACK on the new-kind decision: 2026-04-29
	// (docs/recovery-pin-floor-wire.md §11 Resolution 1).
	FailurePinUnderRetention
)

func (k FailureKind) String() string {
	switch k {
	case FailureUnknown:
		return "Unknown"
	case FailureWire:
		return "Wire"
	case FailureProtocol:
		return "Protocol"
	case FailureSubstrate:
		return "Substrate"
	case FailureContract:
		return "Contract"
	case FailureCancelled:
		return "Cancelled"
	case FailureSingleFlight:
		return "SingleFlight"
	case FailureWALRecycled:
		return "WALRecycled"
	case FailurePinUnderRetention:
		return "PinUnderRetention"
	default:
		return fmt.Sprintf("FailureKind(%d)", int(k))
	}
}

// Phase tags which phase of the session was active when the failure
// surfaced. Diagnostic only — not part of the retry decision.
type Phase string

const (
	PhaseStartSession Phase = "start-session"  // before/during coord.StartSession
	PhaseSendStart    Phase = "send-start"     // SessionStart frame write
	PhaseBaseLane     Phase = "base-lane"      // base block stream
	PhaseBaseDone     Phase = "base-done"      // BaseDone frame write
	PhaseBacklog      Phase = "backlog"        // ScanLBAs + WAL stream
	PhaseDrainSeal    Phase = "drain-seal"     // bridging-sink flushAndSeal
	PhaseTransition   Phase = "transition"     // TryAdvanceToSteadyLive
	PhaseBarrierReq   Phase = "barrier-req"    // BarrierReq frame write
	PhaseBarrierResp  Phase = "barrier-resp"   // BarrierResp read/decode
	PhaseRecvDispatch Phase = "recv-dispatch"  // receiver frame loop
	PhaseRecvApply    Phase = "recv-apply"     // receiver substrate apply
	PhaseRecvSync     Phase = "recv-sync"      // receiver Sync at barrier
	PhaseRecvAckWrite Phase = "recv-ack-write" // receiver writing BaseBatchAck
	PhasePinUpdate    Phase = "pin-update"     // sender translating ack → SetPinFloor
)

// Failure is the recovery package's typed error envelope.
// Always returned from Sender.Run, Receiver.Run, and bridge methods
// when the recovery operation itself failed (vs. caller-side argument
// errors which return plain errors before the session starts).
type Failure struct {
	Kind       FailureKind
	Phase      Phase
	Underlying error
}

func (f *Failure) Error() string {
	if f == nil {
		return "<nil recovery.Failure>"
	}
	if f.Underlying != nil {
		return fmt.Sprintf("recovery: %s during %s: %v", f.Kind, f.Phase, f.Underlying)
	}
	return fmt.Sprintf("recovery: %s during %s", f.Kind, f.Phase)
}

// Unwrap exposes the underlying cause for errors.Is / errors.As.
func (f *Failure) Unwrap() error { return f.Underlying }

// Retryable advises the engine whether the same session parameters
// (fromLSN, frontier hint) may be retried after this failure. NOT a
// guarantee that retry will succeed — the engine still owns the
// retry budget and may choose to escalate to a fresh lineage.
//
// FailureContract is "retryable with caveat" — the engine should
// re-probe before deciding because R/S/H may have shifted; this
// method returns true to permit it, leaving the re-probe step to
// the engine.
func (f *Failure) Retryable() bool {
	if f == nil {
		return false
	}
	switch f.Kind {
	case FailureWire, FailureSubstrate, FailureContract:
		return true
	case FailureProtocol, FailureSingleFlight, FailureWALRecycled,
		FailurePinUnderRetention, FailureCancelled, FailureUnknown:
		return false
	default:
		return false
	}
}

// newFailure is the package-internal constructor. External code
// should never construct a Failure directly; let the sender /
// receiver wrap their errors.
func newFailure(kind FailureKind, phase Phase, underlying error) *Failure {
	return &Failure{Kind: kind, Phase: phase, Underlying: underlying}
}

// asFailure unwraps an error chain looking for a *Failure, returns
// nil if not found. Convenience for callers; equivalent to
// errors.As(err, &f).
func AsFailure(err error) *Failure {
	var f *Failure
	if errors.As(err, &f) {
		return f
	}
	return nil
}
