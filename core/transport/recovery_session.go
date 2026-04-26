package transport

import (
	"errors"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// classifyRecoveryFailure is the T4d-1 boundary mapper: extracts the
// substrate-side `*storage.RecoveryFailure` typed error via
// `errors.As` and maps its `StorageRecoveryFailureKind` to the
// engine-owned `engine.RecoveryFailureKind`. Engine MUST NOT import
// core/storage; transport does the mapping at the boundary because
// transport already imports both packages.
//
// Architect-locked (kickoff §9 / mini-plan v0.3): storage owns
// substrate-side classification; transport maps; engine consumes its
// own type. See `feedback_engine_no_storage_import.md`.
//
// Fallback heuristics for errors that aren't typed `*RecoveryFailure`:
//   - errSessionInvalidated → RecoveryFailureSessionInvalidated
//   - error mentioning "target" + "not reached" → RecoveryFailureTargetNotReached
//     (catch-up sender's existing target-not-reached error wraps;
//     T4e/G5 may add a typed kind for this so the substring match
//     can go away too)
//   - everything else → RecoveryFailureTransport (retryable per
//     RecoveryRuntimePolicy)
//
// Called by: BlockExecutor.finishSession at session close path.
// Owns: nothing; pure mapping.
// Borrows: err is consumed read-only (errors.As doesn't mutate).
func classifyRecoveryFailure(err error) engine.RecoveryFailureKind {
	if err == nil {
		return engine.RecoveryFailureUnknown
	}
	// Typed substrate failure — preferred path.
	var rf *storage.RecoveryFailure
	if errors.As(err, &rf) {
		switch rf.Kind {
		case storage.StorageRecoveryFailureWALRecycled:
			return engine.RecoveryFailureWALRecycled
		case storage.StorageRecoveryFailureSubstrateIO:
			return engine.RecoveryFailureSubstrateIO
		}
		return engine.RecoveryFailureTransport
	}
	// Session invalidation is a transport-internal sentinel.
	if errors.Is(err, errSessionInvalidated) {
		return engine.RecoveryFailureSessionInvalidated
	}
	// Catch-up sender's existing "target N not reached" wrap. T4e/G5
	// candidate to make typed (would add a sender-side typed-error
	// envelope mirroring storage.RecoveryFailure). For now: substring
	// inside transport is acceptable (transport owns its own messages;
	// engine still branches on the typed FailureKind we set here).
	msg := err.Error()
	if strings.Contains(msg, "target") && strings.Contains(msg, "not reached") {
		return engine.RecoveryFailureTargetNotReached
	}
	return engine.RecoveryFailureTransport
}

// StartRecoverySession is the unified recovery dispatch entry per
// design memo §7a (T4c-pre-B). One semantic command, per-content-kind
// runtime envelope.
//
// Per round-30 architect refinement: the unification is at the COMMAND
// boundary, not at runtime. ContentKind selects which substrate
// primitive (and therefore which sender goroutine) executes; policy
// carries the per-kind execution envelope.
//
// Today (T4c-pre-B scope):
//   - wal_delta   → bridges to existing doCatchUp (catch-up ship+barrier)
//   - full_extent → bridges to existing doRebuild (rebuild stream)
//   - partial_lba → not implemented (Stage 2; returns explicit error)
//
// The legacy `StartCatchUp` / `StartRebuild` methods remain as thin
// wrappers (see catchup_sender.go and rebuild_sender.go) and dispatch
// through this method. T4c-3 muscle port migrates engine emission
// from `StartCatchUp` / `StartRebuild` to `StartRecovery`; at that
// point the legacy methods can be removed.
//
// RuntimePolicy honoring (current scope): the existing senders use
// the package-level `recoveryConnTimeout` constant. Per-policy
// timeout splitting is deferred to T4c-3 (memo §7a.1a). This method
// validates the policy is non-zero / has a known cancellation mode
// but does not yet enforce per-call timeouts. POC report
// `v3-phase-15-t4c-pre-poc-report.md` §3.b documents this gap.
func (e *BlockExecutor) StartRecoverySession(
	replicaID string,
	sessionID, epoch, endpointVersion, targetLSN uint64,
	contentKind engine.RecoveryContentKind,
	policy engine.RecoveryRuntimePolicy,
) error {
	if err := validateRecoveryPolicy(contentKind, policy); err != nil {
		return err
	}

	switch contentKind {
	case engine.RecoveryContentWALDelta:
		// Bridge to existing catch-up sender.
		return e.StartCatchUp(replicaID, sessionID, epoch, endpointVersion, targetLSN)

	case engine.RecoveryContentFullExtent:
		// Bridge to existing rebuild sender.
		return e.StartRebuild(replicaID, sessionID, epoch, endpointVersion, targetLSN)

	case engine.RecoveryContentPartialLBA:
		// Stage 2 — archive-driven LBA dump fetch. Not implemented
		// in T4c-pre-B. Engine MUST NOT emit StartRecovery with this
		// kind during Stage 1; if it does, fail closed.
		return fmt.Errorf(
			"transport: StartRecoverySession partial_lba is Stage 2 scope (replica=%s session=%d)",
			replicaID, sessionID,
		)

	default:
		return fmt.Errorf(
			"transport: StartRecoverySession unknown content kind %q (replica=%s session=%d)",
			contentKind, replicaID, sessionID,
		)
	}
}

// validateRecoveryPolicy checks the runtime policy is well-formed
// for the given content kind. Round-30 invariant: every recovery
// session carries an envelope; zero-value policies are a programmer
// error (engine should always populate via DefaultRuntimePolicyFor).
func validateRecoveryPolicy(
	kind engine.RecoveryContentKind,
	policy engine.RecoveryRuntimePolicy,
) error {
	if policy.CancellationMode == "" {
		return fmt.Errorf(
			"transport: StartRecoverySession kind=%q missing cancellation mode",
			kind,
		)
	}
	if policy.ExpectedDurationClass == "" {
		return fmt.Errorf(
			"transport: StartRecoverySession kind=%q missing expected duration class",
			kind,
		)
	}
	// Per memo §7a.1a: wal_delta uses CancelOnTimeout (Timeout > 0
	// required); full_extent / partial_lba use CancelOnProgressStall
	// (Timeout MAY be 0). Validate the wal_delta path; leave the
	// stall-driven paths permissive — the executor falls back to
	// recoveryConnTimeout for the per-frame deadline.
	if kind == engine.RecoveryContentWALDelta &&
		policy.CancellationMode == engine.CancelOnTimeout &&
		policy.Timeout == 0 {
		return fmt.Errorf(
			"transport: StartRecoverySession wal_delta with CancelOnTimeout requires Timeout > 0",
		)
	}
	return nil
}
