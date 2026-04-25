package transport

import (
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

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
