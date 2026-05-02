package adapter

import "github.com/seaweedfs/seaweed-block/core/engine"

// CommandExecutor is the interface that runtime muscles must satisfy.
// Each method corresponds to one engine command. The adapter calls
// these after engine.Apply() emits commands.
//
// Implementations wrap V2 transport muscles (WALShipper, RebuildTransport,
// etc.) but must NOT make semantic decisions. They execute and report.
//
// Session lifecycle: StartCatchUp/StartRebuild run asynchronously.
// The executor MUST call the registered OnSessionClose callback when
// the session completes or fails. Without this, terminal truth never
// reaches the engine and the replica stays stuck non-healthy.
type CommandExecutor interface {
	// SetOnSessionStart registers the callback for real execution start.
	// The adapter calls this during construction. The executor MUST call
	// it when a session has actually begun running, rather than when the
	// engine merely issued the Start* command.
	SetOnSessionStart(fn OnSessionStart)

	// SetOnSessionClose registers the callback for terminal session truth.
	// The adapter calls this during construction. The executor MUST call
	// it when any session completes or fails.
	SetOnSessionClose(fn OnSessionClose)

	// Probe dials the replica and collects transport/recovery facts.
	// Returns a ProbeResult with success/failure and R/S/H boundaries.
	// Must NOT decide recovery class — that's the engine's job.
	//
	// Per T4c-1 (architect Option D): sessionID is a transient probe
	// session minted by the adapter (parallel to FenceAtEpoch). The
	// executor uses it to construct the wire-level RecoveryLineage for
	// the symmetric ProbeReq + ProbeResponse pair. The probe sessionID
	// is NOT registered in any executor session table, NOT represented
	// in engine session truth, and NOT carried in OnSessionStart /
	// OnSessionClose callbacks. Probe is non-mutating.
	Probe(replicaID, dataAddr, ctrlAddr string, sessionID, epoch, endpointVersion uint64) ProbeResult

	// StartCatchUp begins a catch-up session with the given sessionID,
	// fromLSN (T4d-3), and frontierHint.
	// The sessionID is assigned by the adapter (matches the engine's session truth).
	// fromLSN is the LSN the executor scans FROM (inclusive); engine
	// populates as Recovery.R + 1 per G-1 §6.1 architect Option A.
	// frontierHint is advisory/compat lineage data, not a completion
	// predicate and not the WAL scan stop line.
	// Runs asynchronously; completion/failure MUST be reported via the
	// registered OnSessionClose callback using the SAME sessionID.
	StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, fromLSN, frontierHint uint64) error

	// StartRebuild begins a full rebuild session with the given sessionID
	// and frontierHint. The transport chooses/syncs the BASE pin itself.
	// Same contract as StartCatchUp.
	StartRebuild(replicaID string, sessionID, epoch, endpointVersion, frontierHint uint64) error

	// StartRecoverySession begins a recovery session for the given content
	// kind under the given runtime policy. Per design memo §7a (T4c-pre-B):
	// unifies catch-up / rebuild / partial-LBA execution behind one entry.
	//
	// ContentKind selects the substrate primitive:
	//   - RecoveryContentWALDelta   → tier-1 WAL-window scan
	//                                 (catch-up; subsumes StartCatchUp).
	//   - RecoveryContentFullExtent → tier-3 full-extent fill
	//                                 (rebuild; subsumes StartRebuild).
	//   - RecoveryContentPartialLBA → tier-2 archive dump (Stage 2;
	//                                 implementations may return error).
	//
	// RuntimePolicy carries the per-content-kind execution envelope
	// (timeout / progress cadence / cancellation mode); see engine
	// `DefaultRuntimePolicyFor`. The executor MUST honor the policy
	// (no hard-coded timeouts that contradict it).
	//
	// Async lifecycle: same contract as StartCatchUp / StartRebuild —
	// completion / failure MUST be reported via the registered
	// OnSessionClose callback using the SAME sessionID.
	//
	// Transition note (T4c-pre-B): existing `StartCatchUp` /
	// `StartRebuild` remain as the wire-level entry points until
	// T4c-3 muscle port migrates engine emission to `StartRecovery`.
	// Adapters / executors MAY implement `StartCatchUp` and
	// `StartRebuild` as thin wrappers that build a `StartRecovery`
	// command and dispatch through `StartRecoverySession`.
	StartRecoverySession(
		replicaID string,
		sessionID, epoch, endpointVersion, frontierHint uint64,
		contentKind engine.RecoveryContentKind,
		policy engine.RecoveryRuntimePolicy,
	) error

	// InvalidateSession cancels an active session.
	InvalidateSession(replicaID string, sessionID uint64, reason string)

	// PublishHealthy reports this replica as healthy.
	PublishHealthy(replicaID string)

	// PublishDegraded reports this replica as degraded.
	PublishDegraded(replicaID string, reason string)

	// Fence runs a one-barrier exchange at the given lineage
	// (epoch, endpointVersion, sessionID). No catch-up, no
	// rebuild — a single MsgBarrierReq that the replica accepts
	// (advancing its activeLineage) or rejects. Runs
	// asynchronously; outcome MUST be reported via the registered
	// OnFenceComplete callback exactly once.
	Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error

	// SetOnFenceComplete registers the callback for fence outcomes.
	// Success=true means the replica acked at this epoch;
	// Success=false with FailReason means the fence did not
	// complete. Retry is probe-driven — the callback MUST NOT
	// retry internally.
	SetOnFenceComplete(fn OnFenceComplete)
}

// OnSessionClose is the callback signature for session completion/failure.
// The adapter registers this with the executor so terminal truth flows
// back through the engine's explicit close path.
type OnSessionClose func(SessionCloseResult)

// OnSessionStart is the callback signature for real session start.
// The adapter registers this with the executor so SessionStarted is
// tied to actual execution start instead of command issuance alone.
type OnSessionStart func(SessionStartResult)

// OnFenceComplete is the callback signature for FenceAtEpoch outcomes.
// Fires exactly once per Fence call. Success=true advances
// Reachability.FencedEpoch; Success=false leaves it unchanged and
// relies on the next probe to re-trigger.
type OnFenceComplete func(FenceResult)

// FenceResult is the fence outcome reported back to the adapter.
type FenceResult struct {
	ReplicaID       string
	SessionID       uint64
	Epoch           uint64
	EndpointVersion uint64
	Success         bool
	FailReason      string
}
