package engine

import "time"

// Command is an action the engine requests from the runtime.
// Commands are execution requests, not semantic conclusions.
type Command interface {
	commandKind() string
}

// ProbeReplica: request the runtime to probe the replica's reachability.
type ProbeReplica struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	DataAddr        string
	CtrlAddr        string
}

func (ProbeReplica) commandKind() string { return "ProbeReplica" }

// StartCatchUp: request the runtime to start a catch-up session.
// May only be emitted from bounded R/S/H facts, not from transport errors.
//
// T4d-3 (G-1 §6.1 architect Option A — engine adds +1):
// `FromLSN` is the LSN the executor should start scanning FROM
// (inclusive). Engine populates as `Recovery.R + 1` at emit time —
// the "+1 to skip already-applied LSN" is engine-owned policy
// (single-site pin via INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-
// PLUS-1). Sender stays mechanical: scans from whatever the command
// says, no policy. Engine-state-source pinned by
// INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE.
type StartCatchUp struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	FromLSN         uint64 // T4d-3: engine populates as Recovery.R + 1
	TargetLSN       uint64
}

func (StartCatchUp) commandKind() string { return "StartCatchUp" }

// StartRebuild: request the runtime to start a full rebuild session.
// May only be emitted from bounded R/S/H facts, not from transport errors.
type StartRebuild struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	TargetLSN       uint64
}

func (StartRebuild) commandKind() string { return "StartRebuild" }

// InvalidateSession: request the runtime to invalidate a stale session.
type InvalidateSession struct {
	ReplicaID string
	SessionID uint64
	Reason    string
}

func (InvalidateSession) commandKind() string { return "InvalidateSession" }

// FenceAtEpoch: request the runtime to establish the given epoch on
// the replica's lineage gate. Used only on the caught-up handoff
// branch (R >= H), where no recovery traffic would otherwise carry
// the new epoch to the replica.
//
// Ack-gated: PublishHealthy must NOT follow FenceAtEpoch unless a
// FenceCompleted event confirms the replica accepted the barrier
// at this epoch. On failure, the engine leaves Reachability.
// FencedEpoch unchanged; the next probe re-triggers decide().
type FenceAtEpoch struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
}

func (FenceAtEpoch) commandKind() string { return "FenceAtEpoch" }

// PublishHealthy: declare this replica healthy for external consumption.
type PublishHealthy struct {
	ReplicaID string
}

func (PublishHealthy) commandKind() string { return "PublishHealthy" }

// PublishDegraded: declare this replica degraded for external consumption.
type PublishDegraded struct {
	ReplicaID string
	Reason    string
}

func (PublishDegraded) commandKind() string { return "PublishDegraded" }

// CommandKind returns the string name of a command for tracing.
func CommandKind(c Command) string {
	if c == nil {
		return "<nil>"
	}
	return c.commandKind()
}

// --- T4c-pre-B: unified recovery command + runtime policy ---
//
// Per design memo §7a (architect rounds 29-30): T4c introduces a
// unified `StartRecovery` semantic command that subsumes the
// `StartCatchUp` / `StartRebuild` split (and forward-accommodates
// `partial_lba` for Stage 2). Existing `StartCatchUp` / `StartRebuild`
// types remain in place as legacy aliases for transition; the
// adapter / executor layers internally bridge to `StartRecoverySession`
// (T4c-pre-B). Engine-emit migration to `StartRecovery` happens at
// T4c-3 muscle port.
//
// Round-30 refinement: unified semantic command, NOT unified runtime
// behavior. Catch-up (~0.1s) and rebuild (~30min) cannot share
// timeout / progress cadence / cancellation. RuntimePolicy carries
// per-content-kind defaults (memo §7a.1a).

// RecoveryContentKind selects which substrate primitive the recovery
// session draws from. Maps 1:1 to the §13.0a tier-1 sub-modes for
// `wal_delta` (see §5.1 substrate-specific labels) and to T5/Stage 2
// content classes for `full_extent` / `partial_lba`.
type RecoveryContentKind string

const (
	// RecoveryContentWALDelta — tier-1 content drawn from retained
	// WAL window. Substrate provides per-LSN entries (walstore
	// `wal_replay`) or per-LBA-dedup entries (smartwal
	// `state_convergence`); see memo §13.0a.
	RecoveryContentWALDelta RecoveryContentKind = "wal_delta"

	// RecoveryContentFullExtent — tier-3 content drawn from full
	// extent. T5 scope. Used when WAL retention has been exceeded
	// or the replica needs a fresh-from-scratch fill.
	RecoveryContentFullExtent RecoveryContentKind = "full_extent"

	// RecoveryContentPartialLBA — tier-2 content drawn from a
	// persistent LBA-dump archive. Stage 2 scope (post-go-live).
	// Master archive subsystem provides the dump; recovery executor
	// fetches + ships the dump-listed LBAs.
	RecoveryContentPartialLBA RecoveryContentKind = "partial_lba"
)

// DurationClass labels expected runtime envelope. Surfaces in
// projection / log / ops visibility so operators can tell at-a-glance
// whether a given recovery is a 0.1s catch-up or a 30min rebuild
// without parsing payload internals.
type DurationClass string

const (
	DurationShort        DurationClass = "short"          // milliseconds-seconds
	DurationLong         DurationClass = "long"           // minutes-hours
	DurationArchiveBound DurationClass = "archive_bound"  // includes archive/dump fetch latency
)

// CancellationMode defines how a recovery session terminates on
// non-completion. Catch-up's tight expected duration favors timeout-
// driven failure; rebuild's variable duration favors stall-driven.
type CancellationMode string

const (
	// CancelOnTimeout — fixed timeout fires → recovery aborts → tier
	// escalation per memo §7.4. Used for catch-up (`wal_delta`).
	CancelOnTimeout CancellationMode = "timeout"

	// CancelOnExplicit — only cancel on explicit caller signal. Used
	// for unbounded operations where neither timeout nor stall is a
	// good cancellation signal.
	CancelOnExplicit CancellationMode = "explicit"

	// CancelOnProgressStall — cancel if no progress for N cadences.
	// Used for rebuild (`full_extent`) and Stage-2 partial-LBA
	// (`partial_lba`) where total duration is variable but per-step
	// progress should be steady.
	CancelOnProgressStall CancellationMode = "progress_stall"
)

// RecoveryRuntimePolicy carries the per-content-kind execution
// envelope for a recovery session. Engine selects the policy when
// emitting `StartRecovery`; adapter passes through; executor honors.
//
// Per memo §7a.1a defaults (architect round-30): wal_delta is
// timeout-driven, full_extent and partial_lba are progress-stall-
// driven.
type RecoveryRuntimePolicy struct {
	ExpectedDurationClass DurationClass
	ProgressCadence       time.Duration // progress callback / heartbeat cadence
	Timeout               time.Duration // 0 = no fixed timeout (use stall detection)
	CancellationMode      CancellationMode

	// MaxRetries is the per-content-kind retry budget. Engine SessionClose
	// handler retries (re-emits StartRecovery with incremented internal
	// counter) on close-with-non-recycled-error until budget hit; on
	// budget exhaustion, escalates to NeedsRebuild. Mirrors V2's
	// `maxCatchupRetries=3` behavior at `WALShipper.CatchUpTo:286`.
	//
	// Per memo §7a.1a + G-1 §4.1 architect Option B (T4c-2 round-37):
	// retry budget lives in the runtime policy, not on the executor or
	// peer — keeps per-content-kind retry budgets explicit and
	// co-located with timeout/cadence/cancellation. ErrWALRecycled
	// (storage.ErrWALRecycled) is a tier-class change, NOT a retry
	// trigger — it bypasses the budget entirely.
	MaxRetries int
}

// StartRecovery — unified semantic command that subsumes
// `StartCatchUp` / `StartRebuild` and forward-accommodates
// `partial_lba` (Stage 2). Per memo §7a.1.
//
// Engine emits this command when a replica needs recovery (gap
// detected, peer state Degraded → recovery decision). ContentKind
// selects the substrate primitive; RuntimePolicy carries the
// per-kind execution envelope.
type StartRecovery struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	TargetLSN       uint64
	ContentKind     RecoveryContentKind
	RuntimePolicy   RecoveryRuntimePolicy
}

func (StartRecovery) commandKind() string { return "StartRecovery" }

// DefaultRuntimePolicyFor returns the per-content-kind default
// runtime policy from memo §7a.1a. Engine uses this when emitting
// `StartRecovery` without per-call overrides.
func DefaultRuntimePolicyFor(kind RecoveryContentKind) RecoveryRuntimePolicy {
	switch kind {
	case RecoveryContentWALDelta:
		return RecoveryRuntimePolicy{
			ExpectedDurationClass: DurationShort,
			Timeout:               30 * time.Second,
			ProgressCadence:       1 * time.Second,
			CancellationMode:      CancelOnTimeout,
			MaxRetries:            3, // V2 maxCatchupRetries
		}
	case RecoveryContentFullExtent:
		return RecoveryRuntimePolicy{
			ExpectedDurationClass: DurationLong,
			Timeout:               0, // no fixed timeout — rebuild may legitimately take 30min+
			ProgressCadence:       10 * time.Second,
			CancellationMode:      CancelOnProgressStall,
			MaxRetries:            0, // no retry; rebuild restart is a tier-class action
		}
	case RecoveryContentPartialLBA:
		return RecoveryRuntimePolicy{
			ExpectedDurationClass: DurationArchiveBound,
			Timeout:               0, // per-deployment tunable; depends on archive fetch SLA
			ProgressCadence:       10 * time.Second,
			CancellationMode:      CancelOnProgressStall,
			MaxRetries:            1, // archive-bound; one retry tolerates a transient fetch hiccup
		}
	}
	// Unknown kind — return zero-value; caller must validate.
	return RecoveryRuntimePolicy{}
}
