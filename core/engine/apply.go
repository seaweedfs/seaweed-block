package engine

import "fmt"

// ApplyResult holds the output of one Apply call.
type ApplyResult struct {
	Commands   []Command
	Projection ReplicaProjection
	Trace      []TraceEntry
}

// TraceEntry records one decision step for diagnosis.
type TraceEntry struct {
	Step   string
	Detail string
}

// Apply is the deterministic semantic reducer.
// Given current state and an event, it produces:
//   - updated state (mutated in place)
//   - commands to emit
//   - projection for operators
//   - trace for diagnosis
//
// Apply is pure: same (state, event) always yields the same result.
// No goroutines, no time, no randomness.
func Apply(st *ReplicaState, ev Event) ApplyResult {
	var result ApplyResult
	trace := func(step, detail string) {
		result.Trace = append(result.Trace, TraceEntry{Step: step, Detail: detail})
	}

	trace("event", EventKind(ev))

	switch e := ev.(type) {
	case AssignmentObserved:
		// Assignment is the only event that can SET the replicaID.
		applyAssignment(st, e, &result, trace)
	case EndpointObserved:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applyEndpoint(st, e, &result, trace)
	case ReplicaRemoved:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applyRemoval(st, e, &result, trace)
	case ProbeSucceeded:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applyProbeSucceeded(st, e, &result, trace)
	case ProbeFailed:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applyProbeFailed(st, e, &result, trace)
	case RecoveryFactsObserved:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applyRecoveryFacts(st, e, &result, trace)
	case SessionPrepared:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applySessionPrepared(st, e, &result, trace)
	case SessionStarted:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applySessionStarted(st, e, &result, trace)
	case SessionProgressObserved:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applySessionProgress(st, e, &result, trace)
	case SessionClosedCompleted:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applySessionCompleted(st, e, &result, trace)
	case SessionClosedFailed:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applySessionFailed(st, e, &result, trace)
	case SessionInvalidated:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applySessionInvalidated(st, e, &result, trace)
	case FenceCompleted:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applyFenceCompleted(st, e, &result, trace)
	case FenceFailed:
		if !checkReplicaID(st, e.ReplicaID, &result, trace) {
			break
		}
		applyFenceFailed(st, e, &result, trace)
	default:
		trace("unknown_event", "ignored")
	}

	// Derive publication truth.
	derivePublication(st, trace)

	// Derive projection (always, after every event).
	result.Projection = DeriveProjection(st)

	return result
}

// checkReplicaID rejects events targeted at a different replica.
// Returns false if the event should be dropped.
func checkReplicaID(st *ReplicaState, eventReplicaID string, r *ApplyResult, trace func(string, string)) bool {
	if st.Identity.ReplicaID == "" {
		// Engine not yet initialized — accept (assignment will set it).
		return true
	}
	if eventReplicaID == st.Identity.ReplicaID {
		return true
	}
	trace("wrong_replica", "event for "+eventReplicaID+", engine owns "+st.Identity.ReplicaID)
	return false
}

func staleTransportObservation(st *ReplicaState, endpointVersion, transportEpoch uint64) (bool, string) {
	if endpointVersion > 0 && endpointVersion < st.Identity.EndpointVersion {
		return true, "endpoint version too old"
	}
	if transportEpoch > 0 && transportEpoch < st.Identity.Epoch {
		return true, "transport epoch too old"
	}
	if endpointVersion > 0 &&
		endpointVersion == st.Reachability.ObservedEndpointVersion &&
		transportEpoch > 0 &&
		transportEpoch < st.Reachability.TransportEpoch {
		return true, "transport epoch rolled backward"
	}
	return false, ""
}

// --- Identity ---

func applyAssignment(st *ReplicaState, e AssignmentObserved, r *ApplyResult, trace func(string, string)) {
	if e.Epoch == 0 || e.EndpointVersion == 0 {
		trace("invalid_assignment", "epoch and endpoint version must be >= 1")
		return
	}

	// Monotonic identity check: reject stale epoch OR same-epoch with
	// older endpoint version. Prevents rolling endpoint truth backward.
	if e.Epoch < st.Identity.Epoch {
		trace("stale_assignment", "epoch too old")
		return
	}
	if e.Epoch == st.Identity.Epoch && e.EndpointVersion < st.Identity.EndpointVersion {
		trace("stale_assignment", "same epoch but older endpoint version")
		return
	}

	identityChanged := e.Epoch > st.Identity.Epoch ||
		e.EndpointVersion > st.Identity.EndpointVersion

	st.Identity.VolumeID = e.VolumeID
	st.Identity.ReplicaID = e.ReplicaID
	st.Identity.Epoch = e.Epoch
	st.Identity.EndpointVersion = e.EndpointVersion
	st.Identity.DataAddr = e.DataAddr
	st.Identity.CtrlAddr = e.CtrlAddr
	st.Identity.MemberPresent = true
	trace("identity_updated", "member_present=true")

	// If identity advanced, invalidate stale reachability/session.
	// Session truth is cleared immediately — no waiting for adapter to
	// process InvalidateSession. This prevents hasActiveSession() from
	// blocking the new recovery decision path.
	if identityChanged {
		if st.Session.Phase == PhaseRunning || st.Session.Phase == PhaseStarting {
			r.Commands = append(r.Commands, InvalidateSession{
				ReplicaID: e.ReplicaID,
				SessionID: st.Session.SessionID,
				Reason:    "identity_changed",
			})
			trace("invalidate_session", "identity changed under active session")
		}
		st.Session = SessionTruth{} // clear immediately, don't wait for adapter
		st.Reachability = ReachabilityTruth{Status: ProbeUnknown}
		st.Recovery = RecoveryTruth{Decision: DecisionUnknown}
		trace("reachability_reset", "identity changed")
	}

	// Request probe if not reachable.
	if st.Reachability.Status != ProbeReachable {
		r.Commands = append(r.Commands, ProbeReplica{
			ReplicaID:       e.ReplicaID,
			Epoch:           e.Epoch,
			EndpointVersion: e.EndpointVersion,
			DataAddr:        e.DataAddr,
			CtrlAddr:        e.CtrlAddr,
		})
		trace("probe_requested", "not reachable after assignment")
	}
}

func applyEndpoint(st *ReplicaState, e EndpointObserved, r *ApplyResult, trace func(string, string)) {
	if e.EndpointVersion == 0 {
		trace("invalid_endpoint", "endpoint version must be >= 1")
		return
	}
	if e.EndpointVersion <= st.Identity.EndpointVersion {
		trace("stale_endpoint", "version too old")
		return
	}
	st.Identity.EndpointVersion = e.EndpointVersion
	st.Identity.DataAddr = e.DataAddr
	st.Identity.CtrlAddr = e.CtrlAddr
	st.Reachability = ReachabilityTruth{Status: ProbeUnknown}
	trace("endpoint_updated", "reachability reset")

	r.Commands = append(r.Commands, ProbeReplica{
		ReplicaID:       e.ReplicaID,
		Epoch:           st.Identity.Epoch,
		EndpointVersion: e.EndpointVersion,
		DataAddr:        e.DataAddr,
		CtrlAddr:        e.CtrlAddr,
	})
}

func applyRemoval(st *ReplicaState, e ReplicaRemoved, r *ApplyResult, trace func(string, string)) {
	st.Identity.MemberPresent = false
	if st.Session.Phase == PhaseRunning || st.Session.Phase == PhaseStarting {
		r.Commands = append(r.Commands, InvalidateSession{
			ReplicaID: e.ReplicaID,
			SessionID: st.Session.SessionID,
			Reason:    "replica_removed",
		})
	}
	st.Session = SessionTruth{}
	st.Recovery = RecoveryTruth{Decision: DecisionNone}
	st.Publication = PublicationTruth{}
	trace("removed", e.Reason)
}

// --- Reachability ---

func applyProbeSucceeded(st *ReplicaState, e ProbeSucceeded, r *ApplyResult, trace func(string, string)) {
	if stale, reason := staleTransportObservation(st, e.EndpointVersion, e.TransportEpoch); stale {
		trace("stale_probe", reason)
		return
	}
	st.Reachability.Status = ProbeReachable
	st.Reachability.LastContactKind = ContactProbe
	st.Reachability.ObservedEndpointVersion = e.EndpointVersion
	st.Reachability.TransportEpoch = e.TransportEpoch
	trace("reachable", "probe succeeded")

	// If recovery facts were already observed, becoming reachable should reopen
	// the bounded decision path immediately instead of depending on event order.
	if st.Recovery.R != 0 || st.Recovery.S != 0 || st.Recovery.H != 0 {
		decide(st, r, trace)
	}
}

func applyProbeFailed(st *ReplicaState, e ProbeFailed, r *ApplyResult, trace func(string, string)) {
	if stale, reason := staleTransportObservation(st, e.EndpointVersion, e.TransportEpoch); stale {
		trace("stale_probe_failed", reason)
		return
	}
	st.Reachability.Status = ProbeUnreachable
	trace("unreachable", e.Reason)

	// Reachability loss does NOT directly force rebuild.
	// It may trigger a re-probe or degradation, but recovery decision
	// comes only from bounded R/S/H facts.
	r.Commands = append(r.Commands, PublishDegraded{
		ReplicaID: e.ReplicaID,
		Reason:    "probe_failed: " + e.Reason,
	})
}

// --- Recovery facts ---

func applyRecoveryFacts(st *ReplicaState, e RecoveryFactsObserved, r *ApplyResult, trace func(string, string)) {
	if stale, reason := staleTransportObservation(st, e.EndpointVersion, e.TransportEpoch); stale {
		trace("stale_recovery_facts", reason)
		return
	}
	st.Recovery.R = e.R
	st.Recovery.S = e.S
	st.Recovery.H = e.H

	// Core decision logic (from v3-mini-engine.md section 8).
	decide(st, r, trace)
}

// decide runs the core bounded decision logic.
func decide(st *ReplicaState, r *ApplyResult, trace func(string, string)) {
	if !st.Identity.MemberPresent {
		st.Recovery.Decision = DecisionNone
		st.Recovery.DecisionReason = "not_a_member"
		trace("decision", "none (not a member)")
		return
	}

	if st.Reachability.Status != ProbeReachable {
		st.Recovery.Decision = DecisionUnknown
		st.Recovery.DecisionReason = "not_reachable"
		trace("decision", "unknown (not reachable)")
		return
	}

	R, S, H := st.Recovery.R, st.Recovery.S, st.Recovery.H

	if R == 0 && S == 0 && H == 0 {
		st.Recovery.Decision = DecisionUnknown
		st.Recovery.DecisionReason = "no_boundaries"
		trace("decision", "unknown (no R/S/H)")
		return
	}

	switch {
	case R >= H:
		st.Recovery.Decision = DecisionNone
		st.Recovery.DecisionReason = "caught_up"
		trace("decision", "none (R >= H)")

		// Ack-gated caught-up handoff: the replica's lineage gate
		// needs the new epoch established before we can publish
		// healthy, otherwise stale old-epoch traffic could still
		// land. If Identity.Epoch > Reachability.FencedEpoch, emit
		// FenceAtEpoch and HOLD PublishHealthy. PublishHealthy is
		// only emitted once FenceCompleted has bumped FencedEpoch
		// to match Identity.Epoch. On FenceFailed the engine leaves
		// FencedEpoch unchanged; the next probe re-runs decide()
		// and may re-emit the fence.
		if st.Session.Phase == PhaseNone || st.Session.Phase == PhaseCompleted {
			if st.Identity.Epoch > st.Reachability.FencedEpoch {
				r.Commands = append(r.Commands, FenceAtEpoch{
					ReplicaID:       st.Identity.ReplicaID,
					Epoch:           st.Identity.Epoch,
					EndpointVersion: st.Identity.EndpointVersion,
				})
				trace("command", "FenceAtEpoch")
			} else {
				r.Commands = append(r.Commands, PublishHealthy{ReplicaID: st.Identity.ReplicaID})
			}
		}

	case R >= S && R < H:
		st.Recovery.Decision = DecisionCatchUp
		st.Recovery.DecisionReason = "gap_within_wal"
		trace("decision", "catch_up (R >= S, R < H)")

		if !hasActiveSession(st) {
			// T4d-3: FromLSN = R + 1 — engine-owned "skip already-
			// applied LSN" policy (G-1 §6.1 Option A; pins
			// INV-REPL-CATCHUP-FROMLSN-IS-REPLICA-FLUSHED-PLUS-1).
			// Source is engine state Recovery.R, NOT the raw probe
			// payload (INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-
			// NOT-PROBE).
			r.Commands = append(r.Commands, StartCatchUp{
				ReplicaID:       st.Identity.ReplicaID,
				Epoch:           st.Identity.Epoch,
				EndpointVersion: st.Identity.EndpointVersion,
				FromLSN:         R + 1,
				TargetLSN:       H,
			})
			trace("command", "StartCatchUp")
		}

	case R < S:
		st.Recovery.Decision = DecisionRebuild
		st.Recovery.DecisionReason = "gap_beyond_wal"
		trace("decision", "rebuild (R < S)")

		if !hasActiveSession(st) {
			r.Commands = append(r.Commands, StartRebuild{
				ReplicaID:       st.Identity.ReplicaID,
				Epoch:           st.Identity.Epoch,
				EndpointVersion: st.Identity.EndpointVersion,
				TargetLSN:       H,
			})
			trace("command", "StartRebuild")
		}
	}
}

func hasActiveSession(st *ReplicaState) bool {
	return st.Session.Phase == PhaseStarting || st.Session.Phase == PhaseRunning
}

// --- Session lifecycle ---

func applySessionPrepared(st *ReplicaState, e SessionPrepared, r *ApplyResult, trace func(string, string)) {
	// Stale session check.
	if st.Session.SessionID != 0 && e.SessionID <= st.Session.SessionID {
		trace("stale_session_prepared", "session ID too old")
		return
	}
	st.Session = SessionTruth{
		SessionID: e.SessionID,
		Kind:      e.Kind,
		TargetLSN: e.TargetLSN,
		Phase:     PhaseStarting,
	}
	trace("session_prepared", string(e.Kind))
}

func applySessionStarted(st *ReplicaState, e SessionStarted, r *ApplyResult, trace func(string, string)) {
	if e.SessionID != st.Session.SessionID {
		trace("stale_session_started", phaseMismatchDetail(e.SessionID, st.Session.SessionID))
		return
	}
	if st.Session.Phase != PhaseStarting {
		trace("session_started_ignored", "current phase="+string(st.Session.Phase))
		return
	}
	st.Session.Phase = PhaseRunning
	trace("session_started", "")
}

func applySessionProgress(st *ReplicaState, e SessionProgressObserved, r *ApplyResult, trace func(string, string)) {
	if e.SessionID != st.Session.SessionID {
		trace("stale_session_progress", phaseMismatchDetail(e.SessionID, st.Session.SessionID))
		return
	}
	if st.Session.Phase != PhaseRunning {
		trace("session_progress_ignored", "current phase="+string(st.Session.Phase))
		return
	}
	if e.AchievedLSN > st.Session.AchievedLSN {
		st.Session.AchievedLSN = e.AchievedLSN
	}
	// Progress does NOT imply completion. Terminal truth comes only
	// from SessionClosedCompleted or SessionClosedFailed.
	trace("session_progress", "")
}

func applySessionCompleted(st *ReplicaState, e SessionClosedCompleted, r *ApplyResult, trace func(string, string)) {
	if e.SessionID != st.Session.SessionID {
		trace("stale_session_completed", phaseMismatchDetail(e.SessionID, st.Session.SessionID))
		return
	}
	if st.Session.Phase != PhaseStarting && st.Session.Phase != PhaseRunning {
		trace("session_completed_ignored", "current phase="+string(st.Session.Phase))
		return
	}
	st.Session.Phase = PhaseCompleted
	st.Session.AchievedLSN = e.AchievedLSN
	st.Recovery.R = e.AchievedLSN // advance replica boundary
	// T4c-3 retry-loop: success clears the attempt counter so a
	// future independent recovery cycle starts with a fresh budget.
	st.Recovery.Attempts = 0
	// A successful catch-up/rebuild session sent mutating traffic
	// at the current identity epoch, so the replica's lineage gate
	// is now at this epoch. Treat completion as an implicit fence.
	if st.Identity.Epoch > st.Reachability.FencedEpoch {
		st.Reachability.FencedEpoch = st.Identity.Epoch
	}
	trace("session_completed", "")

	// Re-evaluate: may be caught up now.
	decide(st, r, trace)
}

func applySessionFailed(st *ReplicaState, e SessionClosedFailed, r *ApplyResult, trace func(string, string)) {
	if e.SessionID != st.Session.SessionID {
		trace("stale_session_failed", phaseMismatchDetail(e.SessionID, st.Session.SessionID))
		return
	}
	if st.Session.Phase != PhaseStarting && st.Session.Phase != PhaseRunning {
		trace("session_failed_ignored", "current phase="+string(st.Session.Phase))
		return
	}
	st.Session.Phase = PhaseFailed
	st.Session.FailureReason = e.Reason
	trace("session_failed", e.Reason)

	// T4d-1 (architect HIGH v0.1 #1 + v0.3 boundary fix): branch on
	// typed FailureKind, NOT substring match. `Reason` is diagnostic
	// text only and engine MUST NOT parse it.
	//
	// WALRecycled is a tier-class change — force recovery.Decision=
	// Rebuild and reset Attempts so the next decide() pass emits a
	// fresh StartRebuild rather than counting toward the catch-up
	// retry budget.
	if e.FailureKind == RecoveryFailureWALRecycled {
		st.Recovery.Decision = DecisionRebuild
		st.Recovery.DecisionReason = "wal_recycled"
		st.Recovery.Attempts = 0
		trace("recycle_escalation", "FailureKind=WALRecycled → recovery.Decision=Rebuild")
		r.Commands = append(r.Commands, PublishDegraded{
			ReplicaID: e.ReplicaID,
			Reason:    "session_failed: " + e.Reason,
		})
		return
	}

	// T4c-3 §4.1 retry-loop wiring (round-38). Non-recycled failure:
	// increment Attempts; if budget remaining, engine re-emits the
	// matching Start* command on this same Apply (the recovery
	// session was the active session; a new one for the same
	// (decision, target) is in scope). Budget exhaustion publishes
	// Degraded and clears the recovery decision so the next probe
	// re-classifies.
	//
	// Exception: watchdog-synthesized start_timeout means the
	// executor never even started the session — retrying an executor
	// that won't start is unlikely to help. Skip retry; let the
	// adapter's higher-layer machinery (probe re-classification)
	// drive recovery.
	//
	// T4d-1: branch on typed FailureKind (was: substring match on
	// Reason text). Reason stays for diagnostics only.
	if e.FailureKind == RecoveryFailureStartTimeout {
		trace("start_timeout_no_retry", "FailureKind=StartTimeout — skip retry")
		r.Commands = append(r.Commands, PublishDegraded{
			ReplicaID: e.ReplicaID,
			Reason:    "session_failed: " + e.Reason,
		})
		return
	}
	st.Recovery.Attempts++
	policy := DefaultRuntimePolicyFor(contentKindFor(st.Recovery.Decision))
	budget := policy.MaxRetries
	if st.Recovery.Attempts <= budget {
		trace("retry_attempt",
			fmt.Sprintf("attempt=%d budget=%d decision=%s",
				st.Recovery.Attempts, budget, st.Recovery.Decision))
		// Re-emit the appropriate Start* command. Lineage fields come
		// from current Identity + recovery target.
		switch st.Recovery.Decision {
		case DecisionCatchUp:
			// T4d-3 §6.2 architect Option A: retry re-emit reuses
			// ORIGINAL Recovery.R + 1 (not re-probed). Apply gate
			// (T4d-2) handles re-shipped gap via per-LBA stale-skip.
			// Pinned by TestT4d3_RetryAfterReplicaAdvanced_OverScans
			// HandledByApplyGate.
			r.Commands = append(r.Commands, StartCatchUp{
				ReplicaID:       st.Identity.ReplicaID,
				Epoch:           st.Identity.Epoch,
				EndpointVersion: st.Identity.EndpointVersion,
				FromLSN:         st.Recovery.R + 1,
				TargetLSN:       st.Recovery.H,
			})
			trace("command", "StartCatchUp (retry)")
		case DecisionRebuild:
			r.Commands = append(r.Commands, StartRebuild{
				ReplicaID:       st.Identity.ReplicaID,
				Epoch:           st.Identity.Epoch,
				EndpointVersion: st.Identity.EndpointVersion,
				TargetLSN:       st.Recovery.H,
			})
			trace("command", "StartRebuild (retry)")
		}
		return
	}

	// Budget exhausted. Per round-47 architect addition: catch-up
	// budget exhaustion DIRECTLY ESCALATES to rebuild — engine emits
	// StartRebuild without waiting for the next probe to re-classify.
	// Pinned by INV-REPL-CATCHUP-EXHAUSTION-ESCALATES-TO-REBUILD.
	//
	// Pre-round-47 behavior: cleared Decision + emitted PublishDegraded;
	// rebuild only fired if a fresh probe arrived showing R<S. That
	// path was probe-dependent and could leave the replica idle for a
	// probe interval before rebuild started.
	//
	// Round-47 scope: the rebuild path becomes engine-driven
	// end-to-end. Catch-up exhaustion is itself the trigger; rebuild
	// emission is automatic. Rebuild's MaxRetries=0 (per
	// DefaultRuntimePolicyFor) means a rebuild failure terminates
	// without further retry — clean terminal definition.
	if st.Recovery.Decision == DecisionCatchUp {
		trace("retry_exhausted_escalate_to_rebuild",
			fmt.Sprintf("attempts=%d > budget=%d → emit StartRebuild (round-47)",
				st.Recovery.Attempts, budget))
		st.Recovery.Attempts = 0
		st.Recovery.Decision = DecisionRebuild
		st.Recovery.DecisionReason = "catchup_budget_exhausted"
		r.Commands = append(r.Commands, StartRebuild{
			ReplicaID:       st.Identity.ReplicaID,
			Epoch:           st.Identity.Epoch,
			EndpointVersion: st.Identity.EndpointVersion,
			TargetLSN:       st.Recovery.H,
		})
		return
	}

	// Rebuild exhaustion (or any non-catch-up exhaustion):
	// terminal — emit Degraded; no further retry. Rebuild's
	// MaxRetries=0 makes this the natural termination point.
	trace("retry_exhausted_terminal",
		fmt.Sprintf("attempts=%d > budget=%d decision=%s — terminal",
			st.Recovery.Attempts, budget, st.Recovery.Decision))
	st.Recovery.Attempts = 0
	st.Recovery.Decision = DecisionUnknown
	st.Recovery.DecisionReason = "retry_budget_exhausted"
	r.Commands = append(r.Commands, PublishDegraded{
		ReplicaID: e.ReplicaID,
		Reason:    "retry_budget_exhausted: " + e.Reason,
	})
}

// contentKindFor maps the engine's recovery Decision to the
// RecoveryContentKind whose RuntimePolicy the retry budget comes
// from. wal_delta covers catch-up; full_extent covers rebuild;
// partial_lba is Stage 2 and not yet emitted by the engine.
func contentKindFor(d RecoveryDecision) RecoveryContentKind {
	switch d {
	case DecisionCatchUp:
		return RecoveryContentWALDelta
	case DecisionRebuild:
		return RecoveryContentFullExtent
	}
	return RecoveryContentWALDelta
}

// T4d-1: substring-match helpers `isWALRecycledFailure`,
// `isStartTimeoutFailure`, and `containsAny` REMOVED. Engine now
// branches on typed `e.FailureKind` (RecoveryFailureKind enum).
//
// `Reason` field on SessionClosedFailed is DIAGNOSTIC TEXT ONLY —
// engine MUST NOT parse it. Substring matching was a temporary
// T4c-2 contract; T4d-1 closes the binding per architect HIGH v0.1
// #1 + v0.3 boundary discipline (engine→storage decoupling).
//
// Substrate-side classification lives in storage.RecoveryFailure +
// storage.StorageRecoveryFailureKind. Transport extracts via
// errors.As and maps to engine.RecoveryFailureKind at the boundary.
// Adapter copies the typed field through SessionCloseResult.

func applySessionInvalidated(st *ReplicaState, e SessionInvalidated, r *ApplyResult, trace func(string, string)) {
	if e.SessionID != st.Session.SessionID {
		trace("stale_session_invalidated", phaseMismatchDetail(e.SessionID, st.Session.SessionID))
		return
	}
	st.Session = SessionTruth{}
	trace("session_invalidated", e.Reason)
}

// --- Fence events (P14 S1) ---

func applyFenceCompleted(st *ReplicaState, e FenceCompleted, r *ApplyResult, trace func(string, string)) {
	// Lineage check: the fence result must belong to the current
	// identity. A fence at an older epoch than Identity.Epoch is
	// stale (identity advanced mid-fence); drop without mutating.
	// A fence for an endpoint the engine no longer recognizes is
	// also stale.
	if e.Epoch < st.Identity.Epoch {
		trace("stale_fence_completed", fmt.Sprintf("event epoch=%d older than current=%d",
			e.Epoch, st.Identity.Epoch))
		return
	}
	if e.Epoch == st.Identity.Epoch && e.EndpointVersion < st.Identity.EndpointVersion {
		trace("stale_fence_completed", fmt.Sprintf("same epoch=%d but older endpointVersion=%d < current=%d",
			e.Epoch, e.EndpointVersion, st.Identity.EndpointVersion))
		return
	}

	if e.Epoch > st.Reachability.FencedEpoch {
		st.Reachability.FencedEpoch = e.Epoch
		trace("fenced", fmt.Sprintf("epoch=%d", e.Epoch))
	} else {
		trace("fence_completed_idempotent", fmt.Sprintf("epoch=%d already fenced", e.Epoch))
	}

	// Re-run decide: a caught-up replica that was waiting behind
	// the fence can now publish healthy.
	if st.Recovery.R != 0 || st.Recovery.S != 0 || st.Recovery.H != 0 {
		decide(st, r, trace)
	}
}

func applyFenceFailed(st *ReplicaState, e FenceFailed, r *ApplyResult, trace func(string, string)) {
	// Fence failure is fail-closed: FencedEpoch is NOT advanced.
	// The next probe will re-run decide() and re-emit FenceAtEpoch
	// if the engine still believes the replica is caught up under
	// the new epoch. We do NOT re-emit here; retry is probe-driven.
	if e.Epoch < st.Identity.Epoch {
		trace("stale_fence_failed", fmt.Sprintf("event epoch=%d older than current=%d",
			e.Epoch, st.Identity.Epoch))
		return
	}
	trace("fence_failed", fmt.Sprintf("epoch=%d reason=%s", e.Epoch, e.Reason))
}

func phaseMismatchDetail(eventID, currentID uint64) string {
	if currentID == 0 {
		return fmt.Sprintf("event session=%d but no active session", eventID)
	}
	if eventID < currentID {
		return fmt.Sprintf("event session=%d older than current=%d", eventID, currentID)
	}
	return fmt.Sprintf("event session=%d newer than current=%d", eventID, currentID)
}

// --- Publication derivation ---

func derivePublication(st *ReplicaState, trace func(string, string)) {
	if !st.Identity.MemberPresent {
		st.Publication = PublicationTruth{}
		return
	}

	st.Publication.Publishable = true

	switch {
	case st.Recovery.Decision == DecisionNone && st.Reachability.Status == ProbeReachable:
		// Caught-up branch. Ack-gated on fence completion (P14 S1):
		// the operator-visible Healthy must not flip true until the
		// replica's lineage gate has observed Identity.Epoch via
		// FenceCompleted. Otherwise stale old-epoch traffic could
		// still land while the projection already reads healthy.
		if st.Identity.Epoch > st.Reachability.FencedEpoch {
			st.Publication.Healthy = false
			st.Publication.Degraded = false
			st.Publication.NeedsAttention = true
		} else {
			st.Publication.Healthy = true
			st.Publication.Degraded = false
			st.Publication.NeedsAttention = false
		}
	case st.Session.Phase == PhaseRunning || st.Session.Phase == PhaseStarting:
		st.Publication.Healthy = false
		st.Publication.Degraded = false
		st.Publication.NeedsAttention = true
	default:
		st.Publication.Healthy = false
		st.Publication.Degraded = true
		st.Publication.NeedsAttention = true
	}
}
