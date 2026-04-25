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
			r.Commands = append(r.Commands, StartCatchUp{
				ReplicaID:       st.Identity.ReplicaID,
				Epoch:           st.Identity.Epoch,
				EndpointVersion: st.Identity.EndpointVersion,
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

	// T4c-2 G-1 §4.3 architect Option B: error-class mapping at engine
	// SessionClose. ErrWALRecycled is a tier-class change — the gap
	// has exceeded the substrate's retention window, and a retry at
	// the same fromLSN cannot succeed. Force the recovery decision
	// to Rebuild so the next decide() pass emits StartRebuild instead
	// of looping on StartCatchUp.
	//
	// The substrate-package sentinel string is hard-coded here to
	// avoid an engine→storage import dependency; the storage package
	// owns the canonical error message ("storage: WAL recycled past
	// requested LSN"). If that text drifts the integration matrix
	// catches it (T4c-3 scenario #2 RecyclePathEscalates pin).
	if isWALRecycledFailure(e.Reason) {
		st.Recovery.Decision = DecisionRebuild
		st.Recovery.DecisionReason = "wal_recycled"
		trace("recycle_escalation", "ErrWALRecycled → recovery.Decision=Rebuild")
	}

	r.Commands = append(r.Commands, PublishDegraded{
		ReplicaID: e.ReplicaID,
		Reason:    "session_failed: " + e.Reason,
	})
}

// isWALRecycledFailure detects the substrate's ErrWALRecycled sentinel
// in a session-failure reason string. The wrapped error's
// `errors.Is`-friendly form is `... WAL recycled past requested LSN`;
// we match the "WAL recycled" infix to absorb both the wrap-formatted
// and bare forms. Matches the storage package's sentinel text
// (`storage.ErrWALRecycled`) without taking a package dependency on
// it (engine MUST stay decoupled from storage).
//
// Per T4c-2 G-1 §4.3 (architect Option B): substrate-error-class
// mapping at engine SessionClose handler.
func isWALRecycledFailure(reason string) bool {
	return reason != "" && containsAny(reason, []string{
		"WAL recycled",
		"wal recycled",
	})
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if len(sub) <= len(s) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

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
