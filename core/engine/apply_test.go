package engine

import (
	"math/rand"
	"testing"
)

// ============================================================
// V3 Mini Engine Conformance Tests
//
// These prove the core semantic contract:
//   1. Deterministic: same events → same result
//   2. Stale rejection: old epoch/endpoint/session ignored
//   3. R/S/H decision correctness
//   4. Terminal truth: only SessionClosed* changes completion
//   5. Progress does NOT imply completion
//   6. Transport loss does NOT force rebuild
// ============================================================

func newState(volumeID, replicaID string) *ReplicaState {
	return &ReplicaState{}
}

func assignAndProbe(st *ReplicaState) {
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334",
	})
	Apply(st, ProbeSucceeded{
		ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1,
	})
}

// --- Determinism ---

func TestV3_Deterministic_SameEventsSameResult(t *testing.T) {
	events := []Event{
		AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "a", CtrlAddr: "b"},
		ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1},
		RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 10, H: 100},
	}

	// Run twice, compare.
	run := func() ApplyResult {
		st := &ReplicaState{}
		var last ApplyResult
		for _, ev := range events {
			last = Apply(st, ev)
		}
		return last
	}

	r1 := run()
	r2 := run()

	if r1.Projection.Mode != r2.Projection.Mode {
		t.Fatalf("mode: %s vs %s", r1.Projection.Mode, r2.Projection.Mode)
	}
	if r1.Projection.RecoveryDecision != r2.Projection.RecoveryDecision {
		t.Fatalf("decision: %s vs %s", r1.Projection.RecoveryDecision, r2.Projection.RecoveryDecision)
	}
	if len(r1.Commands) != len(r2.Commands) {
		t.Fatalf("commands: %d vs %d", len(r1.Commands), len(r2.Commands))
	}
}

// --- R/S/H Decision Logic ---

func TestV3_Decision_CaughtUp_NoRecovery(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 100})

	if r.Projection.RecoveryDecision != DecisionNone {
		t.Fatalf("R>=H should be none, got %s", r.Projection.RecoveryDecision)
	}
	// P14 S1: caught-up path now emits FenceAtEpoch first to
	// advance the replica's lineage gate. PublishHealthy MUST NOT
	// be in the command list yet, and projection Mode MUST NOT be
	// healthy — the operator-visible contract is ack-gated.
	assertHasCommand(t, r, "FenceAtEpoch")
	for _, cmd := range r.Commands {
		if CommandKind(cmd) == "PublishHealthy" {
			t.Fatal("PublishHealthy emitted before FenceCompleted — contract is ack-gated")
		}
	}
	if r.Projection.Mode == ModeHealthy {
		t.Fatalf("projection flipped healthy before fence complete: %s", r.Projection.Mode)
	}

	// Complete the fence → engine re-runs decide → emits PublishHealthy.
	r2 := Apply(st, FenceCompleted{ReplicaID: "r1", Epoch: st.Identity.Epoch, EndpointVersion: st.Identity.EndpointVersion})
	assertHasCommand(t, r2, "PublishHealthy")
	if r2.Projection.Mode != ModeHealthy {
		t.Fatalf("after FenceCompleted: expected healthy, got %s", r2.Projection.Mode)
	}
}

func TestV3_Decision_CatchUp_GapWithinWAL(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})

	if r.Projection.RecoveryDecision != DecisionCatchUp {
		t.Fatalf("R>=S,R<H should be catch_up, got %s", r.Projection.RecoveryDecision)
	}
	assertHasCommand(t, r, "StartCatchUp")
}

func TestV3_Decision_CatchUp_EmitsFrontierHintAlias(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})

	for _, cmd := range r.Commands {
		if c, ok := cmd.(StartCatchUp); ok {
			if c.FrontierHint != 100 {
				t.Fatalf("FrontierHint = %d, want 100", c.FrontierHint)
			}
			if c.TargetLSN != c.FrontierHint {
				t.Fatalf("legacy TargetLSN = %d, want alias of FrontierHint %d", c.TargetLSN, c.FrontierHint)
			}
			return
		}
	}
	t.Fatal("StartCatchUp not emitted")
}

func TestV3_Decision_Rebuild_GapBeyondWAL(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 5, S: 50, H: 100})

	if r.Projection.RecoveryDecision != DecisionRebuild {
		t.Fatalf("R<S should be rebuild, got %s", r.Projection.RecoveryDecision)
	}
	assertHasCommand(t, r, "StartRebuild")
}

func TestV3_SessionPrepared_FrontierHintIsCanonical(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, SessionPrepared{
		ReplicaID:    "r1",
		SessionID:    7,
		Kind:         SessionCatchUp,
		FrontierHint: 123,
		TargetLSN:    999,
	})

	if st.Session.FrontierHint != 123 {
		t.Fatalf("FrontierHint = %d, want 123", st.Session.FrontierHint)
	}
	if st.Session.TargetLSN != 123 {
		t.Fatalf("legacy TargetLSN = %d, want canonical FrontierHint alias 123", st.Session.TargetLSN)
	}
}

func TestV3_Decision_Unknown_NotReachable(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	// No probe — not reachable
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 5, S: 50, H: 100})

	if r.Projection.RecoveryDecision != DecisionUnknown {
		t.Fatalf("unreachable should be unknown, got %s", r.Projection.RecoveryDecision)
	}
}

// --- Stale Rejection ---

func TestV3_Stale_OldEpochAssignment(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "a", CtrlAddr: "b"})

	// Stale assignment with older epoch.
	r := Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, DataAddr: "a", CtrlAddr: "b"})

	// Epoch should still be 5.
	if r.Projection.Epoch != 5 {
		t.Fatalf("stale epoch accepted: got %d, want 5", r.Projection.Epoch)
	}
	assertTraceContains(t, r, "stale_assignment")
}

func TestV3_Invalid_ZeroEpochAssignmentRejectedEarly(t *testing.T) {
	st := &ReplicaState{}

	r := Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 0, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})

	if st.Identity.MemberPresent {
		t.Fatal("zero-epoch assignment should not initialize identity")
	}
	assertTraceContains(t, r, "invalid_assignment")
	assertNoCommand(t, r, "ProbeReplica")
}

func TestV3_Invalid_ZeroEndpointAssignmentRejectedEarly(t *testing.T) {
	st := &ReplicaState{}

	r := Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 0,
		DataAddr: "a", CtrlAddr: "b",
	})

	if st.Identity.MemberPresent {
		t.Fatal("zero-endpoint assignment should not initialize identity")
	}
	assertTraceContains(t, r, "invalid_assignment")
	assertNoCommand(t, r, "ProbeReplica")
}

func TestV3_Invalid_ZeroEndpointObservedRejectedEarly(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})

	r := Apply(st, EndpointObserved{
		ReplicaID: "r1", EndpointVersion: 0,
		DataAddr: "b", CtrlAddr: "b",
	})

	if st.Identity.EndpointVersion != 1 {
		t.Fatalf("zero endpoint observation should not mutate endpoint version, got %d", st.Identity.EndpointVersion)
	}
	assertTraceContains(t, r, "invalid_endpoint")
	assertNoCommand(t, r, "ProbeReplica")
}

func TestV3_Stale_OldEndpointProbe(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 5, DataAddr: "a", CtrlAddr: "b"})

	r := Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 2, TransportEpoch: 1})

	if st.Reachability.Status == ProbeReachable {
		t.Fatal("stale endpoint probe should not mark reachable")
	}
	assertTraceContains(t, r, "stale_probe")
}

func TestV3_Stale_OldTransportEpochProbeRejected(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 5, DataAddr: "a", CtrlAddr: "b"})

	r := Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 5, TransportEpoch: 4})

	if st.Reachability.Status == ProbeReachable {
		t.Fatal("old transport epoch probe should not mark reachable")
	}
	assertTraceContains(t, r, "stale_probe")
}

func TestV3_Stale_OldTransportEpochProbeFailureRejected(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 5, DataAddr: "a", CtrlAddr: "b"})

	r := Apply(st, ProbeFailed{ReplicaID: "r1", EndpointVersion: 5, TransportEpoch: 4, Reason: "stale"})

	if st.Reachability.Status == ProbeUnreachable {
		t.Fatal("old transport epoch probe failure should not degrade reachability")
	}
	assertTraceContains(t, r, "stale_probe_failed")
}

func TestV3_Stale_OldTransportEpochRecoveryFactsRejected(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 5, DataAddr: "a", CtrlAddr: "b"})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 5, TransportEpoch: 5})

	r := Apply(st, RecoveryFactsObserved{
		ReplicaID:       "r1",
		EndpointVersion: 5,
		TransportEpoch:  4,
		R:               5,
		S:               50,
		H:               100,
	})

	if st.Recovery.H != 0 {
		t.Fatalf("stale recovery facts should not update H, got %d", st.Recovery.H)
	}
	assertTraceContains(t, r, "stale_recovery_facts")
}

func TestV3_Stale_WrongSessionProgress(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 10, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 10})

	// Progress for wrong session ID.
	r := Apply(st, SessionProgressObserved{ReplicaID: "r1", SessionID: 999, AchievedLSN: 50})

	assertTraceContains(t, r, "stale_session_progress")
}

// --- Terminal Truth ---

func TestV3_Terminal_ProgressDoesNotImplyCompletion(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})

	// Progress to target — but NOT completion.
	r := Apply(st, SessionProgressObserved{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})

	if r.Projection.SessionPhase == PhaseCompleted {
		t.Fatal("progress must NOT imply completion")
	}
	assertNoCommand(t, r, "PublishHealthy")
}

func TestV3_Terminal_OnlySessionClosedCompletedGivesSuccess(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})
	Apply(st, SessionProgressObserved{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})

	// Terminal success.
	r := Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})

	if r.Projection.SessionPhase != PhaseCompleted {
		t.Fatalf("phase=%s, want completed", r.Projection.SessionPhase)
	}
	// After completion + re-decision, should be caught up and healthy.
	if r.Projection.RecoveryDecision != DecisionNone {
		t.Fatalf("decision=%s, want none after completion", r.Projection.RecoveryDecision)
	}
	assertHasCommand(t, r, "PublishHealthy")
}

func TestV3_Terminal_DelayedStartAfterFailureIgnored(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "start_timeout"})

	r := Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})

	if st.Session.Phase != PhaseFailed {
		t.Fatalf("phase=%s, want failed", st.Session.Phase)
	}
	assertTraceContains(t, r, "session_started_ignored")
}

func TestV3_Terminal_DelayedCompleteAfterFailureIgnored(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "start_timeout"})

	r := Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})

	if st.Session.Phase != PhaseFailed {
		t.Fatalf("phase=%s, want failed", st.Session.Phase)
	}
	if st.Recovery.R != 0 {
		t.Fatalf("R=%d, want unchanged 0", st.Recovery.R)
	}
	assertTraceContains(t, r, "session_completed_ignored")
	assertNoCommand(t, r, "PublishHealthy")
}

func TestV3_Terminal_DelayedFailureAfterCompletionIgnored(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})
	Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})

	if st.Session.Phase != PhaseCompleted {
		t.Fatalf("precondition: phase=%s, want completed", st.Session.Phase)
	}

	r := Apply(st, SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "late_failure"})

	if st.Session.Phase != PhaseCompleted {
		t.Fatalf("phase=%s, want unchanged completed", st.Session.Phase)
	}
	if st.Session.FailureReason != "" {
		t.Fatalf("FailureReason=%q, want empty after delayed failure", st.Session.FailureReason)
	}
	assertTraceContains(t, r, "session_failed_ignored")
	assertNoCommand(t, r, "PublishDegraded")
}

func TestV3_Terminal_DuplicateCompletedIgnored(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})
	Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})

	before := st.Session

	r := Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})

	if st.Session != before {
		t.Fatalf("session state changed on duplicate completion: %+v", st.Session)
	}
	assertTraceContains(t, r, "session_completed_ignored")
}

func TestV3_Terminal_DuplicateFailedIgnored(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "first_failure"})

	before := st.Session

	r := Apply(st, SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "duplicate_failure"})

	if st.Session != before {
		t.Fatalf("session state changed on duplicate failure: %+v", st.Session)
	}
	if st.Session.FailureReason != "first_failure" {
		t.Fatalf("FailureReason=%q, want first_failure", st.Session.FailureReason)
	}
	assertTraceContains(t, r, "session_failed_ignored")
	assertNoCommand(t, r, "PublishDegraded")
}

func TestV3_Terminal_InvalidPhaseTransitionsLeaveStateUnchanged(t *testing.T) {
	cases := []struct {
		name      string
		terminal  Event
		illegal   []Event
		wantPhase SessionPhase
	}{
		{
			name:     "after_failed",
			terminal: SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "x"},
			illegal: []Event{
				SessionStarted{ReplicaID: "r1", SessionID: 1},
				SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100},
				SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "y"},
				SessionProgressObserved{ReplicaID: "r1", SessionID: 1, AchievedLSN: 50},
			},
			wantPhase: PhaseFailed,
		},
		{
			name:     "after_completed",
			terminal: SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100},
			illegal: []Event{
				SessionStarted{ReplicaID: "r1", SessionID: 1},
				SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 200},
				SessionClosedFailed{ReplicaID: "r1", SessionID: 1, Reason: "x"},
				SessionProgressObserved{ReplicaID: "r1", SessionID: 1, AchievedLSN: 50},
			},
			wantPhase: PhaseCompleted,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &ReplicaState{}
			assignAndProbe(st)
			Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
			Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
			Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})
			Apply(st, tc.terminal)

			if st.Session.Phase != tc.wantPhase {
				t.Fatalf("precondition: phase=%s, want %s", st.Session.Phase, tc.wantPhase)
			}
			snapshot := st.Session

			for _, ev := range tc.illegal {
				Apply(st, ev)
				if st.Session != snapshot {
					t.Fatalf("illegal event %T mutated session: before=%+v after=%+v",
						ev, snapshot, st.Session)
				}
			}
		})
	}
}

// --- Transport Loss Does NOT Force Rebuild ---

func TestV3_TransportLoss_DoesNotForceRebuild(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})

	// Transport loss.
	r := Apply(st, ProbeFailed{ReplicaID: "r1", Reason: "connection_reset"})

	// Decision should NOT change to rebuild from transport loss alone.
	if r.Projection.RecoveryDecision == DecisionRebuild {
		t.Fatal("transport loss must not directly force rebuild")
	}
	assertHasCommand(t, r, "PublishDegraded")
}

// --- Active Session Suppresses Duplicate Commands ---

func TestV3_ActiveSession_SuppressesDuplicateStart(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})

	// Another RecoveryFacts while session is active.
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})

	// Should NOT emit another StartCatchUp.
	assertNoCommand(t, r, "StartCatchUp")
}

// --- Identity Change Invalidates Session ---

func TestV3_IdentityChange_InvalidatesSession(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})

	// New epoch assignment.
	r := Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: "new", CtrlAddr: "new",
	})

	assertHasCommand(t, r, "InvalidateSession")
	assertTraceContains(t, r, "invalidate_session")
}

// --- Removal Clears Everything ---

func TestV3_Removal_ClearsState(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})

	r := Apply(st, ReplicaRemoved{ReplicaID: "r1", Reason: "decommissioned"})

	if r.Projection.Mode != ModeIdle {
		t.Fatalf("mode=%s, want idle after removal", r.Projection.Mode)
	}
	if st.Identity.MemberPresent {
		t.Fatal("member should not be present after removal")
	}
}

// --- Fix: Same-epoch stale endpoint rejection ---

func TestV3_Stale_SameEpochOlderEndpoint(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 5,
		DataAddr: "new", CtrlAddr: "new",
	})

	// Same epoch, older endpoint version — should be rejected.
	r := Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 3,
		DataAddr: "old", CtrlAddr: "old",
	})

	if st.Identity.EndpointVersion != 5 {
		t.Fatalf("endpoint rolled backward: got %d, want 5", st.Identity.EndpointVersion)
	}
	if st.Identity.DataAddr != "new" {
		t.Fatalf("addr rolled backward: got %s, want new", st.Identity.DataAddr)
	}
	assertTraceContains(t, r, "stale_assignment")
}

// --- Fix: Identity change clears session immediately ---

func TestV3_IdentityChange_ClearsSessionImmediately(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})

	// New epoch — session should be cleared immediately.
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: "a", CtrlAddr: "b",
	})

	// hasActiveSession should return false now.
	if hasActiveSession(st) {
		t.Fatal("session should be cleared after identity change")
	}

	// New probe + facts should be able to start a new recovery
	// without waiting for SessionInvalidated from adapter.
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 2, TransportEpoch: 2})
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 200})

	assertHasCommand(t, r, "StartCatchUp")
}

// --- Fix: Wrong-replica events rejected ---

func TestV3_WrongReplica_EventRejected(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st) // sets replicaID = "r1"

	// Event for "r2" should be rejected.
	r := Apply(st, ProbeSucceeded{ReplicaID: "r2", EndpointVersion: 1, TransportEpoch: 1})
	assertTraceContains(t, r, "wrong_replica")

	r2 := Apply(st, RecoveryFactsObserved{ReplicaID: "r2", R: 0, S: 0, H: 100})
	assertTraceContains(t, r2, "wrong_replica")

	r3 := Apply(st, ReplicaRemoved{ReplicaID: "r2", Reason: "oops"})
	assertTraceContains(t, r3, "wrong_replica")

	// State should be unchanged.
	if !st.Identity.MemberPresent {
		t.Fatal("wrong-replica removal should not affect this engine")
	}
}

// --- Order / property checks ---

func TestV3_Order_ProbeAndFactsPermutationConverges(t *testing.T) {
	run := func(events []Event) ApplyResult {
		st := &ReplicaState{}
		Apply(st, AssignmentObserved{
			VolumeID: "vol1", ReplicaID: "r1",
			Epoch: 1, EndpointVersion: 1,
			DataAddr: "a", CtrlAddr: "b",
		})
		var last ApplyResult
		for _, ev := range events {
			last = Apply(st, ev)
		}
		return last
	}

	probe := ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1}
	facts := RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100}

	ordered := run([]Event{probe, facts})
	reversed := run([]Event{facts, probe})

	if ordered.Projection.RecoveryDecision != DecisionCatchUp {
		t.Fatalf("ordered decision=%s, want catch_up", ordered.Projection.RecoveryDecision)
	}
	if reversed.Projection.RecoveryDecision != DecisionCatchUp {
		t.Fatalf("reversed decision=%s, want catch_up", reversed.Projection.RecoveryDecision)
	}
	assertHasCommand(t, ordered, "StartCatchUp")
	assertHasCommand(t, reversed, "StartCatchUp")
}

func TestV3_Property_RandomStaleSessionEventsCannotMutateLiveSession(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 10, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 10})

	for i := 0; i < 200; i++ {
		wrongID := uint64(rng.Intn(1000) + 11)

		var r ApplyResult
		switch rng.Intn(4) {
		case 0:
			r = Apply(st, SessionProgressObserved{ReplicaID: "r1", SessionID: wrongID, AchievedLSN: uint64(rng.Intn(100) + 1)})
			assertTraceContains(t, r, "stale_session_progress")
		case 1:
			r = Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: wrongID, AchievedLSN: 100})
			assertTraceContains(t, r, "stale_session_completed")
		case 2:
			r = Apply(st, SessionClosedFailed{ReplicaID: "r1", SessionID: wrongID, Reason: "stale"})
			assertTraceContains(t, r, "stale_session_failed")
		default:
			r = Apply(st, SessionInvalidated{ReplicaID: "r1", SessionID: wrongID, Reason: "stale"})
			assertTraceContains(t, r, "stale_session_invalidated")
		}

		if st.Session.SessionID != 10 {
			t.Fatalf("iteration %d: stale event changed session ID to %d", i, st.Session.SessionID)
		}
		if st.Session.Phase != PhaseRunning {
			t.Fatalf("iteration %d: stale event changed phase to %s", i, st.Session.Phase)
		}
		if st.Session.TargetLSN != 100 {
			t.Fatalf("iteration %d: stale event changed target to %d", i, st.Session.TargetLSN)
		}
	}
}

// ============================================================
// P14 S1 — Dedicated Fence Proof Tests
//
// These tests pin the operator-visible contract of the ack-gated
// fence on the caught-up path. They must remain together so the
// contract is auditable as a single unit.
//
// Contract surface (what these prove):
//   1. Caught-up path emits ONLY FenceAtEpoch — no StartCatchUp,
//      no StartRebuild. Fence is not a data-recovery session.
//   2. PublishHealthy MUST NOT appear in any command stream before
//      FenceCompleted has been applied.
//   3. FenceFailed is fail-closed: FencedEpoch does not advance.
//      The next probe re-runs decide() and re-emits FenceAtEpoch.
//   4. A FenceCompleted for an old epoch (under a newer identity)
//      is rejected — identity-advancement invalidates in-flight
//      fence results.
// ============================================================

// TestFence_CaughtUpPath_UsesNoDataRecovery — a caught-up replica
// under a newer identity epoch must not trigger a catch-up or
// rebuild session. The only command the engine issues on the
// caught-up branch is FenceAtEpoch. This is the structural proof
// that fence is a command+event pair, NOT a session lifecycle.
func TestFence_CaughtUpPath_UsesNoDataRecovery(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 100})

	assertHasCommand(t, r, "FenceAtEpoch")
	assertNoCommand(t, r, "StartCatchUp")
	assertNoCommand(t, r, "StartRebuild")
	// Also not a session — session phase is untouched.
	if st.Session.Phase != PhaseNone {
		t.Fatalf("fence is not a session: session phase=%s, want none",
			st.Session.Phase)
	}
	if st.Session.SessionID != 0 {
		t.Fatalf("fence must not allocate a session in engine state, got sid=%d",
			st.Session.SessionID)
	}
}

// TestFence_HealthyOnlyAfterFenceComplete — operator-visible
// contract: PublishHealthy is held until FenceCompleted bumps the
// replica's FencedEpoch. Observed from the projection Mode and the
// command stream.
func TestFence_HealthyOnlyAfterFenceComplete(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r1 := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 100})

	// Before FenceCompleted: no PublishHealthy, Mode not Healthy.
	assertNoCommand(t, r1, "PublishHealthy")
	if r1.Projection.Mode == ModeHealthy {
		t.Fatalf("pre-fence projection flipped Healthy: %s", r1.Projection.Mode)
	}
	if st.Reachability.FencedEpoch >= st.Identity.Epoch {
		t.Fatalf("pre-fence FencedEpoch=%d must be < Identity.Epoch=%d",
			st.Reachability.FencedEpoch, st.Identity.Epoch)
	}

	// FenceCompleted at the current lineage — engine re-runs decide
	// and must now emit PublishHealthy.
	r2 := Apply(st, FenceCompleted{
		ReplicaID:       "r1",
		Epoch:           st.Identity.Epoch,
		EndpointVersion: st.Identity.EndpointVersion,
	})
	assertHasCommand(t, r2, "PublishHealthy")
	if r2.Projection.Mode != ModeHealthy {
		t.Fatalf("post-fence projection not Healthy: %s", r2.Projection.Mode)
	}
	if st.Reachability.FencedEpoch != st.Identity.Epoch {
		t.Fatalf("FencedEpoch=%d must match Identity.Epoch=%d after fence",
			st.Reachability.FencedEpoch, st.Identity.Epoch)
	}
}

// TestFence_FailureLeavesFencedEpochUnchanged — fail-closed
// contract: FenceFailed does NOT advance FencedEpoch, and does NOT
// re-emit FenceAtEpoch on its own. Retry is probe-driven: the next
// successful probe re-runs decide() and re-emits FenceAtEpoch.
func TestFence_FailureLeavesFencedEpochUnchanged(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	// Caught-up fact set — the engine should emit FenceAtEpoch.
	r1 := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 100})
	assertHasCommand(t, r1, "FenceAtEpoch")
	preFencedEpoch := st.Reachability.FencedEpoch
	if preFencedEpoch >= st.Identity.Epoch {
		t.Fatalf("pre-fence FencedEpoch=%d must be < Identity.Epoch=%d",
			preFencedEpoch, st.Identity.Epoch)
	}

	// FenceFailed at the current lineage — MUST NOT advance
	// FencedEpoch and MUST NOT re-emit FenceAtEpoch here.
	r2 := Apply(st, FenceFailed{
		ReplicaID:       "r1",
		Epoch:           st.Identity.Epoch,
		EndpointVersion: st.Identity.EndpointVersion,
		Reason:          "simulated_barrier_timeout",
	})
	if st.Reachability.FencedEpoch != preFencedEpoch {
		t.Fatalf("FenceFailed advanced FencedEpoch: pre=%d post=%d",
			preFencedEpoch, st.Reachability.FencedEpoch)
	}
	assertNoCommand(t, r2, "FenceAtEpoch")
	assertNoCommand(t, r2, "PublishHealthy")
	if r2.Projection.Mode == ModeHealthy {
		t.Fatalf("post-FenceFailed projection must not be Healthy, got %s",
			r2.Projection.Mode)
	}

	// Probe-driven retry: another successful probe re-runs decide()
	// and emits FenceAtEpoch again.
	r3 := Apply(st, ProbeSucceeded{
		ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1,
	})
	assertHasCommand(t, r3, "FenceAtEpoch")
	assertNoCommand(t, r3, "PublishHealthy")
}

// TestFence_StaleOldEpochRejectedAfterFenceOnCaughtUp — identity
// advance invalidates in-flight fences: a FenceCompleted carrying
// an older epoch than the current identity must not advance the
// gate and must not produce healthy publication.
func TestFence_StaleOldEpochRejectedAfterFenceOnCaughtUp(t *testing.T) {
	st := &ReplicaState{}
	// Epoch 1 identity, caught-up, fence emitted but in-flight.
	assignAndProbe(st)
	r1 := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 100})
	assertHasCommand(t, r1, "FenceAtEpoch")

	// Identity advances to epoch 2 before the fence completes.
	// Engine must clear reachability/session and request a probe.
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: "a", CtrlAddr: "b",
	})
	if st.Identity.Epoch != 2 {
		t.Fatalf("identity did not advance, got epoch=%d", st.Identity.Epoch)
	}

	// Stale FenceCompleted arrives for epoch 1 (the superseded
	// attempt). It must be rejected: FencedEpoch must not jump to
	// stale epoch, and the replica must not become Healthy.
	rStale := Apply(st, FenceCompleted{
		ReplicaID:       "r1",
		Epoch:           1,
		EndpointVersion: 1,
	})
	if st.Reachability.FencedEpoch >= st.Identity.Epoch {
		t.Fatalf("stale FenceCompleted advanced FencedEpoch: got %d, identity=%d",
			st.Reachability.FencedEpoch, st.Identity.Epoch)
	}
	assertTraceContains(t, rStale, "stale_fence_completed")
	assertNoCommand(t, rStale, "PublishHealthy")
	if rStale.Projection.Mode == ModeHealthy {
		t.Fatalf("stale fence must not produce Healthy, got %s", rStale.Projection.Mode)
	}

	// A fresh fence at the new epoch must advance the gate normally.
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 2, TransportEpoch: 2})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 2, TransportEpoch: 2, R: 100, S: 10, H: 100})
	rFresh := Apply(st, FenceCompleted{
		ReplicaID:       "r1",
		Epoch:           2,
		EndpointVersion: 2,
	})
	if st.Reachability.FencedEpoch != 2 {
		t.Fatalf("fresh fence did not advance FencedEpoch to 2, got %d",
			st.Reachability.FencedEpoch)
	}
	assertHasCommand(t, rFresh, "PublishHealthy")
	if rFresh.Projection.Mode != ModeHealthy {
		t.Fatalf("post-fresh-fence projection not Healthy: %s", rFresh.Projection.Mode)
	}
}

// === Helpers ===

func assertHasCommand(t *testing.T, r ApplyResult, kind string) {
	t.Helper()
	for _, c := range r.Commands {
		if CommandKind(c) == kind {
			return
		}
	}
	kinds := make([]string, len(r.Commands))
	for i, c := range r.Commands {
		kinds[i] = CommandKind(c)
	}
	t.Fatalf("expected command %s, got %v", kind, kinds)
}

func assertNoCommand(t *testing.T, r ApplyResult, kind string) {
	t.Helper()
	for _, c := range r.Commands {
		if CommandKind(c) == kind {
			t.Fatalf("unexpected command %s", kind)
		}
	}
}

func assertTraceContains(t *testing.T, r ApplyResult, step string) {
	t.Helper()
	for _, tr := range r.Trace {
		if tr.Step == step {
			return
		}
	}
	steps := make([]string, len(r.Trace))
	for i, tr := range r.Trace {
		steps[i] = tr.Step
	}
	t.Fatalf("expected trace step %q, got %v", step, steps)
}
