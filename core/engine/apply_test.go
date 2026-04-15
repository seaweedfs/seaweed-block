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
	assertHasCommand(t, r, "PublishHealthy")
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

func TestV3_Decision_Rebuild_GapBeyondWAL(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 5, S: 50, H: 100})

	if r.Projection.RecoveryDecision != DecisionRebuild {
		t.Fatalf("R<S should be rebuild, got %s", r.Projection.RecoveryDecision)
	}
	assertHasCommand(t, r, "StartRebuild")
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
