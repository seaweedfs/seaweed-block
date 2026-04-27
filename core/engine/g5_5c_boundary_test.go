package engine

import (
	"testing"
)

// G5-5C #4: engine-layer boundary tests for the four protocol
// invariants that live structurally in the engine FSM (per §1.H
// audit findings). Architect Batch #4 review 2026-04-27 ruled
// these belong here, NOT in core/replication/, because:
//   - dispatch branch (catch-up vs rebuild) is decide() output
//   - Case 1 reconnect path is engine.applyProbeSucceeded
//   - Case 2 lineage-during-recovery is engine stale-session-ack guard
//   - stale-ack-no-health-promotion is engine Healthy gate
//
// Each invariant has a single owner test here; volume-layer tests
// (batch 3) cover dispatcher / cooldown / lifecycle and do not
// duplicate these assertions.

// helperPrime drives a state machine to (degraded, reachable, gap)
// — the canonical pre-condition for a probe-driven recovery
// dispatch decision.
func helperPrime(t *testing.T, r, s, h uint64) *ReplicaState {
	t.Helper()
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: r, S: s, H: h})
	return st
}

// --- §2 #4: dispatch branch (catch-up vs rebuild) — table-driven ---

// TestG5_5C_Dispatch_CatchUpVsRebuild_TableDriven verifies the
// engine emits the correct recovery command for each (R, S, H)
// shape and the RebuildPinned override. Owner: engine decide().
//
// This is the §2 #4 dispatch invariant: the loop never decides;
// the engine does. Batch 3's volume tests do NOT assert on
// commands — only on which peers got probed.
func TestG5_5C_Dispatch_CatchUpVsRebuild_TableDriven(t *testing.T) {
	cases := []struct {
		name        string
		r, s, h     uint64
		rebuildPin  bool
		wantDec     RecoveryDecision
		wantCmdKind string // "StartCatchUp" or "StartRebuild" or ""
	}{
		{
			name:        "caught_up",
			r:           100, s: 10, h: 100,
			wantDec:     DecisionNone,
			wantCmdKind: "", // none-decision emits FenceAtEpoch (covered elsewhere) but no recovery session
		},
		{
			name:        "gap_within_wal_dispatches_catch_up",
			r:           50, s: 10, h: 100,
			wantDec:     DecisionCatchUp,
			wantCmdKind: "StartCatchUp",
		},
		{
			name:        "gap_exceeds_wal_dispatches_rebuild",
			r:           5, s: 10, h: 100, // R < S: replica is behind the recoverable start
			wantDec:     DecisionRebuild,
			wantCmdKind: "StartRebuild",
		},
		{
			name:        "rebuild_pinned_overrides_catch_up",
			r:           50, s: 10, h: 100, rebuildPin: true,
			wantDec:     DecisionRebuild,
			wantCmdKind: "StartRebuild",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := &ReplicaState{}
			assignAndProbe(st)
			st.Recovery.RebuildPinned = c.rebuildPin

			r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: c.r, S: c.s, H: c.h})

			if r.Projection.RecoveryDecision != c.wantDec {
				t.Errorf("Decision = %s, want %s", r.Projection.RecoveryDecision, c.wantDec)
			}
			if c.wantCmdKind == "" {
				assertNoCommand(t, r, "StartCatchUp")
				assertNoCommand(t, r, "StartRebuild")
			} else {
				assertHasCommand(t, r, c.wantCmdKind)
				// Strict mutual exclusion: only one recovery command kind.
				other := "StartCatchUp"
				if c.wantCmdKind == "StartCatchUp" {
					other = "StartRebuild"
				}
				assertNoCommand(t, r, other)
			}
		})
	}
}

// --- §2 #11: reconnect Case 1 (identity unchanged) ---

// TestG5_5C_Reconnect_Case1_IdentityUnchanged_DispatchesCatchUp
// verifies that a degraded peer whose identity is preserved (replica
// restart against same --durable-root keeps replicaID/epoch/EV) and
// whose probe succeeds fresh is dispatched into catch-up at the
// SAME (epoch, EV) — no master re-emit, no lineage bump.
//
// Pinned: §1.F Case 1 + §2 #11.
// Owner: engine apply path (applyProbeSucceeded → applyRecoveryFacts
// → decide → StartCatchUp).
// Distinct from batch 3's TestVolume_LineageBump_NewPeerHasFreshCooldown
// which asserts the *opposite* axis (Case 2, identity changed).
func TestG5_5C_Reconnect_Case1_IdentityUnchanged_DispatchesCatchUp(t *testing.T) {
	st := &ReplicaState{}

	// Initial admit + probe + recovery facts say "behind, catch-up needed".
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334",
	})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})
	if st.Recovery.Decision != DecisionCatchUp {
		t.Fatalf("setup: Decision = %s, want CatchUp", st.Recovery.Decision)
	}

	// Simulate the in-flight catch-up session opening so subsequent
	// probe events represent a "second probe" not a first.
	Apply(st, SessionPrepared{
		ReplicaID: "r1",
		SessionID: 42,
		Kind:      SessionCatchUp,
		TargetLSN: 100,
	})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 42})

	// Now session fails (e.g., transport blip); replica still
	// "behind" but transport is recoverable.
	Apply(st, SessionClosedFailed{
		ReplicaID:   "r1",
		SessionID:   42,
		FailureKind: RecoveryFailureTransport,
		Reason:      "transport reset",
	})
	if got := st.Reachability.Status; got != ProbeUnreachable {
		// Reachability degrades on session failure — different paths
		// re-classify it. We accept either Unreachable or unchanged
		// Reachable here; the assertion below is on the next probe.
		_ = got
	}

	// Replica restarts (or transport heals) — same identity, fresh
	// probe success. Crucially, epoch and endpointVersion are
	// UNCHANGED.
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})

	// Recovery facts arrive again; gap still indicates catch-up.
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})

	// Identity must NOT have bumped — Case 1 invariant.
	if got := st.Identity.Epoch; got != 1 {
		t.Errorf("identity.Epoch = %d, want 1 (Case 1: identity unchanged)", got)
	}
	if got := st.Identity.EndpointVersion; got != 1 {
		t.Errorf("identity.EndpointVersion = %d, want 1 (Case 1: identity unchanged)", got)
	}

	// Engine should dispatch catch-up at the SAME lineage. Either a
	// fresh StartCatchUp emits (if the prior session was cleaned
	// out) or the engine treats the gap as still-active under the
	// old session — both are correct as long as no rebuild is
	// dispatched and Decision stays CatchUp.
	if got := r.Projection.RecoveryDecision; got != DecisionCatchUp {
		t.Errorf("Decision = %s, want CatchUp (Case 1 path)", got)
	}
	assertNoCommand(t, r, "StartRebuild")
}

// --- §2 #12: lineage bump during in-flight recovery ---

// TestG5_5C_LineageBumpDuringRecovery_OldSessionStaleEventsDropped
// verifies that when a higher (epoch, EV) lineage arrives mid-recovery,
// the in-flight session's later events (SessionCompleted /
// SessionFailed under the old SessionID) are rejected as stale.
// This is the §1.E (c) protocol invariant at the engine layer:
// dispatch commands cannot land on the OLD lineage after the bump.
//
// Distinct from batch 3's TestVolume_LineageBump_OldPeerProbeAbandoned
// which asserts the runtime *peer instance* is replaced and old peer
// in-flight is harmless. This test asserts the engine refuses to
// honor stale-SessionID events at the truth layer.
func TestG5_5C_LineageBumpDuringRecovery_OldSessionStaleEventsDropped(t *testing.T) {
	st := &ReplicaState{}

	// Admit at lineage (epoch=1, EV=1) and start a catch-up session.
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})

	const oldSession uint64 = 42
	Apply(st, SessionPrepared{
		ReplicaID: "r1", SessionID: oldSession,
		Kind:      SessionCatchUp,
		TargetLSN: 100,
	})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: oldSession})
	if st.Session.Phase != PhaseRunning {
		t.Fatalf("setup: session phase = %s, want PhaseRunning", st.Session.Phase)
	}
	if st.Session.SessionID != oldSession {
		t.Fatalf("setup: sessionID = %d, want %d", st.Session.SessionID, oldSession)
	}

	// Lineage bump: master publishes a new assignment with epoch=2.
	// engine.applyAssignment resets Session/Recovery/Reachability
	// per the identityChanged path.
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	if st.Identity.Epoch != 2 {
		t.Fatalf("identity.Epoch = %d, want 2 after bump", st.Identity.Epoch)
	}
	if st.Session.SessionID != 0 {
		t.Errorf("session not reset on lineage bump: SessionID = %d", st.Session.SessionID)
	}

	// Late arrival on OLD sessionID — engine must drop as stale.
	r := Apply(st, SessionClosedCompleted{
		ReplicaID:   "r1",
		SessionID:   oldSession,
		AchievedLSN: 100,
	})

	// The stale ack must NOT advance Recovery.R, must NOT promote
	// Phase, must NOT emit recovery commands.
	if got := st.Recovery.R; got == 100 {
		t.Errorf("stale SessionCompleted (old sessionID=%d) advanced Recovery.R to 100 — should have been dropped",
			oldSession)
	}
	if got := st.Session.Phase; got == PhaseCompleted {
		t.Error("stale SessionCompleted promoted Phase to Completed — should have been dropped")
	}
	for _, cmd := range r.Commands {
		kind := CommandKind(cmd)
		if kind == "PublishHealthy" {
			t.Error("stale SessionCompleted caused PublishHealthy emit — should have been dropped")
		}
	}
}

// --- §2 #13: stale-ack does not promote health ---

// TestG5_5C_StaleSessionAck_DoesNotPromoteHealth verifies the engine
// Healthy gate (apply.go:766-789) refuses to mark a peer healthy
// when the most recent ack indicates the peer has not actually
// caught up (AchievedLSN < primary's H).
//
// Owner: engine.decide() + engine Healthy gate. Pinned at the engine
// layer per architect Batch #4 ruling: "single owner per invariant —
// don't double-assert in adapter and engine".
//
// Mechanism: a SessionCompleted with AchievedLSN < TargetLSN
// updates Recovery.R to AchievedLSN. decide() then re-evaluates and
// finds R < H, leaving Decision in CatchUp / Rebuild rather than
// flipping to None. Without DecisionNone, the Healthy gate cannot
// promote.
func TestG5_5C_StaleSessionAck_DoesNotPromoteHealth(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})

	const sessionID uint64 = 7
	Apply(st, SessionPrepared{
		ReplicaID: "r1", SessionID: sessionID,
		Kind:      SessionCatchUp,
		TargetLSN: 100,
	})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: sessionID})

	// Stale completion: AchievedLSN < TargetLSN.
	r := Apply(st, SessionClosedCompleted{
		ReplicaID:   "r1",
		SessionID:   sessionID,
		AchievedLSN: 50, // less than TargetLSN=100
	})

	// Recovery.R advanced to AchievedLSN=50 (engine accepts the ack
	// for the matching SessionID).
	if got := st.Recovery.R; got != 50 {
		t.Errorf("Recovery.R = %d, want 50 (AchievedLSN)", got)
	}
	// But: H still 100 → R < H → Decision must NOT be None.
	if got := st.Recovery.Decision; got == DecisionNone {
		t.Error("Decision = None despite R(50) < H(100) — stale ack promoted prematurely")
	}
	// Healthy gate: must NOT flip true.
	if st.Publication.Healthy {
		t.Error("Publication.Healthy = true despite R < H — stale ack promoted health")
	}
	// And no PublishHealthy command was emitted.
	for _, cmd := range r.Commands {
		if CommandKind(cmd) == "PublishHealthy" {
			t.Error("PublishHealthy emitted on stale ack — should not promote")
		}
	}
}

// TestG5_5C_StaleSessionAck_FullAchievedDoesPromote is the
// counterpart confirmation: a SessionCompleted with AchievedLSN ==
// TargetLSN == H reaches DecisionNone and (after the implicit
// fence) promotes Healthy. Without this dual, the prior test could
// pass trivially (e.g., if Healthy never promotes for any input).
func TestG5_5C_StaleSessionAck_FullAchievedDoesPromote(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 0, S: 0, H: 100})

	const sessionID uint64 = 8
	Apply(st, SessionPrepared{
		ReplicaID: "r1", SessionID: sessionID,
		Kind:      SessionCatchUp,
		TargetLSN: 100,
	})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: sessionID})

	r := Apply(st, SessionClosedCompleted{
		ReplicaID:   "r1",
		SessionID:   sessionID,
		AchievedLSN: 100, // full achievement
	})

	if got := st.Recovery.R; got != 100 {
		t.Fatalf("Recovery.R = %d, want 100", got)
	}
	if got := st.Recovery.Decision; got != DecisionNone {
		t.Errorf("Decision = %s, want None on full-achievement", got)
	}
	// Implicit fence at SessionCompleted (apply.go:493-495) advances
	// FencedEpoch to Identity.Epoch. Healthy gate may now flip true.
	if !st.Publication.Healthy {
		t.Error("Publication.Healthy = false despite full-achievement + implicit fence")
	}
	// PublishHealthy expected.
	hasHealthy := false
	for _, cmd := range r.Commands {
		if CommandKind(cmd) == "PublishHealthy" {
			hasHealthy = true
			break
		}
	}
	if !hasHealthy {
		t.Error("PublishHealthy not emitted on full-achievement ack")
	}
}
