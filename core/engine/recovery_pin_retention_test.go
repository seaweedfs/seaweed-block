package engine

// G7-redo priority #3 [retry] Option A — engine-side retry-budget
// bypass for `RecoveryFailurePinUnderRetention`.
//
// Architect ratified 2026-04-29: PinUnderRetention is a mid-session
// pin/retention contract violation. Engine MUST treat it as terminal
// for the active lineage (skip retry, leave Attempts untouched, emit
// `PublishDegraded`) so the next probe mints a fresh lineage with
// `fromLSN ≥ current S`. Same shape as `RecoveryFailureStartTimeout`.
//
// Pinned by INV-PIN-COMPATIBLE-WITH-RETENTION at the engine
// retry-budget gate.

import "testing"

// TestT4d_PinUnderRetention_BypassesRetryBudget pins the headline
// behavior: a SessionClosedFailed with FailureKind=PinUnderRetention
// MUST NOT increment Attempts, MUST NOT re-emit Start*, MUST emit
// PublishDegraded.
//
// Negative pin (Counter-WALRecycled): unlike WALRecycled, this kind
// MUST NOT escalate to Rebuild on its own. The new lineage decision
// belongs to the next probe — pin-violation alone doesn't authorize
// engine to flip Decision.
func TestT4d_PinUnderRetention_BypassesRetryBudget(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session:  SessionTruth{SessionID: 7, Kind: SessionCatchUp, Phase: PhaseRunning},
		Recovery: RecoveryTruth{Decision: DecisionCatchUp, H: 100, Attempts: 1},
	}
	ev := SessionClosedFailed{
		ReplicaID:   "r1",
		SessionID:   7,
		FailureKind: RecoveryFailurePinUnderRetention,
		Reason:      "recovery: PinUnderRetention during pin-update: floor=240 < primaryS=250",
	}
	r := Apply(st, ev)

	// Attempts MUST NOT advance (architect: 不计入可重试次数). The
	// implementation choice we ship: leave Attempts at its prior value
	// (1, not bumped to 2). A fresh probe would reset Attempts to 0
	// when a new Decision is set, so this preserves observability of
	// "we tried once already and ran into a contract violation".
	if st.Recovery.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (pin violation MUST NOT count toward retry budget)",
			st.Recovery.Attempts)
	}

	// No Start* re-emit — the active lineage is dead.
	for _, cmd := range r.Commands {
		if _, ok := cmd.(StartCatchUp); ok {
			t.Error("PinUnderRetention must NOT re-emit StartCatchUp")
		}
		if _, ok := cmd.(StartRebuild); ok {
			t.Error("PinUnderRetention must NOT auto-escalate to StartRebuild")
		}
		if _, ok := cmd.(StartRecovery); ok {
			t.Error("PinUnderRetention must NOT re-emit StartRecovery")
		}
	}

	// PublishDegraded emitted — operator surface + lineage signal.
	hasDegraded := false
	for _, cmd := range r.Commands {
		if _, ok := cmd.(PublishDegraded); ok {
			hasDegraded = true
		}
	}
	if !hasDegraded {
		t.Error("PinUnderRetention must emit PublishDegraded")
	}

	// Decision MUST NOT auto-flip to Rebuild (counter-WALRecycled).
	// New lineage decision is the probe's job — engine doesn't know
	// here whether catch-up at fresh fromLSN will work or whether
	// rebuild is required. Leave Decision intact for the probe loop.
	if st.Recovery.Decision != DecisionCatchUp {
		t.Errorf("Decision = %s, want CatchUp (pin violation MUST NOT auto-flip Decision)",
			st.Recovery.Decision)
	}
	if st.Recovery.DecisionReason == "wal_recycled" {
		t.Error("DecisionReason = wal_recycled (PinUnderRetention must NOT borrow WALRecycled escalation path)")
	}
}

// TestT4d_PinUnderRetention_AtBudgetEdge_DoesNotEscalate pins the
// boundary: even when the replica has already burned through almost
// the entire retry budget on transient failures, a PinUnderRetention
// arriving on top of that budget MUST NOT flip into the catch-up-
// exhaustion-escalates-to-Rebuild path (round-47).
//
// Reason: that path is gated on Attempts > budget for transient
// failures; PinUnderRetention bypasses the increment entirely, so
// the gate never trips. This test pins that we don't accidentally
// share state.
func TestT4d_PinUnderRetention_AtBudgetEdge_DoesNotEscalate(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session:  SessionTruth{SessionID: 7, Kind: SessionCatchUp, Phase: PhaseRunning},
		Recovery: RecoveryTruth{
			Decision: DecisionCatchUp,
			R:        50, S: 10, H: 100,
			Attempts: 3, // already at wal_delta MaxRetries=3 edge
		},
	}
	ev := SessionClosedFailed{
		ReplicaID:   "r1",
		SessionID:   7,
		FailureKind: RecoveryFailurePinUnderRetention,
		Reason:      "recovery: PinUnderRetention during pin-update",
	}
	r := Apply(st, ev)

	// Decision still CatchUp (no auto-escalation).
	if st.Recovery.Decision != DecisionCatchUp {
		t.Errorf("Decision = %s, want CatchUp (pin violation must NOT trigger round-47 escalation)",
			st.Recovery.Decision)
	}
	// Attempts unchanged (still 3, not bumped to 4 → that would have
	// crossed the budget edge and triggered round-47 escalation).
	if st.Recovery.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3 (pin violation MUST NOT bump counter)", st.Recovery.Attempts)
	}
	// No StartRebuild emitted from round-47 path.
	for _, cmd := range r.Commands {
		if _, ok := cmd.(StartRebuild); ok {
			t.Error("PinUnderRetention must NOT trigger round-47 escalation to Rebuild")
		}
	}
}

// TestRecoveryFailureKind_PinUnderRetention_String pins the String()
// case so logs / traces emit "PinUnderRetention" (not the default
// "Unknown" fall-through). Diagnostic-only but pinned because the
// rest of the codebase greps trace text in tests.
func TestRecoveryFailureKind_PinUnderRetention_String(t *testing.T) {
	if got := RecoveryFailurePinUnderRetention.String(); got != "PinUnderRetention" {
		t.Errorf("RecoveryFailurePinUnderRetention.String() = %q, want %q", got, "PinUnderRetention")
	}
}
