package engine

import (
	"testing"
)

// T4d-3 engine-side R+1 threading tests. Per G-1 §7 test parity matrix.

// TestT4d3_CatchUp_Engine_RPlus1_FromOwnState_NotProbeDirectly pins
// INV-REPL-CATCHUP-FROMLSN-FROM-ENGINE-STATE-NOT-PROBE: engine
// populates StartCatchUp.FromLSN from its OWN Recovery.R state, NOT
// from the latest probe payload directly. Probe results are ingested
// as facts that update engine state; the command emit path reads the
// updated state.
//
// Setup: probe #1 reports R=50; engine state updated. Probe #2
// reports R=60 (replica advanced via live lane between probes).
// Engine emits StartCatchUp using the CURRENT state (R=60+1=61),
// NOT a stale probe-direct value.
func TestT4d3_CatchUp_Engine_RPlus1_FromOwnState_NotProbeDirectly(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{
			ReplicaID:       "r1",
			Epoch:           1,
			EndpointVersion: 1,
			MemberPresent:   true, // required for decide()
		},
		Reachability: ReachabilityTruth{
			Status:                  ProbeReachable,
			FencedEpoch:             1,
			ObservedEndpointVersion: 1,
			TransportEpoch:          1,
		},
	}

	// Probe #1: R=50, S=10, H=100. Triggers catch-up decide.
	r1 := Apply(st, RecoveryFactsObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		R:               50, S: 10, H: 100,
	})
	got1 := findStartCatchUp(t, r1.Commands)
	if got1.FromLSN != 51 {
		t.Errorf("first emit: FromLSN = %d, want 51 (R=50 + 1)", got1.FromLSN)
	}
	if got1.TargetLSN != 100 {
		t.Errorf("first emit: TargetLSN = %d, want 100 (H)", got1.TargetLSN)
	}

	// Clear session for next decide pass.
	st.Session = SessionTruth{}

	// Probe #2: R=60 (replica advanced via live ship between probes).
	// Engine should emit using CURRENT state — R=60+1=61, NOT stale.
	r2 := Apply(st, RecoveryFactsObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		R:               60, S: 10, H: 100,
	})
	got2 := findStartCatchUp(t, r2.Commands)
	if got2.FromLSN != 61 {
		t.Errorf("second emit: FromLSN = %d, want 61 (R=60 + 1) — engine MUST use CURRENT state, not stale", got2.FromLSN)
	}
}

// TestT4d3_CatchUp_RetryReusesOriginalRPlus1 pins G-1 §6.2 architect
// Option A: engine retry re-emit reuses the ORIGINAL Recovery.R + 1
// (no inter-attempt re-probe). Apply gate (T4d-2) handles any over-
// ship via per-LBA stale-skip — the safety claim is that re-shipping
// already-applied entries is harmless.
func TestT4d3_CatchUp_RetryReusesOriginalRPlus1(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session:  SessionTruth{SessionID: 7, Phase: PhaseRunning},
		Recovery: RecoveryTruth{
			Decision: DecisionCatchUp,
			R:        50,
			H:        100,
		},
	}

	// First catch-up failure (non-recycle): engine should re-emit
	// StartCatchUp with FromLSN = R+1 = 51 (NOT re-probed).
	ev := SessionClosedFailed{
		ReplicaID:   "r1",
		SessionID:   7,
		FailureKind: RecoveryFailureTransport,
		Reason:      "transient stream error",
	}
	r := Apply(st, ev)
	got := findStartCatchUp(t, r.Commands)
	if got.FromLSN != 51 {
		t.Errorf("retry re-emit: FromLSN = %d, want 51 (R+1 reused; G-1 §6.2 Option A — no re-probe)", got.FromLSN)
	}
	if got.TargetLSN != 100 {
		t.Errorf("retry re-emit: TargetLSN = %d, want 100 (H reused)", got.TargetLSN)
	}
}

// TestT4d3_StartCatchUpStruct_HasFromLSNField — compile-time fence:
// signature change pins. If a future commit removes FromLSN, this
// fails to compile.
func TestT4d3_StartCatchUpStruct_HasFromLSNField(t *testing.T) {
	cmd := StartCatchUp{
		ReplicaID:       "r1",
		Epoch:           1,
		EndpointVersion: 1,
		FromLSN:         51,
		TargetLSN:       100,
	}
	if cmd.FromLSN != 51 {
		t.Errorf("FromLSN field accessor broken")
	}
}

// findStartCatchUp helper for T4d-3 tests.
func findStartCatchUp(t *testing.T, cmds []Command) StartCatchUp {
	t.Helper()
	for _, c := range cmds {
		if sc, ok := c.(StartCatchUp); ok {
			return sc
		}
	}
	t.Fatalf("no StartCatchUp command in %v", cmds)
	return StartCatchUp{}
}
