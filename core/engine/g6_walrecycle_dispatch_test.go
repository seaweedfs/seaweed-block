package engine

import (
	"testing"
)

// G6 §1.A α + §2 #3 engine table-driven dispatch test:
//
// When a recovery session fails with a typed FailureKind, the engine
// branches:
//   - WALRecycled  → Decision=Rebuild + RebuildPinned=true + emit StartRebuild
//   - Transport    → Attempts++ + retry catch-up (if budget remains)
//   - SubstrateIO  → Attempts++ + retry catch-up (if budget remains)
//   - StartTimeout → no retry (executor never started; bypass budget)
//
// G5-5C Batch 4's TestG5_5C_Dispatch_CatchUpVsRebuild_TableDriven
// already pinned the (R, S, H) → Decision shape on a fresh probe.
// This test pins the orthogonal axis: how a session-CLOSE event with
// a typed FailureKind affects dispatch. The two together cover both
// entry points to Decision=Rebuild.
//
// Pinned by:
//   - INV-G6-WALRECYCLE-DISPATCHES-REBUILD (positive: WAL-recycled
//     close emits StartRebuild and pins rebuild against future probes)
//   - INV-G6-ENGINE-NO-REBUILD-PINNED-ON-OTHER-FAILURES (negative:
//     only WALRecycled pins; Transport / SubstrateIO stay on catch-up
//     retry path)

// helperPrepareRunningSession primes a state machine to "in-flight
// catch-up session" so applySessionClosedFailed has something to
// terminate. Returns the SessionID.
func helperPrepareRunningSession(t *testing.T, st *ReplicaState, kind SessionKind) uint64 {
	t.Helper()
	const sessionID uint64 = 42
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})
	Apply(st, SessionPrepared{
		ReplicaID: "r1", SessionID: sessionID,
		Kind:      kind,
		TargetLSN: 100,
	})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: sessionID})
	if st.Session.Phase != PhaseRunning {
		t.Fatalf("setup: Phase = %s, want PhaseRunning", st.Session.Phase)
	}
	return sessionID
}

// TestG6_SessionFailed_DispatchByFailureKind_TableDriven pins how
// each FailureKind shapes the engine response.
func TestG6_SessionFailed_DispatchByFailureKind_TableDriven(t *testing.T) {
	cases := []struct {
		name              string
		failureKind       RecoveryFailureKind
		wantDecision      RecoveryDecision
		wantRebuildPinned bool
		wantCmd           string // expected command emit on this Apply: "StartRebuild" / "StartCatchUp" / ""
	}{
		{
			name:              "wal_recycled_pins_rebuild_and_emits_start_rebuild",
			failureKind:       RecoveryFailureWALRecycled,
			wantDecision:      DecisionRebuild,
			wantRebuildPinned: true,
			wantCmd:           "StartRebuild",
		},
		{
			name:              "transport_failure_does_not_pin_rebuild",
			failureKind:       RecoveryFailureTransport,
			wantDecision:      DecisionCatchUp, // unchanged from setup; retry budget engages
			wantRebuildPinned: false,
			wantCmd:           "StartCatchUp", // engine retries within budget
		},
		{
			name:              "substrate_io_does_not_pin_rebuild",
			failureKind:       RecoveryFailureSubstrateIO,
			wantDecision:      DecisionCatchUp, // unchanged; retry budget engages
			wantRebuildPinned: false,
			wantCmd:           "StartCatchUp",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := &ReplicaState{}
			sessionID := helperPrepareRunningSession(t, st, SessionCatchUp)

			r := Apply(st, SessionClosedFailed{
				ReplicaID:   "r1",
				SessionID:   sessionID,
				FailureKind: c.failureKind,
				Reason:      "test-induced " + c.name,
			})

			if got := st.Recovery.Decision; got != c.wantDecision {
				t.Errorf("Recovery.Decision = %s, want %s", got, c.wantDecision)
			}
			if got := st.Recovery.RebuildPinned; got != c.wantRebuildPinned {
				t.Errorf("Recovery.RebuildPinned = %v, want %v", got, c.wantRebuildPinned)
			}

			if c.wantCmd == "" {
				assertNoCommand(t, r, "StartRebuild")
				assertNoCommand(t, r, "StartCatchUp")
			} else {
				assertHasCommand(t, r, c.wantCmd)
				// Strict mutual exclusion: only one recovery cmd.
				other := "StartCatchUp"
				if c.wantCmd == "StartCatchUp" {
					other = "StartRebuild"
				}
				assertNoCommand(t, r, other)
			}
		})
	}
}

// TestG6_RebuildPinned_StaysSticky_AcrossProbe verifies the
// load-bearing piece of INV-G6-WALRECYCLE-DISPATCHES-REBUILD: once
// the engine pins Rebuild via WAL-recycled close, a subsequent
// probe-derived (R, S, H) classification cannot downgrade Decision
// back to CatchUp. Without this, a transient probe arriving
// mid-rebuild could re-classify the gap as catch-up-eligible and
// leave the replica permanently stuck.
func TestG6_RebuildPinned_StaysSticky_AcrossProbe(t *testing.T) {
	st := &ReplicaState{}
	sessionID := helperPrepareRunningSession(t, st, SessionCatchUp)

	// Force a WAL-recycled close → pins rebuild.
	Apply(st, SessionClosedFailed{
		ReplicaID:   "r1",
		SessionID:   sessionID,
		FailureKind: RecoveryFailureWALRecycled,
		Reason:      "wal recycled at the boundary",
	})
	if !st.Recovery.RebuildPinned {
		t.Fatal("setup: RebuildPinned should be true after WAL-recycled close")
	}
	if st.Recovery.Decision != DecisionRebuild {
		t.Fatalf("setup: Decision = %s, want Rebuild", st.Recovery.Decision)
	}

	// New probe arrives reporting R=50 / S=10 / H=100 — would
	// classify as DecisionCatchUp under the strict (R, S, H) decide
	// rule. RebuildPinned must override.
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 50, S: 10, H: 100})
	if st.Recovery.Decision != DecisionRebuild {
		t.Errorf("post-probe Decision = %s, want Rebuild (RebuildPinned should override)",
			st.Recovery.Decision)
	}
	// And no catch-up emit — pin must filter the dispatch too.
	assertNoCommand(t, r, "StartCatchUp")
}

// TestG6_WALRecycled_PublishDegradedSurfaceFires verifies the
// diagnostic surface: when WAL-recycled triggers, engine emits
// PublishDegraded so the runtime layer can call peer.Invalidate /
// log the reason. This is the operator-visible signal that's load-
// bearing for QA's wait_until_rebuild_dispatched harness (which
// scrapes for both rebuild-start AND degraded-published markers).
func TestG6_WALRecycled_PublishDegradedSurfaceFires(t *testing.T) {
	st := &ReplicaState{}
	sessionID := helperPrepareRunningSession(t, st, SessionCatchUp)

	r := Apply(st, SessionClosedFailed{
		ReplicaID:   "r1",
		SessionID:   sessionID,
		FailureKind: RecoveryFailureWALRecycled,
		Reason:      "wal recycled at the boundary",
	})

	// Both PublishDegraded AND StartRebuild should emit on the same
	// Apply — the diagnostic surface fires alongside the recovery
	// dispatch.
	assertHasCommand(t, r, "PublishDegraded")
	assertHasCommand(t, r, "StartRebuild")
}
