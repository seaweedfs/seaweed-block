package engine

import "testing"

func TestRecoveryProbeKeepsCompletedRButAllowsNewWorkAndIdentity(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, R: 50, S: 10, H: 100})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, TargetLSN: 100})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 1})
	Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, R: 50, S: 10, H: 100})
	assertNoCommand(t, r, "StartCatchUp")
	if st.Recovery.R != 100 {
		t.Fatalf("old probe lowered completed R: %d", st.Recovery.R)
	}
	// An unknown probe must not turn the retained R into invented primary
	// boundaries or publish healthy. A later valid observation recovers it.
	r = Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	if r.Projection.RecoveryDecision != DecisionUnknown {
		t.Fatalf("retained R fabricated known boundaries: %+v", r.Projection)
	}
	r = Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	if r.Projection.RecoveryDecision != DecisionUnknown {
		t.Fatal("reachability fabricated missing recovery facts")
	}
	assertNoCommand(t, r, "PublishHealthy")
	r = Apply(st, DurableAckObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, DurableLSN: 100, PrimaryTailLSN: 10, PrimaryHeadLSN: 100})
	if r.Projection.RecoveryDecision != DecisionNone || st.Recovery.FactsMissing {
		t.Fatalf("valid ack did not restore known facts: %+v", r.Projection)
	}
	// A higher primary head remains real new work, starting after durable R.
	r = Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, R: 50, S: 10, H: 150})
	start := findStartCatchUp(t, r.Commands)
	if start.FromLSN != 101 || start.EffectiveFrontierHint() != 150 {
		t.Fatalf("new work lost or replayed completed prefix: %+v", start)
	}
	// Replacement identity must not inherit the old replica's progress floor.
	Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 2})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 2, TransportEpoch: 2})
	r = Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 2, TransportEpoch: 2, R: 0, S: 10, H: 150})
	assertHasCommand(t, r, "StartRebuild")
}
