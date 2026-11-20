package engine

import "testing"

func TestPromotionReadyRefusesDelayedProbeDuringNewRecovery(t *testing.T) {
	st := helperPrime(t, 50, 10, 100)
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, FrontierHint: 100})
	Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})
	Apply(st, DurableAckObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, DurableLSN: 100, PrimaryTailLSN: 10, PrimaryHeadLSN: 100})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, R: 100, S: 10, H: 150})
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 2, Kind: SessionCatchUp, FrontierHint: 150})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, R: 50, S: 10, H: 100})
	if fact := BuildPromotionReadyFact(st); fact.Ready {
		t.Fatalf("active recovery became promotion ready after delayed probe: %+v", fact)
	}
}

func TestMissingProbeAckRestoresNewerDurableProgress(t *testing.T) {
	st := helperPrime(t, 50, 10, 100)
	Apply(st, SessionPrepared{ReplicaID: "r1", SessionID: 1, Kind: SessionCatchUp, FrontierHint: 100})
	Apply(st, SessionClosedCompleted{ReplicaID: "r1", SessionID: 1, AchievedLSN: 100})
	Apply(st, RecoveryFactsObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1})
	result := Apply(st, DurableAckObserved{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, DurableLSN: 150, PrimaryTailLSN: 110, PrimaryHeadLSN: 150})
	if st.Recovery.R != 150 || st.Recovery.Decision != DecisionNone {
		t.Fatalf("missing-probe ACK retained stale progress: R=%d decision=%s", st.Recovery.R, st.Recovery.Decision)
	}
	assertNoCommand(t, result, "StartRebuild")
	assertNoCommand(t, result, "StartCatchUp")
}
