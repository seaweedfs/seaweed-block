package engine

import "testing"

func TestG9C_RecoveredReplica_NotHealthyUntilPostCloseDurableAck(t *testing.T) {
	st := g9cRecoveredReplicaBeforeClose()

	r := Apply(st, SessionClosedCompleted{
		ReplicaID:   "r1",
		SessionID:   11,
		AchievedLSN: 100,
	})

	if st.Session.Phase != PhaseCompleted {
		t.Fatalf("session phase=%s want completed", st.Session.Phase)
	}
	assertNoCommand(t, r, "PublishHealthy")
	if r.Projection.Mode == ModeHealthy {
		t.Fatalf("recovered replica became healthy before post-close durable ack")
	}
}

func TestG9C_RecoveredReplica_PostCloseDurableAckPublishesHealthy(t *testing.T) {
	st := g9cRecoveredReplicaBeforeClose()
	Apply(st, SessionClosedCompleted{
		ReplicaID:   "r1",
		SessionID:   11,
		AchievedLSN: 100,
	})

	r := Apply(st, DurableAckObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		DurableLSN:      100,
		PrimaryTailLSN:  10,
		PrimaryHeadLSN:  100,
	})

	assertHasCommand(t, r, "PublishHealthy")
	if r.Projection.Mode != ModeHealthy {
		t.Fatalf("after post-close durable ack mode=%s want healthy", r.Projection.Mode)
	}
}

func g9cRecoveredReplicaBeforeClose() *ReplicaState {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, RecoveryFactsObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		R:               20,
		S:               10,
		H:               100,
	})
	Apply(st, SessionPrepared{
		ReplicaID:    "r1",
		SessionID:    11,
		Kind:         SessionCatchUp,
		FrontierHint: 100,
	})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 11})
	return st
}
