package engine

import "testing"

func TestDurableAckObserved_RecordsProgressWithoutDecidingHealth(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID:        "v1",
		ReplicaID:       "r1",
		Epoch:           1,
		EndpointVersion: 1,
		DataAddr:        "data",
		CtrlAddr:        "ctrl",
	})
	Apply(st, ProbeSucceeded{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
	})
	Apply(st, RecoveryFactsObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		R:               5,
		S:               5,
		H:               10,
	})
	if st.Recovery.Decision != DecisionCatchUp {
		t.Fatalf("test setup: decision=%s want %s", st.Recovery.Decision, DecisionCatchUp)
	}

	result := Apply(st, DurableAckObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		DurableLSN:      8,
		PrimaryTailLSN:  5,
		PrimaryHeadLSN:  10,
	})

	if st.Recovery.DurableAckR != 8 || !st.Recovery.DurableAckKnown {
		t.Fatalf("DurableAck=(%d known=%v), want (8 true)",
			st.Recovery.DurableAckR, st.Recovery.DurableAckKnown)
	}
	if st.Recovery.R != 5 {
		t.Fatalf("Recovery.R=%d want original probe R=5; durable ack is not yet the recovery-class input", st.Recovery.R)
	}
	if st.Recovery.Decision != DecisionCatchUp {
		t.Fatalf("decision changed to %s; durable ack should not decide health/recovery in this slice",
			st.Recovery.Decision)
	}
	for _, cmd := range result.Commands {
		switch cmd.(type) {
		case PublishHealthy, PublishDegraded, StartCatchUp, StartRebuild:
			t.Fatalf("durable ack must not emit command %T in this slice", cmd)
		}
	}
}

func TestDurableAckObserved_IsMonotonic(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID:        "v1",
		ReplicaID:       "r1",
		Epoch:           1,
		EndpointVersion: 1,
		DataAddr:        "data",
		CtrlAddr:        "ctrl",
	})

	Apply(st, DurableAckObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		DurableLSN:      9,
		PrimaryTailLSN:  5,
		PrimaryHeadLSN:  10,
	})
	Apply(st, DurableAckObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		DurableLSN:      7,
		PrimaryTailLSN:  5,
		PrimaryHeadLSN:  10,
	})
	if st.Recovery.DurableAckR != 9 {
		t.Fatalf("DurableAckR regressed to %d want 9", st.Recovery.DurableAckR)
	}
}
