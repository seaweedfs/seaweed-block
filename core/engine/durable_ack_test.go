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

func TestDurableAckObserved_ProgressingLagDoesNotStartRecovery(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)

	for _, ack := range []DurableAckObserved{
		durableAckEvent(10, 5, 20),
		durableAckEvent(11, 5, 25),
		durableAckEvent(12, 5, 30),
	} {
		r := Apply(st, ack)
		assertNoCommand(t, r, "StartCatchUp")
		assertNoCommand(t, r, "StartRebuild")
	}

	if st.Recovery.LagDecision != LagDecisionKeepFeeding {
		t.Fatalf("LagDecision=%s want %s", st.Recovery.LagDecision, LagDecisionKeepFeeding)
	}
}

func TestDurableAckObserved_StalledLagStartsCatchUp(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)

	r1 := Apply(st, durableAckEvent(10, 5, 20))
	assertNoCommand(t, r1, "StartCatchUp")
	r2 := Apply(st, durableAckEvent(10, 5, 25))
	assertNoCommand(t, r2, "StartCatchUp")
	r3 := Apply(st, durableAckEvent(10, 5, 30))

	cmd := findDurableAckStartCatchUp(t, r3.Commands)
	if cmd.FromLSN != 11 {
		t.Fatalf("FromLSN=%d want durable ack R+1=11", cmd.FromLSN)
	}
	if cmd.FrontierHint != 30 || cmd.TargetLSN != 30 {
		t.Fatalf("frontier/target=(%d,%d), want (30,30)",
			cmd.FrontierHint, cmd.TargetLSN)
	}
	if st.Recovery.Decision != DecisionCatchUp {
		t.Fatalf("Decision=%s want %s", st.Recovery.Decision, DecisionCatchUp)
	}
	if st.Recovery.DecisionReason != "durable_ack_stalled_within_wal" {
		t.Fatalf("DecisionReason=%q", st.Recovery.DecisionReason)
	}
}

func TestDurableAckObserved_BelowRetentionStartsRebuild(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)

	r := Apply(st, durableAckEvent(4, 5, 30))

	assertHasCommand(t, r, "StartRebuild")
	if st.Recovery.Decision != DecisionRebuild {
		t.Fatalf("Decision=%s want %s", st.Recovery.Decision, DecisionRebuild)
	}
	if st.Recovery.DecisionReason != "durable_ack_below_retention" {
		t.Fatalf("DecisionReason=%q", st.Recovery.DecisionReason)
	}
}

func TestDurableAckObserved_DoesNotStartRecoveryWhileSessionActive(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	Apply(st, SessionPrepared{
		ReplicaID:    "r1",
		SessionID:    7,
		Kind:         SessionCatchUp,
		FrontierHint: 30,
	})
	Apply(st, SessionStarted{ReplicaID: "r1", SessionID: 7})

	for _, ack := range []DurableAckObserved{
		durableAckEvent(10, 5, 20),
		durableAckEvent(10, 5, 25),
		durableAckEvent(10, 5, 30),
	} {
		r := Apply(st, ack)
		assertNoCommand(t, r, "StartCatchUp")
		assertNoCommand(t, r, "StartRebuild")
	}
	if st.Recovery.LagDecision != LagDecisionStartCatchUp {
		t.Fatalf("LagDecision=%s want %s", st.Recovery.LagDecision, LagDecisionStartCatchUp)
	}
}

func durableAckEvent(durableR, primaryS, primaryH uint64) DurableAckObserved {
	return DurableAckObserved{
		ReplicaID:       "r1",
		EndpointVersion: 1,
		TransportEpoch:  1,
		DurableLSN:      durableR,
		PrimaryTailLSN:  primaryS,
		PrimaryHeadLSN:  primaryH,
	}
}

func findDurableAckStartCatchUp(t *testing.T, cmds []Command) StartCatchUp {
	t.Helper()
	for _, cmd := range cmds {
		if c, ok := cmd.(StartCatchUp); ok {
			return c
		}
	}
	t.Fatalf("no StartCatchUp command in %v", cmds)
	return StartCatchUp{}
}
