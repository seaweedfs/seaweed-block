package engine

import "testing"

// A replica that has not yet accepted the current epoch holds only entries
// written at older epochs. Above the new primary's start position those were
// never acknowledged (an acknowledged write is on the promotion target), so a
// replica reporting R above that start holds a divergent tail: it is rebuilt,
// never fenced into healthy because R >= H, and never counted as caught up.
func TestDivergentTailBeforeTheEpochIsAcceptedIsRebuilt(t *testing.T) {
	st := &ReplicaState{}
	Apply(st, AssignmentObserved{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 1,
		DataAddr: "127.0.0.1:9333", CtrlAddr: "127.0.0.1:9334",
		PrimaryStartLSN: 90, // the new primary's durable LSN when it took the role at epoch 2
	})
	Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 2})
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 95})

	assertHasCommand(t, r, "StartRebuild")
	assertNoCommand(t, r, "FenceAtEpoch")
	assertNoCommand(t, r, "PublishHealthy")
	if st.Recovery.Decision != DecisionRebuild || st.Recovery.DecisionReason != "divergent_tail" || !st.Recovery.RebuildPinned {
		t.Fatalf("decision %s (%s), pinned %t; want rebuild (divergent_tail), pinned", st.Recovery.Decision, st.Recovery.DecisionReason, st.Recovery.RebuildPinned)
	}
	// A later probe with R >= H does not downgrade it.
	r = Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 100})
	assertNoCommand(t, r, "FenceAtEpoch")
	assertNoCommand(t, r, "PublishHealthy")
}

// The controls: at or below the start position the classification is the
// ordinary one, and once the replica has accepted the epoch its R above the
// start came from this primary.
func TestDivergentTailControls(t *testing.T) {
	for _, c := range []struct {
		name    string
		r, h    uint64
		fenced  bool
		want    string // a command the decision must issue
		without string // a command it must not
	}{
		{"R equal to the start, caught up", 90, 90, false, "FenceAtEpoch", "StartRebuild"},
		{"R below the start, catch-up", 80, 95, false, "StartCatchUp", "StartRebuild"},
		{"epoch accepted, R above the start from this primary", 100, 100, true, "PublishHealthy", "StartRebuild"},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := &ReplicaState{}
			Apply(st, AssignmentObserved{VolumeID: "vol1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1, DataAddr: "a", CtrlAddr: "b", PrimaryStartLSN: 90})
			Apply(st, ProbeSucceeded{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 2})
			if c.fenced {
				Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 90, S: 10, H: 90})
				Apply(st, FenceCompleted{ReplicaID: "r1", Epoch: 2, EndpointVersion: 1})
			}
			r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: c.r, S: 10, H: c.h})
			assertHasCommand(t, r, c.want)
			assertNoCommand(t, r, c.without)
		})
	}
}

// Without a start position (a primary that was never promoted, or a host that
// does not supply one) the classification is unchanged.
func TestDivergentTailNeedsAStartPosition(t *testing.T) {
	st := &ReplicaState{}
	assignAndProbe(st)
	r := Apply(st, RecoveryFactsObserved{ReplicaID: "r1", R: 100, S: 10, H: 95})
	assertHasCommand(t, r, "FenceAtEpoch")
	assertNoCommand(t, r, "StartRebuild")
}
