package authority

import "testing"

func TestG9A_ReturnedReplicaCandidateRequiresReadyForPrimaryProgressFact(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r2", AuthorityBasis{
		Assigned: true, ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
		DataAddr: "d2", CtrlAddr: "c2",
	})
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)

	r1 := candidate("r1", "s1", "d1", "c1", 1_000)
	r1.ReadyForPrimary = false // returned/observed is not reintegrated.
	r2 := candidate("r2", "s2", "d2", "c2", 100)
	r2.Reachable = false // force failover pressure away from current primary.
	r3 := candidate("r3", "s3", "d3", "c3", 10)

	snap := ClusterSnapshot{
		CollectedRevision: 30,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1",
				AuthorityBasis{Assigned: true, ReplicaID: "r2", Epoch: 2, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2"},
				r1, r2, r3,
			),
		},
	}

	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.Intent != IntentReassign || ask.ReplicaID != "r3" {
		t.Fatalf("returned r1 must not be failover target until progress-ready; got %+v", ask)
	}
}
