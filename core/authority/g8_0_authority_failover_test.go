package authority

import "testing"

// G8-0 pins the authority-only failover contract. These tests do
// not touch engine/proto/frontend: the controller may only emit a
// publisher ask, and the publisher remains the sole epoch minter.

func TestG8_0_TopologyController_FailoverRequiresReadyEligibleReachableCandidate(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 7, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)

	r1 := candidate("r1", "s1", "d1", "c1", 10)
	r1.Reachable = false
	r2 := candidate("r2", "s2", "d2", "c2", 100)
	r2.ReadyForPrimary = false // high evidence is not enough.
	r3 := candidate("r3", "s3", "d3", "c3", 1)

	snap := ClusterSnapshot{
		CollectedRevision: 11,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1",
				AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 7, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"},
				r1, r2, r3,
			),
		},
	}

	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.Intent != IntentReassign || ask.ReplicaID != "r3" {
		t.Fatalf("failover must choose the only ready+eligible+reachable candidate r3, got %+v", ask)
	}
}

func TestG8_0_TopologyController_NoEligibleFailoverCandidate_DoesNotMint(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 3, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)

	r1 := candidate("r1", "s1", "d1", "c1", 10)
	r1.Reachable = false
	r2 := candidate("r2", "s2", "d2", "c2", 20)
	r2.ReadyForPrimary = false
	r3 := candidate("r3", "s3", "d3", "c3", 30)
	r3.Eligible = false

	snap := ClusterSnapshot{
		CollectedRevision: 12,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1",
				AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"},
				r1, r2, r3,
			),
		},
	}

	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	expectNoNext(t, ctrl)
	if desired, ok := ctrl.DesiredFor("v1"); ok {
		t.Fatalf("no eligible failover candidate must not leave a desired mint, got %+v", desired)
	}
}

func TestG8_0_Publisher_ReassignAdvancesPerVolumeEpochAcrossReplicaSlots(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind,
	}); err != nil {
		t.Fatalf("bind r1: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2", Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("reassign r2: %v", err)
	}

	line, ok := pub.VolumeAuthorityLine("v1")
	if !ok {
		t.Fatal("missing volume authority line after reassign")
	}
	if line.ReplicaID != "r2" || line.Epoch != 2 || line.EndpointVersion != 1 {
		t.Fatalf("reassign must mint a new per-volume epoch on r2, got %+v", line)
	}

	old, ok := pub.LastPublished("v1", "r1")
	if !ok || old.Epoch != 1 {
		t.Fatalf("old slot record should remain historical r1@1 for subscription catch-up, got %+v ok=%v", old, ok)
	}
}
