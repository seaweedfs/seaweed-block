package authority

import (
	"testing"
	"time"
)

func TestG9B_ObservationLocalPrimaryClaim_DoesNotMintAuthority(t *testing.T) {
	reader := newFakeReaderForHost()
	topo := AcceptedTopology{
		Volumes: []VolumeExpected{{
			VolumeID: "v1",
			Slots: []ExpectedSlot{
				{ReplicaID: "r1", ServerID: "s1"},
				{ReplicaID: "r2", ServerID: "s2"},
				{ReplicaID: "r3", ServerID: "s3"},
			},
		}},
	}
	now := time.Now()
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: time.Minute}, func() time.Time { return now })
	for _, obs := range []Observation{
		{
			ServerID:   "s1",
			ObservedAt: now,
			Server:     ServerFact{Reachable: true, Eligible: true},
			Slots: []SlotFact{{
				VolumeID: "v1", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1",
				Reachable: true, ReadyForPrimary: true, Eligible: true,
				LocalRoleClaim: LocalRolePrimary,
			}},
		},
		{
			ServerID:   "s2",
			ObservedAt: now,
			Server:     ServerFact{Reachable: true, Eligible: true},
			Slots: []SlotFact{{
				VolumeID: "v1", ReplicaID: "r2", DataAddr: "d2", CtrlAddr: "c2",
				Reachable: true, ReadyForPrimary: true, Eligible: true,
			}},
		},
		{
			ServerID:   "s3",
			ObservedAt: now,
			Server:     ServerFact{Reachable: true, Eligible: true},
			Slots: []SlotFact{{
				VolumeID: "v1", ReplicaID: "r3", DataAddr: "d3", CtrlAddr: "c3",
				Reachable: true, ReadyForPrimary: true, Eligible: true,
			}},
		},
	} {
		if err := store.Ingest(obs); err != nil {
			t.Fatalf("Ingest(%s): %v", obs.ServerID, err)
		}
	}

	result := BuildSnapshot(store.Snapshot(), topo, reader)
	if len(result.Snapshot.Volumes) != 1 {
		t.Fatalf("supported volumes = %d want 1", len(result.Snapshot.Volumes))
	}
	if result.Snapshot.Volumes[0].Authority.Assigned {
		t.Fatalf("local primary claim must not mint authority: %+v", result.Snapshot.Volumes[0].Authority)
	}
	if _, ok := reader.VolumeAuthorityLine("v1"); ok {
		t.Fatal("observation path unexpectedly created publisher authority line")
	}
}

func TestG9B_GenesisPlacement_EmitsBindButNotAssignmentInfo(t *testing.T) {
	reader := newFakeBasisReader()
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)
	snap := ClusterSnapshot{
		CollectedRevision: 1,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1", AuthorityBasis{},
				candidate("r1", "s1", "d1", "c1", 10),
				candidate("r2", "s2", "d2", "c2", 30),
				candidate("r3", "s3", "d3", "c3", 20),
			),
		},
	}

	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.Intent != IntentBind || ask.ReplicaID != "r2" {
		t.Fatalf("genesis placement ask = %+v want IntentBind/r2", ask)
	}
	if _, ok := reader.VolumeAuthorityLine("v1"); ok {
		t.Fatal("controller must emit AssignmentAsk only; publisher line should still be absent")
	}
}

func TestG9B_PublisherBind_MintsGenesisPrimaryLine(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))

	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1", DataAddr: "d1", CtrlAddr: "c1", Intent: IntentBind,
	}); err != nil {
		t.Fatalf("apply Bind: %v", err)
	}
	line, ok := pub.VolumeAuthorityLine("v1")
	if !ok {
		t.Fatal("missing genesis authority line")
	}
	if !line.Assigned || line.ReplicaID != "r1" || line.Epoch != 1 || line.EndpointVersion != 1 {
		t.Fatalf("genesis line = %+v want r1@epoch1/ev1", line)
	}
}
