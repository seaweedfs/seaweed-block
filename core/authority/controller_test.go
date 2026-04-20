package authority

import (
	"context"
	"errors"
	"testing"
	"time"
)

func clusterVolume(volumeID string, authority AuthorityBasis, slots ...ReplicaCandidate) VolumeTopologySnapshot {
	return VolumeTopologySnapshot{
		VolumeID:  volumeID,
		Authority: authority,
		Slots:     slots,
	}
}

func candidate(replicaID, serverID, dataAddr, ctrlAddr string, evidence uint64) ReplicaCandidate {
	return ReplicaCandidate{
		ReplicaID:       replicaID,
		ServerID:        serverID,
		DataAddr:        dataAddr,
		CtrlAddr:        ctrlAddr,
		Reachable:       true,
		ReadyForPrimary: true,
		Eligible:        true,
		EvidenceScore:   evidence,
	}
}

func nextAskOrFail(t *testing.T, d Directive) AssignmentAsk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ask, err := d.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return ask
}

func expectNoNext(t *testing.T, d Directive) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := d.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestTopologyController_InitialPlacementBalancesAcrossVolumes(t *testing.T) {
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
			clusterVolume("v1", AuthorityBasis{}, candidate("v1r1", "s1", "d11", "c11", 10), candidate("v1r2", "s2", "d12", "c12", 10), candidate("v1r3", "s3", "d13", "c13", 10)),
			clusterVolume("v2", AuthorityBasis{}, candidate("v2r1", "s1", "d21", "c21", 10), candidate("v2r2", "s2", "d22", "c22", 10), candidate("v2r3", "s3", "d23", "c23", 10)),
			clusterVolume("v3", AuthorityBasis{}, candidate("v3r1", "s1", "d31", "c31", 10), candidate("v3r2", "s2", "d32", "c32", 10), candidate("v3r3", "s3", "d33", "c33", 10)),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	seenServers := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		ask := nextAskOrFail(t, ctrl)
		var gotServer string
		for _, vol := range snap.Volumes {
			if vol.VolumeID != ask.VolumeID {
				continue
			}
			slot, ok := candidateByReplica(vol, ask.ReplicaID)
			if ok {
				gotServer = slot.ServerID
			}
		}
		seenServers[gotServer] = struct{}{}
	}
	if len(seenServers) != 3 {
		t.Fatalf("initial placement should spread across 3 servers, got %d", len(seenServers))
	}
}

func TestTopologyController_InitialPlacementUsesEvidenceTieBreakOnEqualLoad(t *testing.T) {
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
		t.Fatalf("equal-load placement should prefer highest evidence candidate, got %+v", ask)
	}
}

func TestTopologyController_FailoverUsesHighestEvidenceCandidate(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)
	snap := ClusterSnapshot{
		CollectedRevision: 2,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1",
				AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"},
				func() ReplicaCandidate { c := candidate("r1", "s1", "d1", "c1", 10); c.Reachable = false; return c }(),
				candidate("r2", "s2", "d2", "c2", 20),
				candidate("r3", "s3", "d3", "c3", 10),
			),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.Intent != IntentReassign || ask.ReplicaID != "r2" {
		t.Fatalf("want Reassign to r2, got %+v", ask)
	}
}

func TestTopologyController_RebalanceMovesToLighterServer(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "v1r1", AuthorityBasis{Assigned: true, ReplicaID: "v1r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d11", CtrlAddr: "c11"})
	reader.Set("v2", "v2r1", AuthorityBasis{Assigned: true, ReplicaID: "v2r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d21", CtrlAddr: "c21"})
	reader.Set("v3", "v3r2", AuthorityBasis{Assigned: true, ReplicaID: "v3r2", Epoch: 1, EndpointVersion: 1, DataAddr: "d32", CtrlAddr: "c32"})
	ctrl := NewTopologyController(TopologyControllerConfig{RebalanceSkew: 1}, reader)
	snap := ClusterSnapshot{
		CollectedRevision: 3,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1",
				AuthorityBasis{Assigned: true, ReplicaID: "v1r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d11", CtrlAddr: "c11"},
				candidate("v1r1", "s1", "d11", "c11", 10),
				candidate("v1r2", "s2", "d12", "c12", 10),
				candidate("v1r3", "s3", "d13", "c13", 30),
			),
			clusterVolume("v2",
				AuthorityBasis{Assigned: true, ReplicaID: "v2r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d21", CtrlAddr: "c21"},
				candidate("v2r1", "s1", "d21", "c21", 10),
				candidate("v2r2", "s2", "d22", "c22", 10),
				candidate("v2r3", "s3", "d23", "c23", 10),
			),
			clusterVolume("v3",
				AuthorityBasis{Assigned: true, ReplicaID: "v3r2", Epoch: 1, EndpointVersion: 1, DataAddr: "d32", CtrlAddr: "c32"},
				candidate("v3r1", "s1", "d31", "c31", 10),
				candidate("v3r2", "s2", "d32", "c32", 10),
				candidate("v3r3", "s3", "d33", "c33", 10),
			),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.VolumeID != "v1" || ask.Intent != IntentReassign || ask.ReplicaID != "v1r3" {
		t.Fatalf("want v1 rebalance to v1r3 on s3, got %+v", ask)
	}
}

func TestTopologyController_RebalanceSkipsWhenLoadAlreadyWithinBound(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "v1r1", AuthorityBasis{Assigned: true, ReplicaID: "v1r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d11", CtrlAddr: "c11"})
	reader.Set("v2", "v2r2", AuthorityBasis{Assigned: true, ReplicaID: "v2r2", Epoch: 1, EndpointVersion: 1, DataAddr: "d22", CtrlAddr: "c22"})
	ctrl := NewTopologyController(TopologyControllerConfig{RebalanceSkew: 1}, reader)
	snap := ClusterSnapshot{
		CollectedRevision: 5,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1",
				AuthorityBasis{Assigned: true, ReplicaID: "v1r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d11", CtrlAddr: "c11"},
				candidate("v1r1", "s1", "d11", "c11", 10),
				candidate("v1r2", "s2", "d12", "c12", 30),
				candidate("v1r3", "s3", "d13", "c13", 20),
			),
			clusterVolume("v2",
				AuthorityBasis{Assigned: true, ReplicaID: "v2r2", Epoch: 1, EndpointVersion: 1, DataAddr: "d22", CtrlAddr: "c22"},
				candidate("v2r1", "s1", "d21", "c21", 10),
				candidate("v2r2", "s2", "d22", "c22", 10),
				candidate("v2r3", "s3", "d23", "c23", 10),
			),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	expectNoNext(t, ctrl)
}

func TestTopologyController_OutOfTopologyAuthority_NoAskPlusEvidence(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r99", AuthorityBasis{
		Assigned: true, ReplicaID: "r99", Epoch: 5, EndpointVersion: 1,
		DataAddr: "dx", CtrlAddr: "cx",
	})
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)
	snap := ClusterSnapshot{
		CollectedRevision: 4,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1", AuthorityBasis{}, candidate("r1", "s1", "d1", "c1", 10), candidate("r2", "s2", "d2", "c2", 10), candidate("r3", "s3", "d3", "c3", 10)),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	expectNoNext(t, ctrl)
	ev, ok := ctrl.LastUnsupported("v1")
	if !ok {
		t.Fatal("expected unsupported evidence")
	}
	if ev.Basis.ReplicaID != "r99" {
		t.Fatalf("evidence replicaID: got %q want r99", ev.Basis.ReplicaID)
	}
}

func TestTopologyController_UnsupportedVolumeDoesNotBlockOtherVolumes(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r99", AuthorityBasis{
		Assigned: true, ReplicaID: "r99", Epoch: 5, EndpointVersion: 1,
		DataAddr: "dx", CtrlAddr: "cx",
	})
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)
	snap := ClusterSnapshot{
		CollectedRevision: 4,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1", AuthorityBasis{}, candidate("r1", "s1", "d1", "c1", 10), candidate("r2", "s2", "d2", "c2", 10), candidate("r3", "s3", "d3", "c3", 10)),
			clusterVolume("v2", AuthorityBasis{}, candidate("x1", "s1", "d4", "c4", 10), candidate("x2", "s2", "d5", "c5", 20), candidate("x3", "s3", "d6", "c6", 15)),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.VolumeID != "v2" || ask.Intent != IntentBind {
		t.Fatalf("want v2 Bind while v1 stays unsupported, got %+v", ask)
	}
	ev, ok := ctrl.LastUnsupported("v1")
	if !ok || ev.Basis.ReplicaID != "r99" {
		t.Fatalf("expected unsupported evidence for v1, got %+v ok=%v", ev, ok)
	}
}

func TestTopologyController_ObservedAuthorityClearsPending(t *testing.T) {
	reader := newFakeBasisReader()
	ctrl := NewTopologyController(TopologyControllerConfig{}, reader)
	initial := ClusterSnapshot{
		CollectedRevision: 1,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			clusterVolume("v1", AuthorityBasis{}, candidate("r1", "s1", "d1", "c1", 10), candidate("r2", "s2", "d2", "c2", 10), candidate("r3", "s3", "d3", "c3", 10)),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(initial); err != nil {
		t.Fatalf("SubmitClusterSnapshot initial: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.Intent != IntentBind {
		t.Fatalf("want Bind, got %+v", ask)
	}

	// Simulate publisher/cluster observation converging on the desired line.
	reader.Set("v1", ask.ReplicaID, AuthorityBasis{
		Assigned: true, ReplicaID: ask.ReplicaID, Epoch: 1, EndpointVersion: 1,
		DataAddr: ask.DataAddr, CtrlAddr: ask.CtrlAddr,
	})
	observed := initial
	observed.CollectedRevision = 2
	observed.Volumes[0].Authority = AuthorityBasis{
		Assigned: true, ReplicaID: ask.ReplicaID, Epoch: 1, EndpointVersion: 1,
		DataAddr: ask.DataAddr, CtrlAddr: ask.CtrlAddr,
	}
	if err := ctrl.SubmitClusterSnapshot(observed); err != nil {
		t.Fatalf("SubmitClusterSnapshot observed: %v", err)
	}
	expectNoNext(t, ctrl)
}

func TestTopologyController_StalePreConfirmSnapshotDoesNotDuplicateQueuedMove(t *testing.T) {
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
			clusterVolume("v1", AuthorityBasis{}, candidate("r1", "s1", "d1", "c1", 10), candidate("r2", "s2", "d2", "c2", 10), candidate("r3", "s3", "d3", "c3", 10)),
		},
	}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatalf("SubmitClusterSnapshot 1: %v", err)
	}
	ask := nextAskOrFail(t, ctrl)
	if ask.Intent != IntentBind {
		t.Fatalf("want Bind, got %+v", ask)
	}

	// Publisher has already advanced, but the collector is still
	// stale and reports Authority.Assigned=false. The controller
	// must NOT enqueue the same move again; the desired state stays
	// pending until an observed snapshot confirms it.
	reader.Set("v1", ask.ReplicaID, AuthorityBasis{
		Assigned: true, ReplicaID: ask.ReplicaID, Epoch: 1, EndpointVersion: 1,
		DataAddr: ask.DataAddr, CtrlAddr: ask.CtrlAddr,
	})
	staleAgain := snap
	staleAgain.CollectedRevision = 2
	if err := ctrl.SubmitClusterSnapshot(staleAgain); err != nil {
		t.Fatalf("SubmitClusterSnapshot stale: %v", err)
	}
	expectNoNext(t, ctrl)

	observed := snap
	observed.CollectedRevision = 3
	observed.Volumes[0].Authority = AuthorityBasis{
		Assigned: true, ReplicaID: ask.ReplicaID, Epoch: 1, EndpointVersion: 1,
		DataAddr: ask.DataAddr, CtrlAddr: ask.CtrlAddr,
	}
	if err := ctrl.SubmitClusterSnapshot(observed); err != nil {
		t.Fatalf("SubmitClusterSnapshot observed: %v", err)
	}
	expectNoNext(t, ctrl)
}
