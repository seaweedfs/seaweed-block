package master

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
)

func TestPhase175MasterResolvesEveryFreshRestoreTargetAndCompletesGate(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	rec := snapshot.Record{SnapshotID: "snap-abc", State: snapshot.StateReady, SizeBytes: 1 << 20}
	if _, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "restored-a", SizeBytes: rec.SizeBytes, ReplicationFactor: 2, SourceSnapshotID: rec.SnapshotID}); err != nil {
		t.Fatal(err)
	}
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "restored-a", DesiredRF: 2, Candidates: []lifecycle.PlacementCandidate{
		{VolumeID: "restored-a", ServerID: "m01", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica},
		{VolumeID: "restored-a", ServerID: "m02", ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica},
	}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, item := range []struct {
		server, replica, host string
	}{{"m01", "r1", "10.0.0.1"}, {"m02", "r2", "10.0.0.2"}} {
		if err := h.obs.Store().Ingest(authority.Observation{ServerID: item.server, ObservedAt: now, Slots: []authority.SlotFact{{
			VolumeID: "restored-a", ReplicaID: item.replica, DataAddr: item.host + ":9201", CtrlAddr: item.host + ":9101", SnapshotRuntimeEndpoint: "https://" + item.host + ":24443", Reachable: true,
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := h.ResolveSnapshotRestoreTargets(context.Background(), "restored-a", rec)
	if err != nil {
		t.Fatal(err)
	}
	if plan.AlreadyComplete || len(plan.Targets) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	if err := h.obs.Store().Ingest(authority.Observation{ServerID: "m02", ObservedAt: now.Add(time.Second), Slots: []authority.SlotFact{{
		VolumeID: "restored-a", ReplicaID: "r2", DataAddr: "10.0.0.2:9201", CtrlAddr: "10.0.0.2:9101", SnapshotRuntimeEndpoint: "https://10.0.0.2:24444", Reachable: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.CompleteSnapshotRestore(context.Background(), "restored-a", rec.SnapshotID, plan.Targets); err == nil {
		t.Fatal("changed runtime placement opened restore authority gate")
	}
	if volume, _ := stores.Volumes.GetVolume("restored-a"); volume.RestoreState != lifecycle.VolumeRestorePending {
		t.Fatalf("changed target restore state=%q", volume.RestoreState)
	}
	if err := h.obs.Store().Ingest(authority.Observation{ServerID: "m02", ObservedAt: now.Add(2 * time.Second), Slots: []authority.SlotFact{{
		VolumeID: "restored-a", ReplicaID: "r2", DataAddr: "10.0.0.2:9201", CtrlAddr: "10.0.0.2:9101", SnapshotRuntimeEndpoint: "https://10.0.0.2:24443", Reachable: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.CompleteSnapshotRestore(context.Background(), "restored-a", rec.SnapshotID, plan.Targets); err != nil {
		t.Fatal(err)
	}
	plan, err = h.ResolveSnapshotRestoreTargets(context.Background(), "restored-a", rec)
	if err != nil || !plan.AlreadyComplete {
		t.Fatalf("completed plan=%+v error=%v", plan, err)
	}
}

func TestPhase175RestoreTargetFactsRejectWrongServerAndEndpointHost(t *testing.T) {
	base := authority.SlotFact{VolumeID: "restored-a", ReplicaID: "r1", ReportingServerID: "m01", DataAddr: "10.0.0.1:9201", SnapshotRuntimeEndpoint: "https://10.0.0.1:24443", Reachable: true}
	if target, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", true, base); !ok || target.ReplicaID != "r1" {
		t.Fatalf("valid target=%+v ok=%v", target, ok)
	}
	wrongServer := base
	wrongServer.ReportingServerID = "m02"
	if _, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", true, wrongServer); ok {
		t.Fatal("wrong reporting server accepted")
	}
	wrongEndpoint := base
	wrongEndpoint.SnapshotRuntimeEndpoint = "https://10.0.0.9:24443"
	if _, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", true, wrongEndpoint); ok {
		t.Fatal("endpoint on another data host accepted")
	}
}
