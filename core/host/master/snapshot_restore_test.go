package master

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			VolumeID: "restored-a", ReplicaID: item.replica, DataAddr: item.host + ":9201", CtrlAddr: item.host + ":9101", SnapshotRuntimeEndpoint: "https://" + item.host + ":24443", SnapshotRestore: testSnapshotRestoreEvidence(rec.SnapshotID, item.replica), Reachable: true,
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
		VolumeID: "restored-a", ReplicaID: "r2", DataAddr: "10.0.0.2:9201", CtrlAddr: "10.0.0.2:9101", SnapshotRuntimeEndpoint: "https://10.0.0.2:24444", SnapshotRestore: testSnapshotRestoreEvidence(rec.SnapshotID, "r2"), Reachable: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.CompleteSnapshotRestore(context.Background(), "restored-a", rec.SnapshotID, plan.Targets); err == nil {
		t.Fatal("changed runtime placement opened restore authority gate")
	}
	if volume, _ := stores.Volumes.GetVolume("restored-a"); volume.RestoreState != lifecycle.VolumeRestorePending {
		t.Fatalf("changed target restore state=%q", volume.RestoreState)
	}
	replacedStore := testSnapshotRestoreEvidence(rec.SnapshotID, "r2")
	replacedStore.StorageID = "r2-replacement-store"
	if err := h.obs.Store().Ingest(authority.Observation{ServerID: "m02", ObservedAt: now.Add(2 * time.Second), Slots: []authority.SlotFact{{
		VolumeID: "restored-a", ReplicaID: "r2", DataAddr: "10.0.0.2:9201", CtrlAddr: "10.0.0.2:9101", SnapshotRuntimeEndpoint: "https://10.0.0.2:24443", SnapshotRestore: replacedStore, Reachable: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.CompleteSnapshotRestore(context.Background(), "restored-a", rec.SnapshotID, plan.Targets); err == nil {
		t.Fatal("replacement durable store opened restore authority gate")
	}
	if err := h.obs.Store().Ingest(authority.Observation{ServerID: "m02", ObservedAt: now.Add(3 * time.Second), Slots: []authority.SlotFact{{
		VolumeID: "restored-a", ReplicaID: "r2", DataAddr: "10.0.0.2:9201", CtrlAddr: "10.0.0.2:9101", SnapshotRuntimeEndpoint: "https://10.0.0.2:24443", SnapshotRestore: testSnapshotRestoreEvidence(rec.SnapshotID, "r2"), Reachable: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.CompleteSnapshotRestore(context.Background(), "restored-a", rec.SnapshotID, plan.Targets); err == nil {
		t.Fatal("pending restore observation opened restore authority gate")
	}
	for _, item := range []struct {
		server, replica, host string
	}{{"m01", "r1", "10.0.0.1"}, {"m02", "r2", "10.0.0.2"}} {
		evidence := testSnapshotRestoreEvidence(rec.SnapshotID, item.replica)
		evidence.State = snapshot.RestoreStateActivated
		if err := h.obs.Store().Ingest(authority.Observation{ServerID: item.server, ObservedAt: now.Add(4 * time.Second), Slots: []authority.SlotFact{{
			VolumeID: "restored-a", ReplicaID: item.replica, DataAddr: item.host + ":9201", CtrlAddr: item.host + ":9101", SnapshotRuntimeEndpoint: "https://" + item.host + ":24443", SnapshotRestore: evidence, Reachable: true,
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.CompleteSnapshotRestore(context.Background(), "restored-a", rec.SnapshotID, plan.Targets); err != nil {
		t.Fatal(err)
	}
	plan, err = h.ResolveSnapshotRestoreTargets(context.Background(), "restored-a", rec)
	if err != nil || !plan.AlreadyComplete {
		t.Fatalf("completed plan=%+v error=%v", plan, err)
	}
	if err := h.CompleteSnapshotRestore(context.Background(), "restored-a", rec.SnapshotID, plan.Targets); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}
}

func TestPhase175RestoreTargetFactsRejectWrongServerAndEndpointHost(t *testing.T) {
	base := authority.SlotFact{VolumeID: "restored-a", ReplicaID: "r1", ReportingServerID: "m01", DataAddr: "10.0.0.1:9201", SnapshotRuntimeEndpoint: "https://10.0.0.1:24443", SnapshotRestore: testSnapshotRestoreEvidence("snap-abc", "r1"), Reachable: true}
	if target, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", "snap-abc", true, base); !ok || target.ReplicaID != "r1" || target.TargetStorageID != "r1-store" {
		t.Fatalf("valid target=%+v ok=%v", target, ok)
	}
	wrongServer := base
	wrongServer.ReportingServerID = "m02"
	if _, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", "snap-abc", true, wrongServer); ok {
		t.Fatal("wrong reporting server accepted")
	}
	wrongEndpoint := base
	wrongEndpoint.SnapshotRuntimeEndpoint = "https://10.0.0.9:24443"
	if _, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", "snap-abc", true, wrongEndpoint); ok {
		t.Fatal("endpoint on another data host accepted")
	}
	wrongSnapshot := base
	wrongSnapshot.SnapshotRestore.SnapshotID = "snap-other"
	if _, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", "snap-abc", true, wrongSnapshot); ok {
		t.Fatal("restore evidence for another snapshot accepted")
	}
	missingStore := base
	missingStore.SnapshotRestore.StorageID = ""
	if _, ok := snapshotRestoreTargetFromFacts("restored-a", "m01", "r1", "snap-abc", true, missingStore); ok {
		t.Fatal("restore evidence without durable store identity accepted")
	}
}

func TestPhase175RestoreIntegrityFaultRemainsAnObservedTargetState(t *testing.T) {
	if !validSnapshotRestoreObservationState(snapshot.RestoreStateIntegrityFault) {
		t.Fatal("integrity fault was dropped from restore observations")
	}
}

func TestPhase175MasterRequestsDurableRestoreAbortAndSuppressesWorkloads(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	source := createLifecycleSnapshot(t, h, "abort-source")
	h.snapshotAPIToken = "api-token"
	svc := newServices(h)
	const targetVolumeID = "restored-a"
	for i, serverID := range []string{"m01", "m02", "m03"} {
		if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{
			ServerID: serverID, Addr: fmt.Sprintf("10.0.0.%d:19000", i+1), Labels: map[string]string{lifecycle.KubernetesNodeNameLabel: "node-" + serverID},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId: targetVolumeID, SizeBytes: source.SizeBytes, ReplicationFactor: 3, SourceSnapshotId: source.SnapshotID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID: targetVolumeID, DesiredRF: 3, RestoreSnapshotID: source.SnapshotID,
		Candidates: []lifecycle.PlacementCandidate{
			{VolumeID: targetVolumeID, ServerID: "m03", ReplicaID: "r3", Source: lifecycle.PlacementSourceExistingReplica},
			{VolumeID: targetVolumeID, ServerID: "m01", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica},
			{VolumeID: targetVolumeID, ServerID: "m02", ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rec, err := h.RequestSnapshotRestoreAbort(context.Background(), targetVolumeID, source.SnapshotID)
	if err != nil || rec.RestoreState != lifecycle.VolumeRestoreAbortRequested || rec.RestoreAbort == nil || !strings.HasPrefix(rec.RestoreAbort.OperationID, "abort-") {
		t.Fatalf("abort record=%+v error=%v", rec, err)
	}
	if len(rec.RestoreAbort.Replicas) != 3 || rec.RestoreAbort.Replicas[0].ReplicaID != "r1" || rec.RestoreAbort.Replicas[2].ReplicaID != "r3" {
		t.Fatalf("abort replicas=%+v", rec.RestoreAbort.Replicas)
	}
	retry, err := h.RequestSnapshotRestoreAbort(context.Background(), targetVolumeID, source.SnapshotID)
	if err != nil || retry.RestoreAbort.OperationID != rec.RestoreAbort.OperationID {
		t.Fatalf("retry record=%+v error=%v", retry, err)
	}
	if err := h.CompleteSnapshotRestore(context.Background(), targetVolumeID, source.SnapshotID, nil); err == nil {
		t.Fatal("restore completion won after abort request")
	}
	workloads, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{})
	if err != nil || len(workloads.Plans) != 0 || workloads.SkippedRestoreAbort != 1 {
		t.Fatalf("workload result=%+v error=%v", workloads, err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: targetVolumeID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete aborting restore error=%v", err)
	}
	if _, ok := h.Lifecycle().Placements.GetPlacement(targetVolumeID); !ok {
		t.Fatal("delete hold removed abort placement")
	}
	if _, err := svc.DeleteSnapshot(snapshotIncomingContext("api-token"), &control.DeleteSnapshotRequest{SnapshotId: source.SnapshotID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete snapshot during abort error=%v", err)
	}
	wire, err := svc.AbortSnapshotRestore(snapshotIncomingContext("api-token"), &control.AbortSnapshotRestoreRequest{
		SnapshotId: source.SnapshotID, TargetVolumeId: targetVolumeID,
	})
	if err != nil || wire.GetOperationId() != rec.RestoreAbort.OperationID || wire.GetState() != lifecycle.VolumeRestoreAbortRequested || len(wire.GetTargets()) != 3 {
		t.Fatalf("abort RPC response=%+v error=%v", wire, err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	_, running, err := h.Lifecycle().Volumes.BeginRestoreDiscardAttempt(targetVolumeID, rec.RestoreAbort.OperationID, "m01", "r1", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Lifecycle().Volumes.RecordRestoreDiscardFailure(targetVolumeID, rec.RestoreAbort.OperationID, "m01", "r1", running.Attempt, 1, "permission denied", "job/a/pod/r1", now, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: targetVolumeID}); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "terminal failure") {
		t.Fatalf("terminal delete error=%v", err)
	}
	wire, err = svc.AbortSnapshotRestore(snapshotIncomingContext("api-token"), &control.AbortSnapshotRestoreRequest{
		SnapshotId: source.SnapshotID, TargetVolumeId: targetVolumeID,
	})
	if err != nil || wire.GetTargets()[0].GetState() != lifecycle.RestoreDiscardTerminalFailure || wire.GetTargets()[0].GetAttempt() != 1 || wire.GetTargets()[0].GetFailureReason() != "permission denied" || wire.GetTargets()[0].GetEvidenceRef() == "" {
		t.Fatalf("terminal abort RPC response=%+v error=%v", wire, err)
	}
}

func TestPhase175DeleteVolumeRequestsRestoreAbortBeforeReturningHold(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	source := createLifecycleSnapshot(t, h, "delete-abort-source")
	const targetVolumeID = "restored-delete"
	for i, serverID := range []string{"m01", "m02", "m03"} {
		if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{
			ServerID: serverID, Addr: fmt.Sprintf("10.0.1.%d:19000", i+1), Labels: map[string]string{lifecycle.KubernetesNodeNameLabel: "node-" + serverID},
		}); err != nil {
			t.Fatal(err)
		}
	}
	svc := newServices(h)
	if _, err := svc.CreateVolume(context.Background(), &control.CreateVolumeRequest{
		VolumeId: targetVolumeID, SizeBytes: source.SizeBytes, ReplicationFactor: 3, SourceSnapshotId: source.SnapshotID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID: targetVolumeID, DesiredRF: 3, RestoreSnapshotID: source.SnapshotID,
		Candidates: []lifecycle.PlacementCandidate{
			{VolumeID: targetVolumeID, ServerID: "m01", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica},
			{VolumeID: targetVolumeID, ServerID: "m02", ReplicaID: "r2", Source: lifecycle.PlacementSourceExistingReplica},
			{VolumeID: targetVolumeID, ServerID: "m03", ReplicaID: "r3", Source: lifecycle.PlacementSourceExistingReplica},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DeleteVolume(context.Background(), &control.DeleteVolumeRequest{VolumeId: targetVolumeID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete code=%s error=%v", status.Code(err), err)
	}
	record, ok := h.Lifecycle().Volumes.GetVolume(targetVolumeID)
	if !ok || record.RestoreState != lifecycle.VolumeRestoreAbortRequested || record.RestoreAbort == nil {
		t.Fatalf("record=%+v ok=%t", record, ok)
	}
}

func testSnapshotRestoreEvidence(snapshotID, replicaID string) authority.SnapshotRestoreEvidenceFact {
	return authority.SnapshotRestoreEvidenceFact{
		SnapshotID: snapshotID,
		State:      snapshot.RestoreStatePending,
		StorageID:  replicaID + "-store",
		NumBlocks:  256,
		BlockSize:  4096,
	}
}
