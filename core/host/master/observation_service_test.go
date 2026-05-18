package master

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClusterEvidenceService_GetClusterStatusSharesObservationSnapshot(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	lineBefore := waitAuthorityLine(t, h.Publisher(), "pvc-a")

	resp, err := newServices(h).GetClusterStatus(context.Background(), &control.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	lineAfter, ok := h.Publisher().VolumeAuthorityLine("pvc-a")
	if !ok || lineAfter != lineBefore {
		t.Fatalf("read-only service mutated authority before=%+v after=%+v ok=%t", lineBefore, lineAfter, ok)
	}

	if resp.GetSchemaVersion() == "" || resp.GetStatus() != "ok" {
		t.Fatalf("response schema/status=%q/%q", resp.GetSchemaVersion(), resp.GetStatus())
	}
	if len(resp.GetNodes()) != 3 || len(resp.GetVolumes()) != 1 {
		t.Fatalf("nodes=%d volumes=%d", len(resp.GetNodes()), len(resp.GetVolumes()))
	}
	volume := resp.GetVolumes()[0]
	if volume.GetVolumeId() != "pvc-a" || volume.GetPvcName() != "demo-pvc" {
		t.Fatalf("volume=%+v", volume)
	}
	if volume.GetPrimaryReplica() != lineBefore.ReplicaID || volume.GetEpoch() != lineBefore.Epoch || volume.GetEndpointVersion() != lineBefore.EndpointVersion {
		t.Fatalf("volume authority fields=%+v line=%+v", volume, lineBefore)
	}
	if volume.GetPublishTarget() == "" {
		t.Fatalf("missing publish target: %+v", volume)
	}
}

func TestClusterEvidenceService_GRPCRegistered(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := control.NewClusterEvidenceServiceClient(conn).GetVolumeStatus(ctx, &control.GetVolumeStatusRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("GetVolumeStatus over grpc: %v", err)
	}
	if resp.GetVolumeId() != "pvc-a" || resp.GetPrimaryReplica() == "" {
		t.Fatalf("response=%+v", resp)
	}
}

func TestClusterEvidenceService_ListVolumesAndGetVolumeStatus(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	})
	defer closeTestMaster(t, h)
	seedObservationSnapshotVolume(t, h)
	seedRF3PlacementForServers(t, h, "pvc-a", "m01", "m02", "tp01")
	ingestObservationSnapshotRF3(t, h, true, true, false)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}

	svc := newServices(h)
	list, err := svc.ListVolumes(context.Background(), &control.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes: %v", err)
	}
	if len(list.GetVolumes()) != 1 {
		t.Fatalf("volumes=%d want 1", len(list.GetVolumes()))
	}
	if list.GetVolumes()[0].GetStatus() != "degraded" || list.GetVolumes()[0].GetReason() != "observed_replicas_below_desired" {
		t.Fatalf("volume=%+v", list.GetVolumes()[0])
	}

	volume, err := svc.GetVolumeStatus(context.Background(), &control.GetVolumeStatusRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("GetVolumeStatus: %v", err)
	}
	if volume.GetObservedReplicas() != 2 || volume.GetDesiredReplicas() != 3 {
		t.Fatalf("counts observed=%d desired=%d", volume.GetObservedReplicas(), volume.GetDesiredReplicas())
	}
}

func TestClusterEvidenceService_GetVolumeStatusNotFound(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)

	_, err := newServices(h).GetVolumeStatus(context.Background(), &control.GetVolumeStatusRequest{VolumeId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err=%v code=%v want NotFound", err, status.Code(err))
	}
}

func TestClusterEvidenceService_GetVolumeTimelineEmptyButVersioned(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)

	resp, err := newServices(h).GetVolumeTimeline(context.Background(), &control.GetVolumeTimelineRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("GetVolumeTimeline: %v", err)
	}
	if resp.GetSchemaVersion() == "" || resp.GetCapturedAt().AsTime().After(time.Now().Add(time.Second)) {
		t.Fatalf("timeline metadata=%+v", resp)
	}
	if len(resp.GetEvents()) != 0 {
		t.Fatalf("events=%d want 0 before D5 event ring", len(resp.GetEvents()))
	}
}

func TestObservationService_ReportClusterEventAppearsInVolumeTimeline(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)

	svc := newServices(h)
	ack, err := svc.ReportClusterEvent(context.Background(), &control.ClusterEvent{
		EventId:         "client-supplied",
		EventTime:       timestamppb.New(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
		VolumeId:        "pvc-a",
		ReplicaId:       "r2",
		NodeName:        "m02",
		EventType:       "csi_reattach_observed",
		Severity:        "warning",
		Message:         "authority_published spoof",
		ReasonCode:      "candidate_covers_required_frontier",
		OldValue:        "r1",
		CorrelationId:   "client-correlation",
		EvidenceRef:     "authority-log",
		Epoch:           2,
		EndpointVersion: 1,
	})
	if err != nil {
		t.Fatalf("ReportClusterEvent: %v", err)
	}
	if ack.GetEventId() == "" || ack.GetAcceptedAt() == nil {
		t.Fatalf("ack=%+v", ack)
	}

	timeline, err := svc.GetVolumeTimeline(context.Background(), &control.GetVolumeTimelineRequest{VolumeId: "pvc-a"})
	if err != nil {
		t.Fatalf("GetVolumeTimeline: %v", err)
	}
	if len(timeline.GetEvents()) != 1 {
		t.Fatalf("events=%d want 1", len(timeline.GetEvents()))
	}
	event := timeline.GetEvents()[0]
	if event.GetEventId() != ack.GetEventId() || event.GetEventType() != "csi_reattach_observed" || event.GetReplicaId() != "r2" {
		t.Fatalf("event=%+v ack=%+v", event, ack)
	}
	if event.GetEventId() == "client-supplied" || event.GetEventTime().AsTime().Year() == 2000 {
		t.Fatalf("master did not mint event identity/time: %+v", event)
	}
	if event.GetSeverity() != "warning" || event.GetReasonCode() != "csi_reattach_observed" || event.GetMessage() != "CSI staged volume on node" {
		t.Fatalf("external event fields were not sanitized: %+v", event)
	}
	if event.GetOldValue() != "" || event.GetCorrelationId() != "" || event.GetEvidenceRef() != "csi-node" {
		t.Fatalf("external authority-looking fields leaked through: %+v", event)
	}
}

func TestObservationService_ReportClusterEventRejectsMissingType(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)

	_, err := newServices(h).ReportClusterEvent(context.Background(), &control.ClusterEvent{VolumeId: "pvc-a"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err=%v code=%v want InvalidArgument", err, status.Code(err))
	}
}

func TestObservationService_ReportClusterEventRejectsAuthorityOwnedType(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)

	_, err := newServices(h).ReportClusterEvent(context.Background(), &control.ClusterEvent{
		VolumeId:  "pvc-a",
		NodeName:  "m02",
		EventType: "authority_published",
		Severity:  "info",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err=%v code=%v want InvalidArgument", err, status.Code(err))
	}
}

func TestObservationService_ReportClusterEventRejectsCSIEventWithoutVolumeOrNode(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)

	_, err := newServices(h).ReportClusterEvent(context.Background(), &control.ClusterEvent{
		NodeName:  "m02",
		EventType: "csi_reattach_observed",
		Severity:  "info",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing volume err=%v code=%v want InvalidArgument", err, status.Code(err))
	}
	_, err = newServices(h).ReportClusterEvent(context.Background(), &control.ClusterEvent{
		VolumeId:  "pvc-a",
		EventType: "csi_reattach_observed",
		Severity:  "info",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing node err=%v code=%v want InvalidArgument", err, status.Code(err))
	}
}
