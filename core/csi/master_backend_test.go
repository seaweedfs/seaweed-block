package csi

import (
	"context"
	"errors"
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

type fakeEvidenceClient struct {
	resp *control.StatusResponse
	err  error
	reqs []string
}

func (f *fakeEvidenceClient) QueryVolumeStatus(_ context.Context, req *control.StatusRequest, _ ...grpc.CallOption) (*control.StatusResponse, error) {
	f.reqs = append(f.reqs, req.GetVolumeId())
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

type fakeObservationClient struct {
	event *control.ClusterEvent
	err   error
}

func (f *fakeObservationClient) ReportHeartbeat(context.Context, *control.HeartbeatReport, ...grpc.CallOption) (*control.HeartbeatAck, error) {
	return &control.HeartbeatAck{}, nil
}

func (f *fakeObservationClient) ReportClusterEvent(_ context.Context, event *control.ClusterEvent, _ ...grpc.CallOption) (*control.ClusterEventAck, error) {
	f.event = event
	if f.err != nil {
		return nil, f.err
	}
	return &control.ClusterEventAck{EventId: "master-1"}, nil
}

func TestControlEventReporter_ReportsClusterEvent(t *testing.T) {
	client := &fakeObservationClient{}
	reporter := NewControlEventReporter(client)

	err := reporter.ReportEvent(context.Background(), ClusterEvent{
		VolumeID:        "v1",
		ReplicaID:       "r2",
		NodeName:        "node-a",
		Type:            EventTypeCSIReattachObserved,
		Severity:        EventSeverityInfo,
		Reason:          EventTypeCSIReattachObserved,
		NewValue:        "127.0.0.1:3261",
		Epoch:           2,
		EndpointVersion: 1,
	})
	if err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}
	if client.event.GetEventType() != EventTypeCSIReattachObserved || client.event.GetReplicaId() != "r2" || client.event.GetEpoch() != 2 {
		t.Fatalf("event=%+v", client.event)
	}
}

func TestControlStatusLookup_MapsISCSIStatusFrontend(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:        "v1",
		ReplicaId:       "r1",
		Epoch:           7,
		EndpointVersion: 3,
		Assigned:        true,
		Frontends: []*control.FrontendTarget{{
			Protocol: "iscsi",
			Addr:     "127.0.0.1:3260",
			Iqn:      "iqn.2026-05.example:v1",
			Lun:      3,
		}},
	}}
	lookup := NewControlStatusLookupWithMultipath(client)

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Protocol != ProtocolISCSI || got.ISCSIAddr != "127.0.0.1:3260" || got.IQN != "iqn.2026-05.example:v1" || got.LUN != 3 {
		t.Fatalf("target=%+v", got)
	}
	if got.Epoch != 7 || got.EndpointVersion != 3 {
		t.Fatalf("target generation=(%d,%d), want (7,3)", got.Epoch, got.EndpointVersion)
	}
	if len(client.reqs) != 1 || client.reqs[0] != "v1" {
		t.Fatalf("status reqs=%v", client.reqs)
	}
}

func TestControlStatusLookup_MapsMultipleISCSIFrontendsToMultipathTarget(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:        "v1",
		ReplicaId:       "r2",
		Epoch:           2,
		EndpointVersion: 1,
		Assigned:        true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.example:v1", Lun: 1},
			{Protocol: "iscsi", Addr: "127.0.0.1:3261", Iqn: "iqn.2026-05.example:v1", Lun: 1},
		},
	}}
	lookup := NewControlStatusLookupWithMultipath(client)

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if !got.Multipath {
		t.Fatalf("multipath=false target=%+v", got)
	}
	if got.IQN != "iqn.2026-05.example:v1" || got.ISCSIAddr != "127.0.0.1:3260" {
		t.Fatalf("target=%+v", got)
	}
	if len(got.ISCSIAddrs) != 2 || got.ISCSIAddrs[0] != "127.0.0.1:3260" || got.ISCSIAddrs[1] != "127.0.0.1:3261" {
		t.Fatalf("ISCSIAddrs=%v", got.ISCSIAddrs)
	}
	ctx := publishContext(got)
	if ctx["stage2_multipath"] != "true" {
		t.Fatalf("publish_context missing multipath marker: %+v", ctx)
	}
	if ctx["iscsiAddrs"] != "127.0.0.1:3260,127.0.0.1:3261" {
		t.Fatalf("iscsiAddrs=%q", ctx["iscsiAddrs"])
	}
}

func TestControlStatusLookup_DoesNotMergeISCSIFrontendsWithDifferentIQN(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r2",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.example:v1-r1"},
			{Protocol: "iscsi", Addr: "127.0.0.1:3261", Iqn: "iqn.2026-05.example:v1-r2"},
		},
	}}
	lookup := NewControlStatusLookupWithMultipath(client)

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Multipath || len(got.ISCSIAddrs) != 0 {
		t.Fatalf("must not merge different IQNs into one multipath target: %+v", got)
	}
	if got.IQN != "iqn.2026-05.example:v1-r1" || got.ISCSIAddr != "127.0.0.1:3260" {
		t.Fatalf("target=%+v", got)
	}
}

func TestControlStatusLookup_DefaultDoesNotEnableMultipath(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r2",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.example:v1", Lun: 1},
			{Protocol: "iscsi", Addr: "127.0.0.1:3261", Iqn: "iqn.2026-05.example:v1", Lun: 1},
		},
	}}
	lookup := NewControlStatusLookup(client)

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Multipath || len(got.ISCSIAddrs) != 0 {
		t.Fatalf("default lookup must preserve single-path behavior: %+v", got)
	}
	if got.ISCSIAddr != "127.0.0.1:3260" || got.IQN != "iqn.2026-05.example:v1" {
		t.Fatalf("target=%+v", got)
	}
}

func TestNodeLoss_ControlStatusLookup_UsesFirstRoutableFrontendAsCurrentPrimary(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r2",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "10.0.0.2:3260", Iqn: "iqn.2026-05.example:v1", Lun: 1},
			{Protocol: "iscsi", Addr: "10.0.0.1:3260", Iqn: "iqn.2026-05.example:v1", Lun: 1},
			{Protocol: "iscsi", Addr: "10.0.0.3:3260", Iqn: "iqn.2026-05.example:v1", Lun: 1},
		},
	}}
	lookup := NewControlStatusLookupWithOptions(client, WithLoopbackPublishTargetsRejected())

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Multipath || got.ISCSIAddr != "10.0.0.2:3260" || got.ReplicaID != "r2" {
		t.Fatalf("target=%+v want promoted primary r2 first frontend", got)
	}
}

func TestNodeLoss_ControlStatusLookup_RejectsLoopbackPublishTargetsWhenEnabled(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r1",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.example:v1"},
			{Protocol: "iscsi", Addr: "10.0.0.12:3260", Iqn: "iqn.2026-05.example:v1"},
		},
	}}
	lookup := NewControlStatusLookupWithOptions(client, WithLoopbackPublishTargetsRejected())

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.ISCSIAddr != "10.0.0.12:3260" {
		t.Fatalf("iscsi addr=%q want non-loopback target", got.ISCSIAddr)
	}
}

func TestNodeLoss_ControlStatusLookup_AllowsHostnamePublishTargetsWhenLoopbackRejected(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r1",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "sw-blockvolume-r1.default.svc.cluster.local:3260", Iqn: "iqn.2026-05.example:v1"},
		},
	}}
	lookup := NewControlStatusLookupWithOptions(client, WithLoopbackPublishTargetsRejected())

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.ISCSIAddr != "sw-blockvolume-r1.default.svc.cluster.local:3260" {
		t.Fatalf("iscsi addr=%q want hostname target", got.ISCSIAddr)
	}
}

func TestNodeLoss_ControlStatusLookup_FailClosedWhenOnlyLoopbackTargets(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r1",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.example:v1"},
		},
	}}
	lookup := NewControlStatusLookupWithOptions(client, WithLoopbackPublishTargetsRejected())

	if _, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a"); !errors.Is(err, ErrPublishTargetNotFound) {
		t.Fatalf("err=%v want ErrPublishTargetNotFound", err)
	}
}

func TestControlStatusLookup_CarriesGenerationEvidenceForGateOnly(t *testing.T) {
	lookup := NewControlStatusLookup(&fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:        "v1",
		ReplicaId:       "r2",
		Epoch:           2,
		EndpointVersion: 4,
		Assigned:        true,
		Frontends: []*control.FrontendTarget{{
			Protocol: "iscsi",
			Addr:     "127.0.0.2:3260",
			Iqn:      "iqn.2026-05.example:v1-r2",
		}},
	}})

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.ReplicaID != "r2" || got.Epoch != 2 || got.EndpointVersion != 4 {
		t.Fatalf("target=%+v want r2@2/4", got)
	}
}

func TestControlStatusLookup_FailClosedWithoutAssignedFrontend(t *testing.T) {
	tests := []struct {
		name string
		resp *control.StatusResponse
	}{
		{name: "unassigned", resp: &control.StatusResponse{VolumeId: "v1", Assigned: false}},
		{name: "missing-frontends", resp: &control.StatusResponse{VolumeId: "v1", ReplicaId: "r1", Assigned: true}},
		{name: "malformed-iscsi", resp: &control.StatusResponse{
			VolumeId: "v1", ReplicaId: "r1", Assigned: true,
			Frontends: []*control.FrontendTarget{{Protocol: "iscsi", Addr: "127.0.0.1:3260"}},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lookup := NewControlStatusLookup(&fakeEvidenceClient{resp: tc.resp})
			if _, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a"); !errors.Is(err, ErrPublishTargetNotFound) {
				t.Fatalf("err=%v want ErrPublishTargetNotFound", err)
			}
		})
	}
}

func TestControlStatusLookup_MapsNVMeStatusFrontend(t *testing.T) {
	lookup := NewControlStatusLookup(&fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r1",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{{
			Protocol:  "nvme",
			Transport: "rdma",
			Addr:      "127.0.0.1:4420",
			Nqn:       "nqn.2026-05.io.seaweedfs:v1",
			Nsid:      1,
		}},
	}})
	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Protocol != ProtocolNVMe || got.FrontendTransport != FrontendTransportRDMA || got.NVMeAddr != "127.0.0.1:4420" || got.NQN == "" || got.NSID != 1 {
		t.Fatalf("target=%+v", got)
	}
}

func TestControlStatusLookup_MapsMultipleNVMeFrontendsToMultipathTarget(t *testing.T) {
	lookup := NewControlStatusLookupWithMultipath(&fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r1",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
			{Protocol: "nvme", Addr: "127.0.0.1:4421", Nqn: "nqn.2026-05.io.seaweedfs:v1", Nsid: 1},
		},
	}})
	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Protocol != ProtocolNVMe || !got.Multipath {
		t.Fatalf("target=%+v want nvme multipath", got)
	}
	if got.NVMeAddr != "127.0.0.1:4420" || got.NQN != "nqn.2026-05.io.seaweedfs:v1" || got.NSID != 1 {
		t.Fatalf("target=%+v", got)
	}
	if len(got.NVMeAddrs) != 2 || got.NVMeAddrs[0] != "127.0.0.1:4420" || got.NVMeAddrs[1] != "127.0.0.1:4421" {
		t.Fatalf("NVMeAddrs=%v", got.NVMeAddrs)
	}
	ctx := publishContext(got)
	if ctx["nvmeAddrs"] != "127.0.0.1:4420,127.0.0.1:4421" {
		t.Fatalf("nvmeAddrs=%q", ctx["nvmeAddrs"])
	}
	if ctx["nvmeAddr"] != "127.0.0.1:4420" || ctx["nqn"] != "nqn.2026-05.io.seaweedfs:v1" {
		t.Fatalf("publish_context=%+v", ctx)
	}
}

func TestControlStatusLookup_DoesNotMergeNVMeFrontendsWithDifferentNQN(t *testing.T) {
	lookup := NewControlStatusLookupWithMultipath(&fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r1",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{
			{Protocol: "nvme", Addr: "127.0.0.1:4420", Nqn: "nqn.2026-05.io.seaweedfs:v1-r1", Nsid: 1},
			{Protocol: "nvme", Addr: "127.0.0.1:4421", Nqn: "nqn.2026-05.io.seaweedfs:v1-r2", Nsid: 1},
		},
	}})
	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Multipath || len(got.NVMeAddrs) != 0 {
		t.Fatalf("must not merge different NQNs into one multipath target: %+v", got)
	}
	if got.NQN != "nqn.2026-05.io.seaweedfs:v1-r1" || got.NVMeAddr != "127.0.0.1:4420" {
		t.Fatalf("target=%+v", got)
	}
}

type fakeLifecycleClient struct {
	control.LifecycleServiceClient
	createReq *control.CreateVolumeRequest
	deleteReq *control.DeleteVolumeRequest
}

func (f *fakeLifecycleClient) CreateVolume(_ context.Context, req *control.CreateVolumeRequest, _ ...grpc.CallOption) (*control.CreateVolumeResponse, error) {
	f.createReq = req
	return &control.CreateVolumeResponse{
		VolumeId:          req.GetVolumeId(),
		SizeBytes:         req.GetSizeBytes(),
		ReplicationFactor: req.GetReplicationFactor(),
		Protocol:          req.GetProtocol(),
		FrontendTransport: req.GetFrontendTransport(),
		PvcName:           req.GetPvcName(),
		PvcNamespace:      req.GetPvcNamespace(),
		PvcUid:            req.GetPvcUid(),
		PvName:            req.GetPvName(),
	}, nil
}

func (f *fakeLifecycleClient) DeleteVolume(_ context.Context, req *control.DeleteVolumeRequest, _ ...grpc.CallOption) (*control.DeleteVolumeResponse, error) {
	f.deleteReq = req
	return &control.DeleteVolumeResponse{}, nil
}

func TestG15c_ControlLifecycleProvisioner_CreateVolumeRoundTrip(t *testing.T) {
	client := &fakeLifecycleClient{}
	prov := NewControlLifecycleProvisioner(client)
	got, err := prov.CreateVolume(context.Background(), VolumeSpec{
		VolumeID:          "pvc-a",
		SizeBytes:         1 << 30,
		ReplicationFactor: 2,
		Protocol:          ProtocolNVMe,
		FrontendTransport: FrontendTransportRDMA,
		PVCName:           "demo-pvc",
		PVCNamespace:      "demo-ns",
		PVCUID:            "uid-123",
		PVName:            "pvc-a",
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if client.createReq.GetVolumeId() != "pvc-a" || client.createReq.GetSizeBytes() != 1<<30 || client.createReq.GetReplicationFactor() != 2 {
		t.Fatalf("request=%+v", client.createReq)
	}
	if client.createReq.GetProtocol() != "nvme" {
		t.Fatalf("protocol=%q want nvme", client.createReq.GetProtocol())
	}
	if client.createReq.GetFrontendTransport() != "rdma" || got.FrontendTransport != FrontendTransportRDMA {
		t.Fatalf("transport request=%q response=%q want rdma", client.createReq.GetFrontendTransport(), got.FrontendTransport)
	}
	if client.createReq.GetPvcName() != "demo-pvc" || client.createReq.GetPvcNamespace() != "demo-ns" || client.createReq.GetPvcUid() != "uid-123" || client.createReq.GetPvName() != "pvc-a" {
		t.Fatalf("kubernetes metadata request=%+v", client.createReq)
	}
	if got.VolumeID != "pvc-a" || got.SizeBytes != 1<<30 || got.ReplicationFactor != 2 {
		t.Fatalf("spec=%+v", got)
	}
	if got.Protocol != ProtocolNVMe {
		t.Fatalf("protocol=%q want nvme", got.Protocol)
	}
	if got.PVCName != "demo-pvc" || got.PVCNamespace != "demo-ns" || got.PVCUID != "uid-123" || got.PVName != "pvc-a" {
		t.Fatalf("kubernetes metadata spec=%+v", got)
	}
}

func TestG15c_ControlLifecycleProvisioner_DeleteVolumeRoundTrip(t *testing.T) {
	client := &fakeLifecycleClient{}
	prov := NewControlLifecycleProvisioner(client)
	if err := prov.DeleteVolume(context.Background(), "pvc-a"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if client.deleteReq.GetVolumeId() != "pvc-a" {
		t.Fatalf("delete request=%+v", client.deleteReq)
	}
}
