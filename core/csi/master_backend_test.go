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

func TestControlStatusLookup_MapsISCSIStatusFrontend(t *testing.T) {
	client := &fakeEvidenceClient{resp: &control.StatusResponse{
		VolumeId:  "v1",
		ReplicaId: "r1",
		Assigned:  true,
		Frontends: []*control.FrontendTarget{{
			Protocol: "iscsi",
			Addr:     "127.0.0.1:3260",
			Iqn:      "iqn.2026-05.example:v1",
			Lun:      3,
		}},
	}}
	lookup := NewControlStatusLookup(client)

	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Protocol != ProtocolISCSI || got.ISCSIAddr != "127.0.0.1:3260" || got.IQN != "iqn.2026-05.example:v1" || got.LUN != 3 {
		t.Fatalf("target=%+v", got)
	}
	if len(client.reqs) != 1 || client.reqs[0] != "v1" {
		t.Fatalf("status reqs=%v", client.reqs)
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
			Protocol: "nvme",
			Addr:     "127.0.0.1:4420",
			Nqn:      "nqn.2026-05.io.seaweedfs:v1",
			Nsid:     1,
		}},
	}})
	got, err := lookup.LookupPublishTarget(context.Background(), "v1", "node-a")
	if err != nil {
		t.Fatalf("LookupPublishTarget: %v", err)
	}
	if got.Protocol != ProtocolNVMe || got.NVMeAddr != "127.0.0.1:4420" || got.NQN == "" || got.NSID != 1 {
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
		PvcName:           req.GetPvcName(),
		PvcNamespace:      req.GetPvcNamespace(),
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
		PVCName:           "demo-pvc",
		PVCNamespace:      "demo-ns",
		PVName:            "pvc-a",
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if client.createReq.GetVolumeId() != "pvc-a" || client.createReq.GetSizeBytes() != 1<<30 || client.createReq.GetReplicationFactor() != 2 {
		t.Fatalf("request=%+v", client.createReq)
	}
	if client.createReq.GetPvcName() != "demo-pvc" || client.createReq.GetPvcNamespace() != "demo-ns" || client.createReq.GetPvName() != "pvc-a" {
		t.Fatalf("kubernetes metadata request=%+v", client.createReq)
	}
	if got.VolumeID != "pvc-a" || got.SizeBytes != 1<<30 || got.ReplicationFactor != 2 {
		t.Fatalf("spec=%+v", got)
	}
	if got.PVCName != "demo-pvc" || got.PVCNamespace != "demo-ns" || got.PVName != "pvc-a" {
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
