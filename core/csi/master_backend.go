package csi

import (
	"context"
	"fmt"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// ControlStatusLookup reads master EvidenceService status and
// converts observed frontend facts into CSI publish targets. It is
// intentionally read-only: it does not call lifecycle, placement, or
// authority mutation APIs.
type ControlStatusLookup struct {
	client control.EvidenceServiceClient
}

func NewControlStatusLookup(client control.EvidenceServiceClient) *ControlStatusLookup {
	return &ControlStatusLookup{client: client}
}

type ControlLifecycleProvisioner struct {
	client control.LifecycleServiceClient
}

func NewControlLifecycleProvisioner(client control.LifecycleServiceClient) *ControlLifecycleProvisioner {
	return &ControlLifecycleProvisioner{client: client}
}

func (p *ControlLifecycleProvisioner) CreateVolume(ctx context.Context, spec VolumeSpec) (VolumeSpec, error) {
	if p == nil || p.client == nil {
		return VolumeSpec{}, fmt.Errorf("csi: lifecycle provisioner not configured")
	}
	resp, err := p.client.CreateVolume(ctx, &control.CreateVolumeRequest{
		VolumeId:          spec.VolumeID,
		SizeBytes:         spec.SizeBytes,
		ReplicationFactor: int32(spec.ReplicationFactor),
		PvcName:           spec.PVCName,
		PvcNamespace:      spec.PVCNamespace,
		PvcUid:            spec.PVCUID,
		PvName:            spec.PVName,
	})
	if err != nil {
		return VolumeSpec{}, err
	}
	return VolumeSpec{
		VolumeID:          resp.GetVolumeId(),
		SizeBytes:         resp.GetSizeBytes(),
		ReplicationFactor: int(resp.GetReplicationFactor()),
		PVCName:           resp.GetPvcName(),
		PVCNamespace:      resp.GetPvcNamespace(),
		PVCUID:            resp.GetPvcUid(),
		PVName:            resp.GetPvName(),
	}, nil
}

func (p *ControlLifecycleProvisioner) DeleteVolume(ctx context.Context, volumeID string) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("csi: lifecycle provisioner not configured")
	}
	_, err := p.client.DeleteVolume(ctx, &control.DeleteVolumeRequest{VolumeId: volumeID})
	return err
}

func (l *ControlStatusLookup) LookupPublishTarget(ctx context.Context, volumeID, nodeID string) (PublishTarget, error) {
	if l == nil || l.client == nil {
		return PublishTarget{}, fmt.Errorf("csi: control status lookup not configured")
	}
	resp, err := l.client.QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: volumeID})
	if err != nil {
		return PublishTarget{}, err
	}
	if !resp.GetAssigned() || resp.GetReplicaId() == "" {
		return PublishTarget{}, ErrPublishTargetNotFound
	}
	if t, ok := publishTargetFromStatus(resp); ok {
		return t, nil
	}
	return PublishTarget{}, ErrPublishTargetNotFound
}

func publishTargetFromStatus(resp *control.StatusResponse) (PublishTarget, bool) {
	if resp == nil {
		return PublishTarget{}, false
	}
	base := PublishTarget{
		VolumeID:  resp.GetVolumeId(),
		ReplicaID: resp.GetReplicaId(),
	}
	for _, ft := range resp.GetFrontends() {
		if ft == nil {
			continue
		}
		switch ft.GetProtocol() {
		case string(ProtocolISCSI):
			if ft.GetAddr() == "" || ft.GetIqn() == "" {
				continue
			}
			out := base
			out.Protocol = ProtocolISCSI
			out.ISCSIAddr = ft.GetAddr()
			out.IQN = ft.GetIqn()
			out.LUN = ft.GetLun()
			return out, true
		case string(ProtocolNVMe):
			if ft.GetAddr() == "" || ft.GetNqn() == "" {
				continue
			}
			out := base
			out.Protocol = ProtocolNVMe
			out.NVMeAddr = ft.GetAddr()
			out.NQN = ft.GetNqn()
			out.NSID = ft.GetNsid()
			return out, true
		}
	}
	return PublishTarget{}, false
}
