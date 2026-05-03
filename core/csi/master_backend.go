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
