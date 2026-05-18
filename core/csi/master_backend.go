package csi

import (
	"context"
	"fmt"
	"net"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// ControlStatusLookup reads master EvidenceService status and
// converts observed frontend facts into CSI publish targets. It is
// intentionally read-only: it does not call lifecycle, placement, or
// authority mutation APIs.
type ControlStatusLookup struct {
	client          control.EvidenceServiceClient
	enableMultipath bool
	rejectLoopback  bool
}

type ControlStatusLookupOption func(*ControlStatusLookup)

func NewControlStatusLookup(client control.EvidenceServiceClient) *ControlStatusLookup {
	return &ControlStatusLookup{client: client}
}

func NewControlStatusLookupWithMultipath(client control.EvidenceServiceClient) *ControlStatusLookup {
	return &ControlStatusLookup{client: client, enableMultipath: true}
}

func NewControlStatusLookupWithOptions(client control.EvidenceServiceClient, opts ...ControlStatusLookupOption) *ControlStatusLookup {
	out := &ControlStatusLookup{client: client}
	for _, opt := range opts {
		if opt != nil {
			opt(out)
		}
	}
	return out
}

func WithMultipathPublishTargets() ControlStatusLookupOption {
	return func(l *ControlStatusLookup) { l.enableMultipath = true }
}

func WithLoopbackPublishTargetsRejected() ControlStatusLookupOption {
	return func(l *ControlStatusLookup) { l.rejectLoopback = true }
}

type ControlLifecycleProvisioner struct {
	client control.LifecycleServiceClient
}

type ControlEventReporter struct {
	client control.ObservationServiceClient
}

func NewControlLifecycleProvisioner(client control.LifecycleServiceClient) *ControlLifecycleProvisioner {
	return &ControlLifecycleProvisioner{client: client}
}

func NewControlEventReporter(client control.ObservationServiceClient) *ControlEventReporter {
	return &ControlEventReporter{client: client}
}

func (r *ControlEventReporter) ReportEvent(ctx context.Context, event ClusterEvent) error {
	if r == nil || r.client == nil {
		return fmt.Errorf("csi: event reporter not configured")
	}
	_, err := r.client.ReportClusterEvent(ctx, &control.ClusterEvent{
		VolumeId:        event.VolumeID,
		ReplicaId:       event.ReplicaID,
		NodeName:        event.NodeName,
		EventType:       event.Type,
		Severity:        event.Severity,
		Message:         event.Message,
		ReasonCode:      event.Reason,
		OldValue:        event.OldValue,
		NewValue:        event.NewValue,
		Epoch:           event.Epoch,
		EndpointVersion: event.EndpointVersion,
		CorrelationId:   event.CorrelationID,
		EvidenceRef:     event.EvidenceRef,
	})
	return err
}

func (p *ControlLifecycleProvisioner) CreateVolume(ctx context.Context, spec VolumeSpec) (VolumeSpec, error) {
	if p == nil || p.client == nil {
		return VolumeSpec{}, fmt.Errorf("csi: lifecycle provisioner not configured")
	}
	resp, err := p.client.CreateVolume(ctx, &control.CreateVolumeRequest{
		VolumeId:          spec.VolumeID,
		SizeBytes:         spec.SizeBytes,
		ReplicationFactor: int32(spec.ReplicationFactor),
		Protocol:          string(spec.Protocol),
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
		Protocol:          normalizeProtocol(Protocol(resp.GetProtocol())),
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
	if t, ok := publishTargetFromStatus(resp, l.enableMultipath, l.rejectLoopback); ok {
		return t, nil
	}
	return PublishTarget{}, ErrPublishTargetNotFound
}

func publishTargetFromStatus(resp *control.StatusResponse, enableMultipath bool, rejectLoopback bool) (PublishTarget, bool) {
	if resp == nil {
		return PublishTarget{}, false
	}
	base := PublishTarget{
		VolumeID:        resp.GetVolumeId(),
		ReplicaID:       resp.GetReplicaId(),
		Epoch:           resp.GetEpoch(),
		EndpointVersion: resp.GetEndpointVersion(),
	}
	if enableMultipath {
		iscsiByIQN := map[string][]*control.FrontendTarget{}
		iscsiOrder := []string{}
		for _, ft := range resp.GetFrontends() {
			if ft == nil {
				continue
			}
			if ft.GetProtocol() != string(ProtocolISCSI) || ft.GetAddr() == "" || ft.GetIqn() == "" || rejectFrontendAddr(ft.GetAddr(), rejectLoopback) {
				continue
			}
			if _, ok := iscsiByIQN[ft.GetIqn()]; !ok {
				iscsiOrder = append(iscsiOrder, ft.GetIqn())
			}
			iscsiByIQN[ft.GetIqn()] = append(iscsiByIQN[ft.GetIqn()], ft)
		}
		for _, iqn := range iscsiOrder {
			frontends := iscsiByIQN[iqn]
			if len(frontends) == 0 {
				continue
			}
			out := base
			out.Protocol = ProtocolISCSI
			out.ISCSIAddr = frontends[0].GetAddr()
			out.IQN = iqn
			out.LUN = frontends[0].GetLun()
			if len(frontends) > 1 {
				out.Multipath = true
				for _, ft := range frontends {
					out.ISCSIAddrs = append(out.ISCSIAddrs, ft.GetAddr())
				}
			}
			return out, true
		}
	} else {
		for _, ft := range resp.GetFrontends() {
			if ft == nil || ft.GetProtocol() != string(ProtocolISCSI) || ft.GetAddr() == "" || ft.GetIqn() == "" || rejectFrontendAddr(ft.GetAddr(), rejectLoopback) {
				continue
			}
			out := base
			out.Protocol = ProtocolISCSI
			out.ISCSIAddr = ft.GetAddr()
			out.IQN = ft.GetIqn()
			out.LUN = ft.GetLun()
			return out, true
		}
	}
	for _, ft := range resp.GetFrontends() {
		if ft == nil || ft.GetProtocol() != string(ProtocolNVMe) || ft.GetAddr() == "" || ft.GetNqn() == "" || rejectFrontendAddr(ft.GetAddr(), rejectLoopback) {
			continue
		}
		out := base
		out.Protocol = ProtocolNVMe
		out.NVMeAddr = ft.GetAddr()
		out.NQN = ft.GetNqn()
		out.NSID = ft.GetNsid()
		return out, true
	}
	return PublishTarget{}, false
}

func rejectFrontendAddr(addr string, rejectLoopback bool) bool {
	if !rejectLoopback {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return true
	}
	ip := net.ParseIP(host)
	return ip == nil || ip.IsLoopback()
}
