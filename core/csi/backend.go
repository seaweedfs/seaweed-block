package csi

import (
	"context"
	"errors"
	"strings"
)

var ErrPublishTargetNotFound = errors.New("csi: publish target not found")
var ErrVolumeConflict = errors.New("csi: volume already exists with different spec")

type Protocol string
type FrontendTransport string

const (
	ProtocolISCSI         Protocol          = "iscsi"
	ProtocolNVMe          Protocol          = "nvme"
	FrontendTransportTCP  FrontendTransport = "tcp"
	FrontendTransportRDMA FrontendTransport = "rdma"

	EventTypeCSIReattachObserved = "csi_reattach_observed"
	EventSeverityInfo            = "info"
)

func normalizeProtocol(p Protocol) Protocol {
	if p == "" {
		return ProtocolISCSI
	}
	return p
}

func normalizeFrontendTransport(protocol Protocol, transport FrontendTransport) FrontendTransport {
	if protocol == ProtocolNVMe && transport == "" {
		return FrontendTransportTCP
	}
	return transport
}

// PublishTarget is a read-only frontend target fact. Epoch and
// EndpointVersion are evidence copied from master status so recovery gates can
// compare before/after target generations. They are not exposed in CSI
// publish_context and do not give CSI authority mutation capability.
type PublishTarget struct {
	VolumeID          string
	ReplicaID         string
	Epoch             uint64
	EndpointVersion   uint64
	Protocol          Protocol
	FrontendTransport FrontendTransport

	ISCSIAddr  string
	ISCSIAddrs []string
	IQN        string
	LUN        uint32
	Multipath  bool

	NVMeAddr  string
	NVMeAddrs []string
	NQN       string
	NSID      uint32
}

type PublishTargetLookup interface {
	LookupPublishTarget(ctx context.Context, volumeID, nodeID string) (PublishTarget, error)
}

type ClusterEvent struct {
	VolumeID        string
	ReplicaID       string
	NodeName        string
	Type            string
	Severity        string
	Message         string
	Reason          string
	OldValue        string
	NewValue        string
	Epoch           uint64
	EndpointVersion uint64
	CorrelationID   string
	EvidenceRef     string
}

type EventReporter interface {
	ReportEvent(ctx context.Context, event ClusterEvent) error
}

// VolumeProvisioner records product-level desired volume intent. It must not
// mint assignment, epoch, endpoint_version, readiness, or frontend facts.
type VolumeProvisioner interface {
	CreateVolume(ctx context.Context, spec VolumeSpec) (VolumeSpec, error)
	DeleteVolume(ctx context.Context, volumeID string) error
}

// VolumeObjectRegistrar creates the Kubernetes-facing SwBlockVolume identity
// object after CSI provisioning succeeds. It owns metadata/spec identity only;
// status remains operator-status owned and finalizers remain lifecycle-owner
// owned.
type VolumeObjectRegistrar interface {
	EnsureVolumeObject(ctx context.Context, spec VolumeSpec) error
}

type KubernetesMetadataResolver interface {
	ResolvePVCUID(ctx context.Context, name, namespace string) (string, error)
}

type VolumeSpec struct {
	VolumeID          string
	SizeBytes         uint64
	ReplicationFactor int
	Protocol          Protocol
	FrontendTransport FrontendTransport
	SourceSnapshotID  string
	PVCName           string
	PVCNamespace      string
	PVCUID            string
	PVName            string
	StorageClass      string
}

func publishContext(t PublishTarget) map[string]string {
	ctx := map[string]string{}
	if t.Protocol != "" {
		ctx["protocol"] = string(t.Protocol)
	}
	if t.Protocol == ProtocolNVMe {
		if t.FrontendTransport != "" {
			ctx["nvmeTransport"] = string(t.FrontendTransport)
		}
	}
	if t.ISCSIAddr != "" && t.IQN != "" {
		ctx["iscsiAddr"] = t.ISCSIAddr
		ctx["iqn"] = t.IQN
	}
	if len(t.ISCSIAddrs) > 0 && t.IQN != "" {
		ctx["iscsiAddrs"] = strings.Join(t.ISCSIAddrs, ",")
		if ctx["iscsiAddr"] == "" {
			ctx["iscsiAddr"] = t.ISCSIAddrs[0]
		}
		ctx["iqn"] = t.IQN
	}
	if t.Multipath {
		ctx["stage2_multipath"] = "true"
	}
	if t.NVMeAddr != "" && t.NQN != "" {
		ctx["nvmeAddr"] = t.NVMeAddr
		ctx["nqn"] = t.NQN
	}
	if len(t.NVMeAddrs) > 0 && t.NQN != "" {
		ctx["nvmeAddrs"] = strings.Join(t.NVMeAddrs, ",")
		if ctx["nvmeAddr"] == "" {
			ctx["nvmeAddr"] = t.NVMeAddrs[0]
		}
		ctx["nqn"] = t.NQN
	}
	return ctx
}
