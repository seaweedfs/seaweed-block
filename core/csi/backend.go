package csi

import (
	"context"
	"errors"
)

var ErrPublishTargetNotFound = errors.New("csi: publish target not found")

type Protocol string

const (
	ProtocolISCSI Protocol = "iscsi"
	ProtocolNVMe  Protocol = "nvme"
)

// PublishTarget is a read-only frontend target fact. It is deliberately
// not authority-shaped: no epoch/endpoint_version inputs, no assignment
// intent, no publisher call surface.
type PublishTarget struct {
	VolumeID  string
	ReplicaID string
	Protocol  Protocol

	ISCSIAddr string
	IQN       string
	LUN       uint32

	NVMeAddr string
	NQN      string
	NSID     uint32
}

type PublishTargetLookup interface {
	LookupPublishTarget(ctx context.Context, volumeID, nodeID string) (PublishTarget, error)
}

func publishContext(t PublishTarget) map[string]string {
	ctx := map[string]string{}
	if t.ISCSIAddr != "" && t.IQN != "" {
		ctx["iscsiAddr"] = t.ISCSIAddr
		ctx["iqn"] = t.IQN
	}
	if t.NVMeAddr != "" && t.NQN != "" {
		ctx["nvmeAddr"] = t.NVMeAddr
		ctx["nqn"] = t.NQN
	}
	return ctx
}
