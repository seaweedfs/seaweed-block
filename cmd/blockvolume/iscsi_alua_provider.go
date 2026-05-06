package main

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

type projectionALUAProvider struct {
	view      *volume.AdapterProjectionView
	volumeID  string
	replicaID string
}

var _ iscsi.ALUAProvider = (*projectionALUAProvider)(nil)

func newProjectionALUAProvider(view *volume.AdapterProjectionView, volumeID, replicaID string) *projectionALUAProvider {
	return &projectionALUAProvider{
		view:      view,
		volumeID:  volumeID,
		replicaID: replicaID,
	}
}

func (p *projectionALUAProvider) ALUAState() iscsi.ALUAState {
	if p == nil || p.view == nil {
		return iscsi.ALUAUnavailable
	}
	fp := p.view.Projection()
	ep := p.view.EngineProjection()
	if fp.VolumeID != p.volumeID || fp.ReplicaID != p.replicaID {
		return iscsi.ALUAUnavailable
	}
	if fp.Healthy {
		return iscsi.ALUAActiveOptimized
	}
	switch ep.Mode {
	case engine.ModeRecovering:
		return iscsi.ALUATransitioning
	case engine.ModeHealthy:
		return iscsi.ALUAStandby
	case engine.ModeIdle:
		// Supporting replicas can remain engine-idle while still serving
		// ALUA/VPD path probes through the metadata probe backend. They are
		// standby paths, not frontend-write-ready paths.
		return iscsi.ALUAStandby
	case engine.ModeDegraded:
		return iscsi.ALUAUnavailable
	default:
		return iscsi.ALUATransitioning
	}
}

func (p *projectionALUAProvider) TargetPortGroupID() uint16 {
	return stableALUAID("tpg", p.volumeID, p.replicaID)
}

func (p *projectionALUAProvider) RelativeTargetPortID() uint16 {
	return stableALUAID("rtp", p.volumeID, p.replicaID)
}

func (p *projectionALUAProvider) DeviceNAA() [8]byte {
	sum := sha256.Sum256([]byte(p.volumeID))
	var out [8]byte
	out[0] = 0x60 | (sum[0] & 0x0f)
	copy(out[1:], sum[1:8])
	return out
}

func stableALUAID(parts ...string) uint16 {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	id := binary.BigEndian.Uint16(sum[:2])
	if id == 0 {
		return 1
	}
	return id
}
