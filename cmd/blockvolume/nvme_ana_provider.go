package main

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

type projectionANAProvider struct {
	view      *volume.AdapterProjectionView
	volumeID  string
	replicaID string
}

var _ nvme.ANAProvider = (*projectionANAProvider)(nil)

func newProjectionANAProvider(view *volume.AdapterProjectionView, volumeID, replicaID string, _ ...string) *projectionANAProvider {
	return &projectionANAProvider{
		view:      view,
		volumeID:  volumeID,
		replicaID: replicaID,
	}
}

func (p *projectionANAProvider) ANAState() nvme.ANAState {
	if p == nil || p.view == nil {
		return nvme.ANAInaccessible
	}
	fp := p.view.Projection()
	if fp.VolumeID != p.volumeID || fp.ReplicaID != p.replicaID {
		return nvme.ANAInaccessible
	}
	ep := p.view.EngineProjection()
	if fp.Healthy {
		return nvme.ANAOptimized
	}
	switch ep.Mode {
	case engine.ModeRecovering:
		return nvme.ANAChange
	case engine.ModeHealthy, engine.ModeIdle:
		return nvme.ANANonOptimized
	case engine.ModeDegraded:
		return nvme.ANAInaccessible
	default:
		return nvme.ANAChange
	}
}

func (p *projectionANAProvider) ANAChangeCount() uint64 {
	if p == nil || p.view == nil {
		return 1
	}
	ep := p.view.EngineProjection()
	const maxUint32 = uint64(^uint32(0))
	if ep.Epoch <= maxUint32 && ep.EndpointVersion <= maxUint32 {
		count := uint64(uint32(ep.Epoch))<<32 | uint64(uint32(ep.EndpointVersion))
		if count == 0 {
			return 1
		}
		return count
	}
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], ep.Epoch)
	binary.LittleEndian.PutUint64(buf[8:16], ep.EndpointVersion)
	h := fnv.New64a()
	_, _ = h.Write(buf[:])
	count := h.Sum64()
	if count == 0 {
		return 1
	}
	return count
}

func (p *projectionANAProvider) ANAGroupID() uint32 {
	// Linux validates ANA group descriptors against Identify Controller's
	// ANAGRPMAX/NANAGRPID. The current target exposes one namespace and one ANA
	// group, so the only valid advertised group id is the dense group 1.
	return 1
}
