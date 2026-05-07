package main

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

type projectionANAProvider struct {
	view      *volume.AdapterProjectionView
	volumeID  string
	replicaID string
	seed      string
}

var _ nvme.ANAProvider = (*projectionANAProvider)(nil)

func newProjectionANAProvider(view *volume.AdapterProjectionView, volumeID, replicaID string, exportSeed ...string) *projectionANAProvider {
	seed := volumeID
	for _, candidate := range exportSeed {
		if candidate != "" {
			seed = candidate
			break
		}
	}
	return &projectionANAProvider{
		view:      view,
		volumeID:  volumeID,
		replicaID: replicaID,
		seed:      seed,
	}
}

func (p *projectionANAProvider) ANAState() nvme.ANAState {
	if p == nil || p.view == nil {
		return nvme.ANAInaccessible
	}
	fp := p.view.Projection()
	ep := p.view.EngineProjection()
	if fp.VolumeID != p.volumeID || fp.ReplicaID != p.replicaID {
		return nvme.ANAInaccessible
	}
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

func (p *projectionANAProvider) ANAGroupID() uint32 {
	return stableANAID("ana", p.seed, p.volumeID, p.replicaID)
}

func (p *projectionANAProvider) ANAChangeCount() uint64 {
	if p == nil || p.view == nil {
		return 1
	}
	ep := p.view.EngineProjection()
	count := ep.Epoch<<32 | ep.EndpointVersion
	if count == 0 {
		return 1
	}
	return count
}

func stableANAID(parts ...string) uint32 {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	id := binary.BigEndian.Uint32(sum[:4])
	if id == 0 {
		return 1
	}
	return id
}
