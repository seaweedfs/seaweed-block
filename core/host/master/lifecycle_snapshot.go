package master

import (
	"time"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

// LifecycleSnapshot is a read-only product registration view. It is
// deliberately not authority-shaped.
type LifecycleSnapshot struct {
	Volumes            []lifecycle.VolumeRecord
	Nodes              []lifecycle.NodeRegistration
	Placements         []lifecycle.PlacementIntent
	VerifiedPlacements []lifecycle.VerifiedPlacement
}

// LifecycleSnapshot returns copies of lifecycle facts when configured.
func (h *Host) LifecycleSnapshot() (LifecycleSnapshot, bool) {
	stores := h.Lifecycle()
	if stores == nil {
		return LifecycleSnapshot{}, false
	}
	nodes := stores.Nodes.ListNodes()
	placements := stores.Placements.ListPlacements()
	verified := make([]lifecycle.VerifiedPlacement, 0, len(placements))
	cfg := lifecycle.VerificationConfig{
		Now:             time.Now().UTC(),
		FreshnessWindow: h.cfg.Freshness.FreshnessWindow,
	}
	for _, placement := range placements {
		verified = append(verified, lifecycle.VerifyPlacementIntent(placement, nodes, cfg))
	}
	return LifecycleSnapshot{
		Volumes:            stores.Volumes.ListVolumes(),
		Nodes:              nodes,
		Placements:         placements,
		VerifiedPlacements: verified,
	}, true
}
