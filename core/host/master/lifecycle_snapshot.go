package master

import "github.com/seaweedfs/seaweed-block/core/lifecycle"

// LifecycleSnapshot is a read-only product registration view. It is
// deliberately not authority-shaped.
type LifecycleSnapshot struct {
	Volumes    []lifecycle.VolumeRecord
	Nodes      []lifecycle.NodeRegistration
	Placements []lifecycle.PlacementIntent
}

// LifecycleSnapshot returns copies of lifecycle facts when configured.
func (h *Host) LifecycleSnapshot() (LifecycleSnapshot, bool) {
	stores := h.Lifecycle()
	if stores == nil {
		return LifecycleSnapshot{}, false
	}
	return LifecycleSnapshot{
		Volumes:    stores.Volumes.ListVolumes(),
		Nodes:      stores.Nodes.ListNodes(),
		Placements: stores.Placements.ListPlacements(),
	}, true
}
