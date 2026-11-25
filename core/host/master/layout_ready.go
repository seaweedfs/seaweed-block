package master

import "github.com/seaweedfs/seaweed-block/core/lifecycle"

// layoutStoreReady is a placement hint, not Start's safety check. Production
// layout masters install LayoutCreateVolume; older non-layout deployments do
// not use the filer-ready bit.
func (h *Host) layoutStoreReady(volume string) bool {
	if !h.cfg.WorkspaceRPC && h.cfg.LayoutCreateVolume == nil {
		return true
	}
	if h.lifecycle == nil || h.lifecycle.Volumes == nil {
		return false
	}
	r, ok := h.lifecycle.Volumes.GetVolumeSnapshot(volume)
	return ok && r.StoreReady
}

func (h *Host) readyVolumeRecords(all []lifecycle.VolumeRecord) []lifecycle.VolumeRecord {
	ready := make([]lifecycle.VolumeRecord, 0, len(all))
	for _, r := range all {
		if h.layoutStoreReady(r.Spec.VolumeID) {
			ready = append(ready, r)
		}
	}
	return ready
}

func (h *Host) readyPlacements(all []lifecycle.PlacementIntent) []lifecycle.PlacementIntent {
	ready := make([]lifecycle.PlacementIntent, 0, len(all))
	for _, p := range all {
		if h.layoutStoreReady(p.VolumeID) {
			ready = append(ready, p)
		}
	}
	return ready
}
