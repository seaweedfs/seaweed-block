package volume

import control "github.com/seaweedfs/seaweed-block/core/rpc/control"

// INV-WS-STARTUP-RETRY-001: remember the newest assignment observed for us,
// including one whose local readiness admission subsequently refuses.
func (h *Host) recordSelfAssignment(f *control.AssignmentFact) {
	if f.VolumeId != h.cfg.VolumeID {
		return
	}
	h.selfLineageMu.Lock()
	defer h.selfLineageMu.Unlock()
	if f.Epoch > h.selfEpoch || f.Epoch == h.selfEpoch && f.EndpointVersion > h.selfEV {
		h.selfEpoch, h.selfEV = f.Epoch, f.EndpointVersion
	}
}

// IsCurrentReplicaSuperseded reuses the authority predicate with the newest
// observed own lineage, zero before any own assignment. Keep that lineage
// stable through the comparison; a historical other line alone cannot mean78.
// This is exit classification only: frontend admission keeps its own lineage.
func (h *Host) IsCurrentReplicaSuperseded() bool {
	h.selfLineageMu.RLock()
	defer h.selfLineageMu.RUnlock()
	other := h.LastOtherLine()
	if other == nil || other.VolumeId != h.cfg.VolumeID {
		return false
	}
	return h.IsSuperseded(h.cfg.ReplicaID, h.selfEpoch, h.selfEV)
}
