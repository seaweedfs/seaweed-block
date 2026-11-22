package volume

import (
	"fmt"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// assignmentNamesSelf implements INV-AUTH-HOLDER-IDENTITY-001. Missing server identity is
// refused. Endpoints are routing data and never qualify holder identity.
func (h *Host) assignmentNamesSelf(f *control.AssignmentFact) bool {
	return f.GetServerId() != "" && f.GetServerId() == h.cfg.ServerID &&
		f.GetVolumeId() == h.cfg.VolumeID && f.GetReplicaId() == h.cfg.ReplicaID
}

// observeForeignHolder implements M3 / INV-AUTH-HOLDER-IDENTITY-001.
// Delivery before a move is not supersession: record every refused fact,
// and use the one supersession decision against the adopted engine lineage.
func (h *Host) observeForeignHolder(f *control.AssignmentFact) {
	h.log.Printf("blockvolume: assignment identity refusal volume=%q replica=%q server=%q epoch=%d ev=%d data=%q ctrl=%q "+
		"expected_volume=%q expected_replica=%q expected_server=%q expected_data=%q expected_ctrl=%q",
		f.VolumeId, f.ReplicaId, f.ServerId, f.Epoch, f.EndpointVersion, f.DataAddr, f.CtrlAddr,
		h.cfg.VolumeID, h.cfg.ReplicaID, h.cfg.ServerID, h.cfg.DataAddr, h.cfg.CtrlAddr)
	h.recordOtherLine(f)
	if h.view == nil {
		return
	}
	own := h.view.EngineProjection()
	if !h.IsSuperseded(h.cfg.ReplicaID, own.Epoch, own.EndpointVersion) {
		h.log.Printf("blockvolume: volume %s foreign assignment server=%q replica=%q recorded, not adopted; waiting for own current line", h.cfg.VolumeID, f.ServerId, f.ReplicaId)
		return
	}
	h.wd.notPrimary()
	if err := h.fenceSelfWithPeers(selfRolePrimary, f.Epoch, false); err != nil {
		h.log.Printf("blockvolume: foreign holder ingress fence: %v", err)
	}
	h.log.Printf("blockvolume: volume %s superseded by assignment server=%q replica=%q; fenced, not serving", h.cfg.VolumeID, f.ServerId, f.ReplicaId)
}

// reportAssignmentEndpoints is diagnostic only under INV-AUTH-HOLDER-IDENTITY-001:
// the master's published routing values need not spell this process's listeners.
func (h *Host) reportAssignmentEndpoints(f *control.AssignmentFact) {
	warning := ""
	if (f.DataAddr != "" && f.DataAddr != h.cfg.DataAddr) || (f.CtrlAddr != "" && f.CtrlAddr != h.cfg.CtrlAddr) {
		warning = fmt.Sprintf("master advertises data=%s ctrl=%s for this holder; this process listens on %s, %s", f.DataAddr, f.CtrlAddr, h.cfg.DataAddr, h.cfg.CtrlAddr)
	}
	previous := h.assignmentEndpointWarning.Swap(warning)
	if warning != "" && previous != warning {
		h.log.Printf("blockvolume: %s", warning)
	}
}

// AssignmentEndpointWarning is visible on status without changing readiness.
func (h *Host) AssignmentEndpointWarning() string {
	warning, _ := h.assignmentEndpointWarning.Load().(string)
	return warning
}

// refusePrimaryAssignment implements M3 / INV-AUTH-HOLDER-IDENTITY-001:
// a candidate cannot enter the adapter behind an already observed foreign line.
// Identity and ordering each use their existing single deciding function.
func (h *Host) refusePrimaryAssignment(f *control.AssignmentFact) bool {
	if f.ReplicaId != h.cfg.ReplicaID {
		return false
	}
	if !h.assignmentNamesSelf(f) {
		h.observeForeignHolder(f)
		return true
	}
	other := h.LastOtherLine()
	// A candidate's owner cannot change within one epoch, even when its EV
	// advances. This is an admission refusal, not a supersession fence on
	// an already-adopted lineage (whose strict-newer rule is unchanged).
	sameEpochForeign := other != nil && other.Epoch == f.Epoch && other.ReplicaId == f.ReplicaId && other.ServerId != "" && other.ServerId != f.ServerId
	if sameEpochForeign || h.IsSuperseded(h.cfg.ReplicaID, f.Epoch, f.EndpointVersion) {
		h.log.Printf("blockvolume: volume %s refuses stale self-labelled assignment server=%q epoch=%d ev=%d behind recorded foreign line",
			h.cfg.VolumeID, f.ServerId, f.Epoch, f.EndpointVersion)
		return true
	}
	return false
}
