package volume

import control "github.com/seaweedfs/seaweed-block/core/rpc/control"

// assignmentNamesSelf implements INV-AUTH-HOLDER-IDENTITY-001. Missing server identity is
// refused; endpoints qualify identity only when the master carries them.
func (h *Host) assignmentNamesSelf(f *control.AssignmentFact) bool {
	return f.GetServerId() != "" && f.GetServerId() == h.cfg.ServerID &&
		f.GetVolumeId() == h.cfg.VolumeID && f.GetReplicaId() == h.cfg.ReplicaID &&
		(f.GetDataAddr() == "" || f.GetDataAddr() == h.cfg.DataAddr) &&
		(f.GetCtrlAddr() == "" || f.GetCtrlAddr() == h.cfg.CtrlAddr)
}

// observeForeignHolder implements M3 / INV-AUTH-HOLDER-IDENTITY-001.
// Delivery before a move is not supersession: record every refused fact,
// and use the one supersession decision against the adopted engine lineage.
func (h *Host) observeForeignHolder(f *control.AssignmentFact) {
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
