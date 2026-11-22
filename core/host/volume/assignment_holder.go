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

// clearOwnEndpointRefusal implements M3's successful self-primary admission
// under INV-AUTH-HOLDER-IDENTITY-001. A rejected endpoint is not a different
// holder: a subsequent valid fact may supersede it, but never a foreign-server
// fence or a newer lineage. Caller has validated assignmentNamesSelf.
func (h *Host) clearOwnEndpointRefusal(f *control.AssignmentFact) {
	h.otherMu.Lock()
	defer h.otherMu.Unlock()
	old := h.lastOther
	if old != nil && old.GetServerId() == h.cfg.ServerID && old.GetReplicaId() == h.cfg.ReplicaID &&
		(f.Epoch > old.Epoch || f.Epoch == old.Epoch && f.EndpointVersion >= old.EndpointVersion) {
		h.lastOther = nil
	}
}
