package volume

import control "github.com/seaweedfs/seaweed-block/core/rpc/control"

// assignmentNamesSelf implements INV-ASSIGNMENT-HOLDER-IDENTITY-001. Missing server identity is
// refused; endpoints qualify identity only when the master carries them.
func (h *Host) assignmentNamesSelf(f *control.AssignmentFact) bool {
	return f.GetServerId() != "" && f.GetServerId() == h.cfg.ServerID &&
		f.GetVolumeId() == h.cfg.VolumeID && f.GetReplicaId() == h.cfg.ReplicaID &&
		(f.GetDataAddr() == "" || f.GetDataAddr() == h.cfg.DataAddr) &&
		(f.GetCtrlAddr() == "" || f.GetCtrlAddr() == h.cfg.CtrlAddr)
}
