package authority

import "github.com/seaweedfs/seaweed-block/core/adapter"

// captureAssignmentOwner binds owner and line in one publication under p.mu
// (INV-AUTH-HOLDER-IDENTITY-001). Reload derives it from that same durable
// record's journal. A reserved/unissued move never permits placement fallback.
func (p *Publisher) captureAssignmentOwner(info *adapter.AssignmentInfo, issuing *WorkspaceState) {
	state := issuing
	if state == nil {
		state = p.workspace[info.VolumeID]
	}
	if state == nil {
		return
	}
	for _, move := range state.Moves {
		if move.NewReplicaID == info.ReplicaID && move.NewEpoch == info.Epoch {
			info.OwnerFromMove = true
			info.ServerID = ""
			if move.Issued {
				info.ServerID = move.NewHost
			}
			return
		}
	}
}
