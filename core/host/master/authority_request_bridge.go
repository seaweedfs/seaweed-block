package master

import (
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

// assignmentRequestsFromVerifiedPlacement is the G9F-2 controller seam from
// verified placement facts to authority asks. It deliberately returns
// AssignmentAsk values only: publisher remains the sole epoch/endpoint-version
// author.
func assignmentRequestsFromVerifiedPlacement(verified lifecycle.VerifiedPlacement) ([]authority.AssignmentAsk, error) {
	if !verified.Verified {
		return nil, nil
	}
	if verified.VolumeID == "" {
		return nil, fmt.Errorf("master: verified placement missing volume id")
	}
	if len(verified.Slots) == 0 {
		return nil, nil
	}
	// The current authority publisher mints one frontend-primary line per
	// volume. RF>1 slots are placement/recovery inputs; they are not multiple
	// competing Bind asks. Pick the first verified slot deterministically until
	// a later placement policy grows an explicit primary-candidate field.
	slot := verified.Slots[0]
	if slot.Source != lifecycle.PlacementSourceExistingReplica {
		return nil, fmt.Errorf("master: verified placement slot %s/%s source %q needs replica-id allocator",
			verified.VolumeID, slot.ServerID, slot.Source)
	}
	if slot.ReplicaID == "" {
		return nil, fmt.Errorf("master: verified placement slot %s/%s missing replica id", verified.VolumeID, slot.ServerID)
	}
	if slot.DataAddr == "" || slot.CtrlAddr == "" {
		return nil, fmt.Errorf("master: verified placement slot %s/%s missing data/control address", verified.VolumeID, slot.ReplicaID)
	}
	return []authority.AssignmentAsk{{
		VolumeID:  verified.VolumeID,
		ReplicaID: slot.ReplicaID,
		DataAddr:  slot.DataAddr,
		CtrlAddr:  slot.CtrlAddr,
		Intent:    authority.IntentBind,
	}}, nil
}
