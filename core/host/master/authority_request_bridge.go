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
	asks := make([]authority.AssignmentAsk, 0, len(verified.Slots))
	for _, slot := range verified.Slots {
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
		asks = append(asks, authority.AssignmentAsk{
			VolumeID:  verified.VolumeID,
			ReplicaID: slot.ReplicaID,
			DataAddr:  slot.DataAddr,
			CtrlAddr:  slot.CtrlAddr,
			Intent:    authority.IntentBind,
		})
	}
	return asks, nil
}
