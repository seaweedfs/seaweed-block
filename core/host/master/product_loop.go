package master

// LifecycleProductTickResult summarizes one explicit product-loop tick.
type LifecycleProductTickResult struct {
	VerifiedPlacements int
	PublishedAsks      int
	SkippedCurrent     int
	SkippedUnverified  int
}

// RunLifecycleProductTick drives the first G9G product loop:
// lifecycle facts -> verified placement -> AssignmentAsk -> controller
// directive queue. It never calls Publisher.apply; Publisher remains the only
// authority minter through its normal Run loop.
func (h *Host) RunLifecycleProductTick() (LifecycleProductTickResult, error) {
	stores := h.Lifecycle()
	if stores == nil {
		return LifecycleProductTickResult{}, nil
	}
	nodes := stores.Nodes.ListNodes()
	placements := stores.Placements.ListPlacements()
	verified := h.verifyPlacements(placements, nodes)
	result := LifecycleProductTickResult{
		VerifiedPlacements: len(verified),
	}
	for _, placement := range verified {
		if !placement.Verified {
			result.SkippedUnverified++
			continue
		}
		asks, err := assignmentRequestsFromVerifiedPlacement(placement)
		if err != nil {
			return result, err
		}
		for _, ask := range asks {
			if h.assignmentAskAlreadyCurrent(ask.VolumeID, ask.ReplicaID, ask.DataAddr, ask.CtrlAddr) {
				result.SkippedCurrent++
				continue
			}
			if err := h.ctrl.SubmitAssignmentAsk(ask); err != nil {
				return result, err
			}
			result.PublishedAsks++
		}
	}
	return result, nil
}

func (h *Host) assignmentAskAlreadyCurrent(volumeID, replicaID, dataAddr, ctrlAddr string) bool {
	line, ok := h.Publisher().VolumeAuthorityLine(volumeID)
	if !ok {
		return false
	}
	return line.Assigned &&
		line.ReplicaID == replicaID &&
		line.DataAddr == dataAddr &&
		line.CtrlAddr == ctrlAddr
}
