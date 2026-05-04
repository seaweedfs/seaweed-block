package master

import "github.com/seaweedfs/seaweed-block/core/lifecycle"

// LifecycleWorkloadPlanTickResult summarizes the launcher planning seam. Plans
// are workload intents only; they do not mint authority.
type LifecycleWorkloadPlanTickResult struct {
	PlannedVolumes          int
	MaterializedPlacements  int
	Plans                   []lifecycle.BlockVolumeWorkloadPlan
	SkippedMissingVolume    int
	SkippedMissingInventory int
}

// RunLifecycleWorkloadPlanTick converts desired lifecycle state into
// blockvolume workload plans. If a plan assigns concrete replica IDs for
// blank-pool slots, it writes those identities back as existing-replica
// placement intent so the launched blockvolume daemons can later verify via
// heartbeat.
func (h *Host) RunLifecycleWorkloadPlanTick(cfg lifecycle.WorkloadPlanConfig) (LifecycleWorkloadPlanTickResult, error) {
	stores := h.Lifecycle()
	if stores == nil {
		return LifecycleWorkloadPlanTickResult{}, nil
	}
	volumes := stores.Volumes.ListVolumes()
	volumeByID := make(map[string]lifecycle.VolumeRecord, len(volumes))
	for _, volume := range volumes {
		volumeByID[volume.Spec.VolumeID] = volume
	}
	nodes := stores.Nodes.ListNodes()
	var result LifecycleWorkloadPlanTickResult
	for _, placement := range stores.Placements.ListPlacements() {
		volume, ok := volumeByID[placement.VolumeID]
		if !ok {
			result.SkippedMissingVolume++
			continue
		}
		plan, err := lifecycle.PlanBlockVolumeWorkloads(volume, placement, nodes, cfg)
		if err != nil {
			result.SkippedMissingInventory++
			continue
		}
		result.Plans = append(result.Plans, plan)
		result.PlannedVolumes++
		if placementHasBlankPool(placement) {
			materialized, err := lifecycle.MaterializePlacementFromWorkloadPlan(placement, plan)
			if err != nil {
				return result, err
			}
			if _, err := stores.Placements.ApplyPlan(placementPlanFromIntent(materialized)); err != nil {
				return result, err
			}
			result.MaterializedPlacements++
		}
	}
	return result, nil
}

func placementHasBlankPool(intent lifecycle.PlacementIntent) bool {
	for _, slot := range intent.Slots {
		if slot.Source == lifecycle.PlacementSourceBlankPool {
			return true
		}
	}
	return false
}

func placementPlanFromIntent(intent lifecycle.PlacementIntent) lifecycle.PlacementPlan {
	plan := lifecycle.PlacementPlan{
		VolumeID:   intent.VolumeID,
		DesiredRF:  intent.DesiredRF,
		Candidates: make([]lifecycle.PlacementCandidate, 0, len(intent.Slots)),
	}
	for _, slot := range intent.Slots {
		plan.Candidates = append(plan.Candidates, lifecycle.PlacementCandidate{
			VolumeID:  intent.VolumeID,
			ServerID:  slot.ServerID,
			PoolID:    slot.PoolID,
			ReplicaID: slot.ReplicaID,
			Source:    slot.Source,
		})
	}
	return plan
}
