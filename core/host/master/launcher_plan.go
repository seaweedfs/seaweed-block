package master

import (
	"net"
	"strconv"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

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
	portAllocator := newWorkloadPortAllocator()
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
		portAllocator.assign(&plan)
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

type workloadPortAllocator struct {
	nextOrdinalByServer map[string]int
}

func newWorkloadPortAllocator() *workloadPortAllocator {
	return &workloadPortAllocator{nextOrdinalByServer: make(map[string]int)}
}

func (a *workloadPortAllocator) assign(plan *lifecycle.BlockVolumeWorkloadPlan) {
	if a == nil || plan == nil {
		return
	}
	for i := range plan.Replicas {
		replica := &plan.Replicas[i]
		ordinal := a.nextOrdinalByServer[replica.ServerID]
		a.nextOrdinalByServer[replica.ServerID] = ordinal + 1
		shiftReplicaNodeLocalPorts(replica, ordinal)
	}
}

func shiftReplicaNodeLocalPorts(replica *lifecycle.BlockVolumeReplicaWorkload, ordinal int) {
	if replica == nil || ordinal <= 0 {
		return
	}
	replica.ISCSIListenPort += ordinal
	replica.NVMeListenPort += ordinal
	replica.DataAddr = addPortOffset(replica.DataAddr, ordinal*2)
	replica.CtrlAddr = addPortOffset(replica.CtrlAddr, ordinal*2)
}

func addPortOffset(addr string, offset int) string {
	if addr == "" || offset == 0 {
		return addr
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(host, strconv.Itoa(port+offset))
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
