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
	type pendingPlan struct {
		placement   lifecycle.PlacementIntent
		plan        lifecycle.BlockVolumeWorkloadPlan
		materialize bool
	}
	var result LifecycleWorkloadPlanTickResult
	var pending []pendingPlan
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
		pending = append(pending, pendingPlan{
			placement:   placement,
			plan:        plan,
			materialize: placementHasBlankPool(placement),
		})
		result.PlannedVolumes++
	}
	plans := make([]*lifecycle.BlockVolumeWorkloadPlan, 0, len(pending))
	for i := range pending {
		plans = append(plans, &pending[i].plan)
	}
	newWorkloadPortAllocator().assignAll(plans)
	for _, item := range pending {
		result.Plans = append(result.Plans, item.plan)
		if item.materialize {
			materialized, err := lifecycle.MaterializePlacementFromWorkloadPlan(item.placement, item.plan)
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
	nextOrdinalByNode map[string]int
	baseByNode        map[string]nodePortBase
	usedISCSIByNode   map[string]map[int]bool
	usedNVMeByNode    map[string]map[int]bool
	usedDataByNode    map[string]map[string]bool
	usedCtrlByNode    map[string]map[string]bool
}

type nodePortBase struct {
	iscsi int
	nvme  int
	data  string
	ctrl  string
}

func newWorkloadPortAllocator() *workloadPortAllocator {
	return &workloadPortAllocator{
		nextOrdinalByNode: make(map[string]int),
		baseByNode:        make(map[string]nodePortBase),
		usedISCSIByNode:   make(map[string]map[int]bool),
		usedNVMeByNode:    make(map[string]map[int]bool),
		usedDataByNode:    make(map[string]map[string]bool),
		usedCtrlByNode:    make(map[string]map[string]bool),
	}
}

func (a *workloadPortAllocator) assign(plan *lifecycle.BlockVolumeWorkloadPlan) {
	a.assignAll([]*lifecycle.BlockVolumeWorkloadPlan{plan})
}

func (a *workloadPortAllocator) assignAll(plans []*lifecycle.BlockVolumeWorkloadPlan) {
	if a == nil {
		return
	}
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		for i := range plan.Replicas {
			replica := &plan.Replicas[i]
			key := workloadNodeKey(*replica)
			a.ensureNode(key)
			if _, ok := a.baseByNode[key]; !ok {
				a.baseByNode[key] = nodePortBase{
					iscsi: replica.ISCSIListenPort - i,
					nvme:  replica.NVMeListenPort - i,
					data:  addPortOffset(replica.DataAddr, -i*2),
					ctrl:  addPortOffset(replica.CtrlAddr, -i*2),
				}
			}
			if replica.PortAssignmentPinned {
				a.markUsed(key, *replica)
			}
		}
	}
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		for i := range plan.Replicas {
			replica := &plan.Replicas[i]
			if replica.PortAssignmentPinned {
				continue
			}
			key := workloadNodeKey(*replica)
			a.ensureNode(key)
			base, ok := a.baseByNode[key]
			if !ok {
				base = nodePortBase{
					iscsi: replica.ISCSIListenPort - i,
					nvme:  replica.NVMeListenPort - i,
					data:  addPortOffset(replica.DataAddr, -i*2),
					ctrl:  addPortOffset(replica.CtrlAddr, -i*2),
				}
				a.baseByNode[key] = base
			}
			ordinal := a.nextFreeOrdinal(key, base)
			a.nextOrdinalByNode[key] = ordinal + 1
			replica.ISCSIListenPort = base.iscsi + ordinal
			replica.NVMeListenPort = base.nvme + ordinal
			replica.DataAddr = addPortOffset(base.data, ordinal*2)
			replica.CtrlAddr = addPortOffset(base.ctrl, ordinal*2)
			a.markUsed(key, *replica)
		}
	}
}

func workloadNodeKey(replica lifecycle.BlockVolumeReplicaWorkload) string {
	if replica.KubernetesNodeName != "" {
		return replica.KubernetesNodeName
	}
	return replica.ServerID
}

func (a *workloadPortAllocator) ensureNode(key string) {
	if a.usedISCSIByNode[key] == nil {
		a.usedISCSIByNode[key] = make(map[int]bool)
		a.usedNVMeByNode[key] = make(map[int]bool)
		a.usedDataByNode[key] = make(map[string]bool)
		a.usedCtrlByNode[key] = make(map[string]bool)
	}
}

func (a *workloadPortAllocator) markUsed(key string, replica lifecycle.BlockVolumeReplicaWorkload) {
	a.ensureNode(key)
	if replica.ISCSIListenPort != 0 {
		a.usedISCSIByNode[key][replica.ISCSIListenPort] = true
	}
	if replica.NVMeListenPort != 0 {
		a.usedNVMeByNode[key][replica.NVMeListenPort] = true
	}
	if replica.DataAddr != "" {
		a.usedDataByNode[key][replica.DataAddr] = true
	}
	if replica.CtrlAddr != "" {
		a.usedCtrlByNode[key][replica.CtrlAddr] = true
	}
}

func (a *workloadPortAllocator) nextFreeOrdinal(key string, base nodePortBase) int {
	for ordinal := a.nextOrdinalByNode[key]; ; ordinal++ {
		data := addPortOffset(base.data, ordinal*2)
		ctrl := addPortOffset(base.ctrl, ordinal*2)
		if a.usedISCSIByNode[key][base.iscsi+ordinal] ||
			a.usedNVMeByNode[key][base.nvme+ordinal] ||
			a.usedDataByNode[key][data] ||
			a.usedCtrlByNode[key][ctrl] {
			continue
		}
		return ordinal
	}
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
			VolumeID:        intent.VolumeID,
			ServerID:        slot.ServerID,
			PoolID:          slot.PoolID,
			ReplicaID:       slot.ReplicaID,
			Source:          slot.Source,
			DataAddr:        slot.DataAddr,
			CtrlAddr:        slot.CtrlAddr,
			ISCSIListenPort: slot.ISCSIListenPort,
			NVMeListenPort:  slot.NVMeListenPort,
		})
	}
	return plan
}
