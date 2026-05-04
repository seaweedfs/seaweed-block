package lifecycle

import (
	"fmt"
	"sort"
)

// BlockVolumeWorkloadPlan is launcher input. It describes which blockvolume
// daemons should exist for one desired volume. It is deliberately not
// authority-shaped: no epoch, endpoint_version, primary, ready, or healthy.
type BlockVolumeWorkloadPlan struct {
	VolumeID  string
	SizeBytes uint64
	Replicas  []BlockVolumeReplicaWorkload
}

type BlockVolumeReplicaWorkload struct {
	ServerID           string
	PoolID             string
	ReplicaID          string
	Source             string
	DataAddr           string
	CtrlAddr           string
	ISCSIListenPort    int
	ISCSIQualifiedName string
}

// PlanBlockVolumeWorkloads converts desired lifecycle state plus placement
// intent into blockvolume daemon workload intent. It does not verify
// observation, choose primary, or publish authority.
func PlanBlockVolumeWorkloads(volume VolumeRecord, placement PlacementIntent, nodes []NodeRegistration, cfg WorkloadPlanConfig) (BlockVolumeWorkloadPlan, error) {
	if err := validateSpec(volume.Spec); err != nil {
		return BlockVolumeWorkloadPlan{}, err
	}
	if placement.VolumeID != volume.Spec.VolumeID {
		return BlockVolumeWorkloadPlan{}, fmt.Errorf("%w: placement volume %q != desired volume %q", ErrInvalidVolumeSpec, placement.VolumeID, volume.Spec.VolumeID)
	}
	if err := validatePlacementIntent(placement); err != nil {
		return BlockVolumeWorkloadPlan{}, err
	}
	if cfg.ISCSIPortBase == 0 {
		cfg.ISCSIPortBase = 3260
	}
	if cfg.IQNPrefix == "" {
		cfg.IQNPrefix = "iqn.2026-05.io.seaweedfs"
	}
	nodeByID := make(map[string]NodeRegistration, len(nodes))
	for _, node := range nodes {
		nodeByID[node.ServerID] = node
	}

	out := BlockVolumeWorkloadPlan{
		VolumeID:  volume.Spec.VolumeID,
		SizeBytes: volume.Spec.SizeBytes,
		Replicas:  make([]BlockVolumeReplicaWorkload, 0, len(placement.Slots)),
	}
	for i, slot := range placement.Slots {
		node, ok := nodeByID[slot.ServerID]
		if !ok {
			return BlockVolumeWorkloadPlan{}, fmt.Errorf("%w: placement server %q has no node inventory", ErrInvalidNodeRegistration, slot.ServerID)
		}
		replicaID := slot.ReplicaID
		if replicaID == "" {
			replicaID = fmt.Sprintf("r%d", i+1)
		}
		dataAddr, ctrlAddr := nodePlacementAddrs(node)
		out.Replicas = append(out.Replicas, BlockVolumeReplicaWorkload{
			ServerID:           slot.ServerID,
			PoolID:             slot.PoolID,
			ReplicaID:          replicaID,
			Source:             slot.Source,
			DataAddr:           dataAddr,
			CtrlAddr:           ctrlAddr,
			ISCSIListenPort:    cfg.ISCSIPortBase + i,
			ISCSIQualifiedName: fmt.Sprintf("%s:%s", cfg.IQNPrefix, volume.Spec.VolumeID),
		})
	}
	return out, nil
}

type WorkloadPlanConfig struct {
	ISCSIPortBase int
	IQNPrefix     string
}

func SortWorkloadPlans(plans []BlockVolumeWorkloadPlan) {
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].VolumeID < plans[j].VolumeID
	})
}
