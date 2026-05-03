package lifecycle

import (
	"fmt"
	"sort"
)

const (
	PlacementSourceBlankPool       = "blank_pool"
	PlacementSourceExistingReplica = "existing_replica"
)

// PlacementCandidate is planner output. It is a candidate for future
// controller action, not an assignment and not readiness.
type PlacementCandidate struct {
	VolumeID  string
	ServerID  string
	PoolID    string
	ReplicaID string
	Source    string
}

// PlacementConflict records inventory that blocks automatic placement.
type PlacementConflict struct {
	VolumeID string
	ServerID string
	Reason   string
}

// PlacementPlan summarizes candidate slots for one desired volume.
type PlacementPlan struct {
	VolumeID   string
	DesiredRF  int
	Candidates []PlacementCandidate
	Conflicts  []PlacementConflict
}

// PlanPlacement converts desired volume state plus node inventory into
// placement candidates. It deliberately stops before authority: callers must
// pass the plan through a controller/publisher seam before any assignment can
// exist.
func PlanPlacement(volume VolumeRecord, nodes []NodeRegistration) PlacementPlan {
	spec := volume.Spec
	plan := PlacementPlan{
		VolumeID:  spec.VolumeID,
		DesiredRF: spec.ReplicationFactor,
	}
	seenServers := make(map[string]bool)
	nodes = append([]NodeRegistration(nil), nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ServerID < nodes[j].ServerID
	})
	for _, node := range nodes {
		if seenServers[node.ServerID] {
			plan.Conflicts = append(plan.Conflicts, PlacementConflict{
				VolumeID: spec.VolumeID,
				ServerID: node.ServerID,
				Reason:   "duplicate server inventory",
			})
			continue
		}
		seenServers[node.ServerID] = true
		if existing, ok := matchingReplica(spec.VolumeID, node.Replicas); ok {
			if existing.SizeBytes != spec.SizeBytes {
				plan.Conflicts = append(plan.Conflicts, PlacementConflict{
					VolumeID: spec.VolumeID,
					ServerID: node.ServerID,
					Reason: fmt.Sprintf("existing replica %s size %d != desired %d",
						existing.ReplicaID, existing.SizeBytes, spec.SizeBytes),
				})
				continue
			}
			plan.Candidates = append(plan.Candidates, PlacementCandidate{
				VolumeID:  spec.VolumeID,
				ServerID:  node.ServerID,
				ReplicaID: existing.ReplicaID,
				Source:    PlacementSourceExistingReplica,
			})
			continue
		}
		if pool, ok := firstPoolWithCapacity(spec.SizeBytes, node.Pools); ok {
			plan.Candidates = append(plan.Candidates, PlacementCandidate{
				VolumeID: spec.VolumeID,
				ServerID: node.ServerID,
				PoolID:   pool.PoolID,
				Source:   PlacementSourceBlankPool,
			})
			continue
		}
		plan.Conflicts = append(plan.Conflicts, PlacementConflict{
			VolumeID: spec.VolumeID,
			ServerID: node.ServerID,
			Reason:   "no matching replica and no pool with enough free capacity",
		})
	}
	if len(plan.Candidates) > spec.ReplicationFactor {
		plan.Candidates = plan.Candidates[:spec.ReplicationFactor]
	}
	return plan
}

// EnoughCandidates reports whether the plan has enough candidate slots to
// satisfy the desired replication factor. This is a planner predicate only;
// it must not be treated as authority or replica_ready.
func (p PlacementPlan) EnoughCandidates() bool {
	return p.DesiredRF > 0 && len(p.Candidates) >= p.DesiredRF
}

func matchingReplica(volumeID string, replicas []ReplicaInventory) (ReplicaInventory, bool) {
	for _, replica := range replicas {
		if replica.VolumeID == volumeID {
			return replica, true
		}
	}
	return ReplicaInventory{}, false
}

func firstPoolWithCapacity(sizeBytes uint64, pools []StoragePool) (StoragePool, bool) {
	pools = append([]StoragePool(nil), pools...)
	sort.Slice(pools, func(i, j int) bool {
		if pools[i].FreeBytes == pools[j].FreeBytes {
			return pools[i].PoolID < pools[j].PoolID
		}
		return pools[i].FreeBytes > pools[j].FreeBytes
	})
	for _, pool := range pools {
		if pool.FreeBytes >= sizeBytes {
			return pool, true
		}
	}
	return StoragePool{}, false
}
