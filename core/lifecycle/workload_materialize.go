package lifecycle

import "fmt"

// MaterializePlacementFromWorkloadPlan records concrete replica IDs chosen by
// the launcher back into placement intent. It converts blank-pool slots into
// existing-replica slots only after a workload plan has produced per-replica
// identities.
//
// This remains non-authority: it does not choose primary, mark readiness, or
// mint epoch/endpoint-version.
func MaterializePlacementFromWorkloadPlan(placement PlacementIntent, plan BlockVolumeWorkloadPlan) (PlacementIntent, error) {
	if placement.VolumeID != plan.VolumeID {
		return PlacementIntent{}, fmt.Errorf("%w: workload plan volume %q != placement volume %q", ErrInvalidVolumeSpec, plan.VolumeID, placement.VolumeID)
	}
	if len(placement.Slots) != len(plan.Replicas) {
		return PlacementIntent{}, fmt.Errorf("%w: workload replica count %d != placement slot count %d", ErrInvalidVolumeSpec, len(plan.Replicas), len(placement.Slots))
	}
	out := placement
	out.Slots = append([]PlacementSlotIntent(nil), placement.Slots...)
	for i := range out.Slots {
		slot := &out.Slots[i]
		replica := plan.Replicas[i]
		if slot.ServerID != replica.ServerID {
			return PlacementIntent{}, fmt.Errorf("%w: workload replica[%d] server %q != placement server %q", ErrInvalidVolumeSpec, i, replica.ServerID, slot.ServerID)
		}
		if replica.ReplicaID == "" {
			return PlacementIntent{}, fmt.Errorf("%w: workload replica[%d] missing replica id", ErrInvalidVolumeSpec, i)
		}
		slot.ReplicaID = replica.ReplicaID
		slot.Source = PlacementSourceExistingReplica
	}
	return out, nil
}
