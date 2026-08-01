package lifecycle

// ReconcileResult reports one desired volume reconciliation attempt.
type ReconcileResult struct {
	VolumeID string
	Plan     PlacementPlan
	Intent   PlacementIntent
	Applied  bool
	Err      error
}

// ReconcilePlacement plans every desired volume against the latest node
// inventory and persists placement intent only when enough candidates exist.
// This is still controller input: no authority is minted here.
func ReconcilePlacement(volumes []VolumeRecord, nodes []NodeRegistration, placements *PlacementIntentStore) []ReconcileResult {
	volumes = append([]VolumeRecord(nil), volumes...)
	nodes = append([]NodeRegistration(nil), nodes...)
	results := make([]ReconcileResult, 0, len(volumes))
	for _, volume := range volumes {
		if volume.RestoreState == VolumeRestoreAbortRequested || volume.RestoreState == VolumeRestoreDiscarded {
			continue
		}
		plan := PlanPlacement(volume, nodes)
		result := ReconcileResult{
			VolumeID: volume.Spec.VolumeID,
			Plan:     plan,
		}
		if existing, ok := placements.GetPlacement(volume.Spec.VolumeID); ok &&
			shouldPreserveMaterializedPlacement(volume, existing, nodes) {
			result.Intent = existing
			result.Applied = true
			results = append(results, result)
			continue
		}
		intent, err := placements.ApplyPlan(plan)
		if err != nil {
			result.Err = err
			results = append(results, result)
			continue
		}
		result.Intent = intent
		result.Applied = true
		results = append(results, result)
	}
	return results
}

func shouldPreserveMaterializedPlacement(volume VolumeRecord, intent PlacementIntent, nodes []NodeRegistration) bool {
	if intent.VolumeID != volume.Spec.VolumeID || intent.DesiredRF != volume.Spec.ReplicationFactor {
		return false
	}
	if intent.RestoreSnapshotID != desiredPlacementRestoreSnapshotID(volume) {
		return false
	}
	if len(intent.Slots) != intent.DesiredRF || len(intent.Slots) == 0 {
		return false
	}
	currentNodes := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		currentNodes[node.ServerID] = true
	}
	for _, slot := range intent.Slots {
		if slot.Source != PlacementSourceExistingReplica || slot.ServerID == "" || slot.ReplicaID == "" {
			return false
		}
		if !currentNodes[slot.ServerID] {
			return false
		}
	}
	return true
}
