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
		plan := PlanPlacement(volume, nodes)
		result := ReconcileResult{
			VolumeID: volume.Spec.VolumeID,
			Plan:     plan,
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
