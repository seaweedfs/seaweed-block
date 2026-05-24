package ops

const ObservationOperatorSnapshotArtifact = "operator-snapshot.json"

type OperatorFoundationSnapshot struct {
	APIVersion  string                          `json:"api_version"`
	Kind        string                          `json:"kind"`
	ReadOnly    bool                            `json:"read_only"`
	Mutation    OperatorMutationBoundary        `json:"mutation"`
	CRDContract ManagedVolumeCRDContract        `json:"crd_contract"`
	Cluster     OperatorClusterStatus           `json:"cluster"`
	Volumes     []ManagedVolumeOperatorContract `json:"volumes"`
}

type OperatorMutationBoundary struct {
	MutationAllowed bool     `json:"mutation_allowed"`
	AllowedModes    []string `json:"allowed_modes"`
	NonClaims       []string `json:"non_claims"`
}

type OperatorClusterStatus struct {
	Status             string                 `json:"status"`
	VolumeCount        int                    `json:"volume_count"`
	ReadyVolumeCount   int                    `json:"ready_volume_count"`
	BlockedVolumeCount int                    `json:"blocked_volume_count"`
	Cleanup            *CleanupEvidence       `json:"cleanup,omitempty"`
	Conditions         []ObservationCondition `json:"conditions,omitempty"`
	NonClaims          []string               `json:"non_claims,omitempty"`
}

func BuildOperatorFoundationSnapshot(cluster ClusterEvidence) OperatorFoundationSnapshot {
	cluster = NormalizeObservationCluster(cluster)
	snapshot := OperatorFoundationSnapshot{
		APIVersion: "block.seaweedfs.com/v1alpha1",
		Kind:       "ReadOnlyOperatorFoundationSnapshot",
		ReadOnly:   true,
		Mutation: OperatorMutationBoundary{
			MutationAllowed: false,
			AllowedModes: []string{
				ManagedVolumeActionModeReadOnly,
				ManagedVolumeActionModeDryRun,
			},
			NonClaims: []string{
				"no_promote",
				"no_repair",
				"no_rebuild",
				"no_failback",
				"no_delete",
				"no_cleanup_mutation",
			},
		},
		CRDContract: ManagedVolumeCRDContractDefinition(),
		Cluster: OperatorClusterStatus{
			Status:     cluster.Status,
			Cleanup:    cluster.Cleanup,
			Conditions: append([]ObservationCondition(nil), cluster.Conditions...),
			NonClaims:  append([]string(nil), cluster.NonClaims...),
		},
	}

	for _, managed := range cluster.ManagedVolumes {
		contract := ManagedVolumeOperatorContractFromProjection(managed)
		snapshot.Volumes = append(snapshot.Volumes, contract)
		snapshot.Cluster.VolumeCount++
		switch managed.Status {
		case ManagedVolumeStatusReady, ManagedVolumeStatusRecovered:
			snapshot.Cluster.ReadyVolumeCount++
		case ManagedVolumeStatusBlocked, ManagedVolumeStatusInvalid, ManagedVolumeStatusUnsafe:
			snapshot.Cluster.BlockedVolumeCount++
		}
	}
	return snapshot
}
