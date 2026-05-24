package ops

const (
	SwBlockVolumeAPIVersion = "block.seaweedfs.com/v1alpha1"
	SwBlockVolumeKind       = "SwBlockVolume"
	SwBlockClusterKind      = "SwBlockCluster"

	ConditionReady           = "Ready"
	ConditionRecovered       = "Recovered"
	ConditionRecovering      = "Recovering"
	ConditionBlocked         = "Blocked"
	ConditionInvalid         = "Invalid"
	ConditionCleanupRequired = "CleanupRequired"
)

type ManagedVolumeCRDContract struct {
	Group      string                 `json:"group"`
	Version    string                 `json:"version"`
	Resources  []ManagedVolumeCRDKind `json:"resources"`
	Conditions []string               `json:"conditions"`
	EventRules []ManagedVolumeEventRule `json:"event_rules"`
}

type ManagedVolumeCRDKind struct {
	Kind        string   `json:"kind"`
	Scope       string   `json:"scope"`
	StatusFrom  string   `json:"status_from"`
	SpecFields  []string `json:"spec_fields,omitempty"`
	StatusPaths []string `json:"status_paths"`
	NonClaims   []string `json:"non_claims,omitempty"`
}

type ManagedVolumeEventRule struct {
	ConditionSeverity string `json:"condition_severity"`
	KubernetesType    string `json:"kubernetes_type"`
}

func ManagedVolumeCRDContractDefinition() ManagedVolumeCRDContract {
	return ManagedVolumeCRDContract{
		Group:   "block.seaweedfs.com",
		Version: "v1alpha1",
		Resources: []ManagedVolumeCRDKind{{
			Kind:       SwBlockClusterKind,
			Scope:      "Namespaced",
			StatusFrom: "ManagedVolumeProjection aggregate",
			SpecFields: []string{
				"image",
				"csiImage",
				"storageClass",
				"blockNodes",
				"ackProfile",
				"protocol",
			},
			StatusPaths: []string{
				"status.conditions",
				"status.nodeCount",
				"status.volumeCount",
				"status.readyVolumeCount",
				"status.blockedVolumeCount",
				"status.observedGeneration",
			},
			NonClaims: []string{
				"no_mutating_storage_actions",
				"no_repair_rebuild_failback",
				"no_backup_restore",
			},
		}, {
			Kind:       SwBlockVolumeKind,
			Scope:      "Namespaced",
			StatusFrom: "ManagedVolumeProjection",
			SpecFields: []string{
				"pvcName",
				"storageClass",
			},
			StatusPaths: []string{
				"status.volumeID",
				"status.pvcName",
				"status.status",
				"status.reasonCode",
				"status.conditions",
				"status.nonClaims",
				"status.evidenceRefs",
				"status.allowedActions",
			},
			NonClaims: []string{
				"status_only",
				"no_primary_selection",
				"no_promote_repair_rebuild_delete",
			},
		}},
		Conditions: []string{
			ConditionReady,
			ConditionRecovered,
			ConditionRecovering,
			ConditionBlocked,
			ConditionInvalid,
			ConditionCleanupRequired,
		},
		EventRules: []ManagedVolumeEventRule{{
			ConditionSeverity: "info",
			KubernetesType:    "Normal",
		}, {
			ConditionSeverity: "warning",
			KubernetesType:    "Warning",
		}, {
			ConditionSeverity: "error",
			KubernetesType:    "Warning",
		}},
	}
}
