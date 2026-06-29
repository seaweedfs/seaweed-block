package ops

const (
	SwBlockVolumeAPIVersion            = "block.seaweedfs.com/v1alpha1"
	SwBlockVolumeKind                  = "SwBlockVolume"
	SwBlockClusterKind                 = "SwBlockCluster"
	SwBlockReplicaEligibilityKind      = "SwBlockReplicaEligibility"
	SwBlockReplicaEligibilityPlural    = "swblockreplicaeligibilities"
	SwBlockReplicaEligibilitySingular  = "swblockreplicaeligibility"
	SwBlockReplicaRebuildKind          = "SwBlockReplicaRebuild"
	SwBlockReplicaRebuildPlural        = "swblockreplicarebuilds"
	SwBlockReplicaRebuildSingular      = "swblockreplicarebuild"
	SwBlockFrontendPublicationKind     = "SwBlockFrontendPublication"
	SwBlockFrontendPublicationPlural   = "swblockfrontendpublications"
	SwBlockFrontendPublicationSingular = "swblockfrontendpublication"
	SwBlockReplicaFailbackKind         = "SwBlockReplicaFailback"
	SwBlockReplicaFailbackPlural       = "swblockreplicafailbacks"
	SwBlockReplicaFailbackSingular     = "swblockreplicafailback"

	ConditionReady           = "Ready"
	ConditionRecovered       = "Recovered"
	ConditionRecovering      = "Recovering"
	ConditionBlocked         = "Blocked"
	ConditionInvalid         = "Invalid"
	ConditionCleanupRequired = "CleanupRequired"
	ConditionEvidenceStale   = "EvidenceStale"
)

type ManagedVolumeCRDContract struct {
	Group      string                   `json:"group"`
	Version    string                   `json:"version"`
	ReadOnly   bool                     `json:"read_only"`
	RBAC       ManagedVolumeCRDRBAC     `json:"rbac"`
	Resources  []ManagedVolumeCRDKind   `json:"resources"`
	Conditions []string                 `json:"conditions"`
	EventRules []ManagedVolumeEventRule `json:"event_rules"`
}

type ManagedVolumeCRDRBAC struct {
	AllowedVerbs                []string `json:"allowed_verbs"`
	ForbiddenActions            []string `json:"forbidden_actions"`
	MutatingStorageVerbsAllowed bool     `json:"mutating_storage_verbs_allowed"`
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
		Group:    "block.seaweedfs.com",
		Version:  "v1alpha1",
		ReadOnly: true,
		RBAC: ManagedVolumeCRDRBAC{
			AllowedVerbs: []string{
				"get",
				"list",
				"watch",
				"update_status",
				"patch_status",
				"create_event",
			},
			ForbiddenActions: []string{
				"promote",
				"repair",
				"rebuild",
				"failback",
				"create_pvc",
				"delete_pvc",
				"delete_storage",
				"delete_iscsi_session",
				"cleanup_live_state",
			},
			MutatingStorageVerbsAllowed: false,
		},
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
				"status.observedAt",
				"status.nodeCount",
				"status.nodes",
				"status.volumeCount",
				"status.readyVolumeCount",
				"status.blockedVolumeCount",
				"status.staleVolumeCount",
				"status.observedGeneration",
				"status.evidenceRefs",
				"status.supportBundleRefs",
				"status.cleanup",
				"status.safeNextSteps",
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
				"status.observedAt",
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
			ConditionEvidenceStale,
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
