package ops

import "testing"

func TestManagedVolumeFactContract_CriticalFieldsHaveAuthorities(t *testing.T) {
	entries := ManagedVolumeFactContract()
	required := map[string]string{
		"identity.volume_id":              FactAuthorityKubernetesObject,
		"identity.pvc_name":               FactAuthorityKubernetesObject,
		"desired.replication_factor":      FactAuthorityKubernetesObject,
		"desired.ack_profile":             FactAuthorityKubernetesObject,
		"desired.claim_profile":           FactAuthorityObservation,
		"desired.protocol":                FactAuthorityKubernetesObject,
		"authority.primary_replica":       FactAuthorityAuthorityLine,
		"authority.publish_target":        FactAuthorityAuthorityLine,
		"authority.endpoint_version":      FactAuthorityAuthorityLine,
		"replica.durable_frontier_lsn":    FactAuthorityReplicaDurability,
		"csi.staged_target":               FactAuthorityCSIAttach,
		"host_path.rtpg_aas":              FactAuthorityHostPath,
		"host_path.stale_path_probe":      FactAuthorityHostPath,
		"workload.reader_verified":        FactAuthorityWorkloadEvidence,
		"cleanup.status":                  FactAuthorityCleanup,
		"cleanup.k8s_residue_count":       FactAuthorityCleanup,
		"cleanup.iscsi_residue_count":     FactAuthorityCleanup,
		"cleanup.multipath_residue_count": FactAuthorityCleanup,
		"cleanup.process_residue_count":   FactAuthorityCleanup,
		"cleanup.hostpath_residue_count":  FactAuthorityCleanup,
		"cleanup.failure_count":           FactAuthorityCleanup,
		"evidence.reason_code":            FactAuthorityObservation,
	}

	byPath := managedVolumeContractByPath(entries)
	for path, authority := range required {
		entry, ok := byPath[path]
		if !ok {
			t.Fatalf("missing contract path %q", path)
		}
		if entry.FactAuthority != authority {
			t.Fatalf("path %s authority=%s want %s", path, entry.FactAuthority, authority)
		}
		if entry.Master == "" {
			t.Fatalf("path %s missing master", path)
		}
		if entry.EvidenceRequired == "" {
			t.Fatalf("path %s missing evidence requirement", path)
		}
	}
}

func TestManagedVolumeFactContract_FieldRolesStayReadOnly(t *testing.T) {
	for _, entry := range ManagedVolumeFactContract() {
		if entry.Path == "" {
			t.Fatalf("contract entry missing path: %+v", entry)
		}
		if entry.FactAuthority == "" || entry.Master == "" || entry.Participant == "" {
			t.Fatalf("field %s missing layered role: %+v", entry.Path, entry)
		}
		if entry.EvidenceRequired == "" {
			t.Fatalf("field %s missing evidence requirement", entry.Path)
		}
	}
}

func TestManagedVolumeFactContract_DualModeOnlyForDecisionBoundaries(t *testing.T) {
	byPath := managedVolumeContractByPath(ManagedVolumeFactContract())

	for _, path := range []string{
		"authority.primary_replica",
		"replica.durable_frontier_lsn",
		"csi.staged_target",
		"host_path.rtpg_aas",
	} {
		entry := byPath[path]
		if entry.AggregationMode != FactAggregationDual {
			t.Fatalf("path %s aggregation=%s want %s", path, entry.AggregationMode, FactAggregationDual)
		}
		if !entry.ProbeAllowed || entry.ProbeTrigger == "" {
			t.Fatalf("path %s must allow bounded probe with trigger: %+v", path, entry)
		}
	}

	for _, path := range []string{
		"identity.pvc_name",
		"placement.replica_node",
		"evidence.reason_code",
	} {
		entry := byPath[path]
		if entry.AggregationMode != FactAggregationPassive {
			t.Fatalf("path %s aggregation=%s want passive", path, entry.AggregationMode)
		}
		if entry.ProbeAllowed {
			t.Fatalf("path %s should not need active probe: %+v", path, entry)
		}
	}
}

func TestManagedVolumeFactContract_StableFieldsAreOperatorSafe(t *testing.T) {
	for _, entry := range ManagedVolumeFactContract() {
		if entry.Stability != ManagedVolumeFieldStable {
			continue
		}
		if entry.ConditionSurface == "" {
			t.Fatalf("stable path %s missing condition surface", entry.Path)
		}
		if entry.FactAuthority == "" || entry.Participant == "" || entry.Master == "" {
			t.Fatalf("stable path %s incomplete role contract: %+v", entry.Path, entry)
		}
		switch entry.AggregationMode {
		case FactAggregationPassive:
			if entry.ProbeTrigger != "" {
				t.Fatalf("passive path %s should not define probe trigger: %+v", entry.Path, entry)
			}
		case FactAggregationProbe, FactAggregationDual:
			if !entry.ProbeAllowed || entry.ProbeTrigger == "" {
				t.Fatalf("probe-capable path %s missing bounded probe contract: %+v", entry.Path, entry)
			}
		default:
			t.Fatalf("path %s unknown aggregation mode %q", entry.Path, entry.AggregationMode)
		}
	}
}

func TestManagedVolumeActionContract_AllProjectedActionsHaveExecutors(t *testing.T) {
	contract := managedVolumeActionContractByType(ManagedVolumeActionContract())

	projections := []ManagedVolumeProjection{
		ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID: "pvc-ready",
			PVC:      &PVCFact{Phase: "Bound"},
			Authority: &AuthorityFact{
				PrimaryReplica: "r1",
				PublishTarget:  "192.168.1.181:3260",
			},
			Replicas: []ReplicaFact{{
				ReplicaID:      "r1",
				KubernetesNode: "m01",
				Role:           "primary",
				Observed:       true,
			}},
			CSIStages: []CSIStageFact{{NodeName: "m01", Target: "192.168.1.181:3260"}},
			Workload:  &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
		}),
		ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID: "pvc-loopback",
			PVCName:  "demo-pvc",
			PVC:      &PVCFact{Phase: "Bound"},
			Authority: &AuthorityFact{
				PrimaryReplica: "r1",
				PublishTarget:  "127.0.0.1:3260",
			},
			Replicas: []ReplicaFact{{
				ReplicaID:      "r1",
				KubernetesNode: "m01",
				Role:           "primary",
				Observed:       true,
			}},
			CSIStages: []CSIStageFact{{NodeName: "m02", Target: "127.0.0.1:3260"}},
		}),
		ProjectManagedVolume(ManagedVolumeFacts{
			VolumeID: "pvc-pending",
			PVCName:  "pending-pvc",
			PVC:      &PVCFact{Phase: "Pending"},
		}),
	}

	for _, projection := range projections {
		for _, action := range projection.Actions {
			entry, ok := contract[action.Type]
			if !ok {
				t.Fatalf("action %s missing action contract", action.Type)
			}
			if entry.Master == "" || entry.OwnerExecutor == "" {
				t.Fatalf("action %s missing master/executor: %+v", action.Type, entry)
			}
			if entry.MutationAllowed {
				t.Fatalf("phase30 must not expose mutating actions: %+v", entry)
			}
			if action.Mode != entry.Mode || action.SideEffectClass != entry.SideEffectClass || action.OwnerExecutor != entry.OwnerExecutor {
				t.Fatalf("action %s projection boundary=%+v contract=%+v", action.Type, action, entry)
			}
		}
	}
}

func TestManagedVolumeActionContract_Phase30NoExecutableMutation(t *testing.T) {
	for _, entry := range ManagedVolumeActionContract() {
		if entry.Type == "" || entry.Master == "" || entry.OwnerExecutor == "" {
			t.Fatalf("action contract missing role boundary: %+v", entry)
		}
		if entry.EvidenceRequired == "" {
			t.Fatalf("action %s missing evidence requirement", entry.Type)
		}
		if len(entry.RequiredFacts) == 0 {
			t.Fatalf("action %s missing required facts", entry.Type)
		}
		if entry.Mode != ManagedVolumeActionModeReadOnly && entry.Mode != ManagedVolumeActionModeDryRun {
			t.Fatalf("action %s exposes executable mode: %+v", entry.Type, entry)
		}
		if entry.MutationAllowed {
			t.Fatalf("phase30 action must not allow mutation: %+v", entry)
		}
		if entry.SideEffectClass != ManagedVolumeSideEffectObserve && entry.PolicyGate != ActionPolicyDryRun && entry.PolicyGate != ActionPolicyDisabled {
			t.Fatalf("non-observe action %s must be dry-run or disabled: %+v", entry.Type, entry)
		}
	}
}

func managedVolumeContractByPath(entries []ManagedVolumeFactContractEntry) map[string]ManagedVolumeFactContractEntry {
	out := make(map[string]ManagedVolumeFactContractEntry, len(entries))
	for _, entry := range entries {
		out[entry.Path] = entry
	}
	return out
}

func managedVolumeActionContractByType(entries []ManagedVolumeActionContractEntry) map[string]ManagedVolumeActionContractEntry {
	out := make(map[string]ManagedVolumeActionContractEntry, len(entries))
	for _, entry := range entries {
		out[entry.Type] = entry
	}
	return out
}
