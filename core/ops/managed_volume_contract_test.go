package ops

import "testing"

func TestManagedVolumeFactContract_CriticalFieldsHaveAuthorities(t *testing.T) {
	entries := ManagedVolumeFactContract()
	required := map[string]string{
		"identity.pvc_name":                FactAuthorityKubernetesObject,
		"authority.primary_replica":        FactAuthorityAuthorityLine,
		"replica.durable_frontier_lsn":     FactAuthorityReplicaDurability,
		"csi.staged_target":               FactAuthorityCSIAttach,
		"host_path.rtpg_aas":              FactAuthorityHostPath,
		"host_path.stale_path_probe":      FactAuthorityHostPath,
		"workload.reader_verified":        FactAuthorityWorkloadEvidence,
		"cleanup.multipath_residue_count": FactAuthorityCleanup,
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

func managedVolumeContractByPath(entries []ManagedVolumeFactContractEntry) map[string]ManagedVolumeFactContractEntry {
	out := make(map[string]ManagedVolumeFactContractEntry, len(entries))
	for _, entry := range entries {
		out[entry.Path] = entry
	}
	return out
}
