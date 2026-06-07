package ops

import "testing"

func TestPhase37D1LiveNodeEvidenceContractCoversRequiredFacts(t *testing.T) {
	byPath := managedVolumeContractByPath(LiveNodeEvidenceFactContract())
	required := map[string]string{
		"node.kubernetes_ready":                   FactAuthorityKubernetesObject,
		"node.scheduling_disabled":                FactAuthorityKubernetesObject,
		"node.csi_node_pod_ready":                 FactAuthorityKubernetesObject,
		"node.csi_driver_exists":                  FactAuthorityKubernetesObject,
		"node.csi_node_driver_registered":         FactAuthorityKubernetesObject,
		"node.required_image_presence":            FactAuthorityObservation,
		"node.image_pull_status":                  FactAuthorityKubernetesObject,
		"node.iscsi_prereq":                       FactAuthorityHostPath,
		"node.multipath_prereq":                   FactAuthorityHostPath,
		"node.loopback_publish_target_cross_node": FactAuthorityObservation,
	}
	for path, authority := range required {
		entry, ok := byPath[path]
		if !ok {
			t.Fatalf("missing live node evidence contract path %q", path)
		}
		if entry.FactAuthority != authority {
			t.Fatalf("path %s authority=%s want %s", path, entry.FactAuthority, authority)
		}
		if entry.Master != MasterManagedVolume {
			t.Fatalf("path %s master=%s want %s", path, entry.Master, MasterManagedVolume)
		}
		if entry.EvidenceRequired == "" || entry.ConditionSurface == "" {
			t.Fatalf("path %s missing evidence/surface contract: %+v", path, entry)
		}
	}
}

func TestPhase37D1LiveNodeEvidenceContractIsReadOnlyPassiveObservation(t *testing.T) {
	for _, entry := range LiveNodeEvidenceFactContract() {
		if entry.Path == "" || entry.Participant == "" || entry.FactAuthority == "" {
			t.Fatalf("incomplete live node evidence entry: %+v", entry)
		}
		if entry.AggregationMode != FactAggregationPassive {
			t.Fatalf("phase37 node evidence must be passive observation, got %+v", entry)
		}
		if entry.ProbeAllowed || entry.ProbeTrigger != "" {
			t.Fatalf("phase37 D1 must not authorize active mutation/probe behavior: %+v", entry)
		}
		switch entry.Stability {
		case ManagedVolumeFieldStable, ManagedVolumeFieldProvisional, ManagedVolumeFieldTestOnly:
		default:
			t.Fatalf("path %s has invalid stability %q", entry.Path, entry.Stability)
		}
	}
}

func TestPhase37D1LiveNodeEvidenceReasonVocabulary(t *testing.T) {
	reasons := LiveNodeEvidenceReasonCodes()
	for _, want := range []string{
		ReasonNodeReady,
		ReasonNodeNotReady,
		ReasonNodeSchedulingDisabled,
		ReasonCSINodePodNotReady,
		ReasonCSIDriverNotRegistered,
		ReasonImageMissingOnNode,
		ReasonISCSIPrereqMissing,
		ReasonMultipathPrereqMissing,
		ReasonPublishTargetLoopbackCrossNode,
	} {
		if !stringSliceContains(reasons, want) {
			t.Fatalf("missing phase37 node reason %s in %+v", want, reasons)
		}
	}
	for _, reason := range reasons {
		if reason == "" {
			t.Fatalf("empty phase37 node reason in %+v", reasons)
		}
	}
}
