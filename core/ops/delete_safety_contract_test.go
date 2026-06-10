package ops

import "testing"

func TestSwBlockVolumeDeleteSafetyContract_BoundsFinalizerOwnership(t *testing.T) {
	contract := SwBlockVolumeDeleteSafetyContractDefinition()
	if contract.FinalizerName != SwBlockVolumeFinalizerName {
		t.Fatalf("finalizer=%s", contract.FinalizerName)
	}
	if contract.OwnedKind != SwBlockVolumeKind {
		t.Fatalf("owned kind=%s", contract.OwnedKind)
	}
	if contract.ReleaseActionType != SwBlockVolumeDeleteActionReleaseFinalizer {
		t.Fatalf("release action=%s", contract.ReleaseActionType)
	}
	for _, want := range []string{
		"swblockvolumes.metadata.finalizers",
		"swblockvolumes/status",
		"events",
	} {
		if !stringSliceContains(contract.OwnedMutationScope, want) {
			t.Fatalf("mutation scope missing %s: %+v", want, contract.OwnedMutationScope)
		}
	}
	for _, forbidden := range []string{
		"persistentvolumeclaims",
		"persistentvolumes",
		"pods",
		"deployments",
		"storageclasses",
		"iscsi",
		"multipath",
		"hostpath",
	} {
		if stringSliceContains(contract.OwnedMutationScope, forbidden) {
			t.Fatalf("mutation scope includes forbidden target %s: %+v", forbidden, contract.OwnedMutationScope)
		}
	}
	for _, want := range []string{
		"identity.volume_id",
		"identity.pvc_name",
		"identity.pv_name",
		"kubernetes.swblockvolume.deletion_timestamp",
		"cleanup.status",
		"cleanup.iscsi_residue_count",
		"cleanup.multipath_residue_count",
		"cleanup.hostpath_residue_count",
	} {
		if !stringSliceContains(contract.RequiredFacts, want) {
			t.Fatalf("required facts missing %s: %+v", want, contract.RequiredFacts)
		}
	}
	for _, want := range []string{
		DeleteSafetyStateNotRequested,
		DeleteSafetyStateRequested,
		DeleteSafetyStateBlocked,
		DeleteSafetyStateReleasable,
		DeleteSafetyStateReleased,
	} {
		if !stringSliceContains(contract.DeleteStates, want) {
			t.Fatalf("delete states missing %s: %+v", want, contract.DeleteStates)
		}
	}
	for _, want := range []string{
		"no_pvc_finalizer_ownership",
		"no_automatic_cleanup_execution",
		"no_iscsi_or_multipath_mutation",
		"no_hostpath_delete",
	} {
		if !stringSliceContains(contract.NonClaims, want) {
			t.Fatalf("non-claims missing %s: %+v", want, contract.NonClaims)
		}
	}
}

func TestEvaluateSwBlockVolumeDeleteSafety_NotRequested(t *testing.T) {
	decision := EvaluateSwBlockVolumeDeleteSafety(SwBlockVolumeDeleteSafetyFacts{})
	if decision.State != DeleteSafetyStateNotRequested ||
		decision.Decision != ManagedVolumeActionDecisionRejected ||
		decision.Reason != ReasonDeleteNotRequested ||
		decision.FinalizerReleaseAllowed {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestEvaluateSwBlockVolumeDeleteSafety_BlocksWithoutCleanupEvidence(t *testing.T) {
	decision := EvaluateSwBlockVolumeDeleteSafety(SwBlockVolumeDeleteSafetyFacts{
		DeleteRequested:  true,
		FinalizerPresent: true,
	})
	if decision.State != DeleteSafetyStateBlocked ||
		decision.Decision != ManagedVolumeActionDecisionRejected ||
		decision.Reason != ReasonCleanupEvidenceMissing ||
		decision.FinalizerReleaseAllowed ||
		decision.SafeNextAction != ManagedVolumeActionVerifyCleanup {
		t.Fatalf("decision=%+v", decision)
	}
	if !stringSliceContains(decision.MissingFacts, "cleanup.status") {
		t.Fatalf("missing facts=%+v", decision.MissingFacts)
	}
}

func TestEvaluateSwBlockVolumeDeleteSafety_BlocksWithResidue(t *testing.T) {
	decision := EvaluateSwBlockVolumeDeleteSafety(SwBlockVolumeDeleteSafetyFacts{
		DeleteRequested:  true,
		FinalizerPresent: true,
		Cleanup: &CleanupEvidence{
			Status:            ObservationStatusBlocked,
			ISCSIResidueCount: 1,
			ReasonCodes:       []string{"iscsi_node_records_present"},
			EvidenceRef:       "cleanup-summary.txt",
		},
	})
	if decision.State != DeleteSafetyStateBlocked ||
		decision.Decision != ManagedVolumeActionDecisionRejected ||
		decision.Reason != "iscsi_node_records_present" ||
		decision.FinalizerReleaseAllowed ||
		decision.SafeNextAction != ManagedVolumeActionVerifyCleanup {
		t.Fatalf("decision=%+v", decision)
	}
	if !stringSliceContains(decision.EvidenceRefs, "cleanup-summary.txt") {
		t.Fatalf("evidence refs=%+v", decision.EvidenceRefs)
	}
}

func TestEvaluateSwBlockVolumeDeleteSafety_ReleasableWithCleanCleanupEvidence(t *testing.T) {
	decision := EvaluateSwBlockVolumeDeleteSafety(SwBlockVolumeDeleteSafetyFacts{
		DeleteRequested:  true,
		FinalizerPresent: true,
		Cleanup: &CleanupEvidence{
			Status:      ObservationStatusOK,
			EvidenceRef: "cleanup-summary.txt",
		},
	})
	if decision.State != DeleteSafetyStateReleasable ||
		decision.Decision != ManagedVolumeActionDecisionAllowed ||
		decision.Reason != ReasonDeleteFinalizerReleasable ||
		!decision.FinalizerReleaseAllowed ||
		decision.SafeNextAction != "" {
		t.Fatalf("decision=%+v", decision)
	}
	if !stringSliceContains(decision.EvidenceRefs, "cleanup-summary.txt") {
		t.Fatalf("evidence refs=%+v", decision.EvidenceRefs)
	}
}
