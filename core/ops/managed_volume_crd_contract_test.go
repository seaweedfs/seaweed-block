package ops

import "testing"

func TestManagedVolumeCRDContract_StatusOnlyResources(t *testing.T) {
	contract := ManagedVolumeCRDContractDefinition()
	if contract.Group != "block.seaweedfs.com" || contract.Version != "v1alpha1" {
		t.Fatalf("contract group/version=%+v", contract)
	}
	if !contract.ReadOnly {
		t.Fatalf("CRD contract must be read-only: %+v", contract)
	}
	if contract.RBAC.MutatingStorageVerbsAllowed {
		t.Fatalf("CRD contract exposes storage mutation: %+v", contract.RBAC)
	}
	for _, want := range []string{"get", "list", "watch", "update_status", "patch_status", "create_event"} {
		if !stringSliceContains(contract.RBAC.AllowedVerbs, want) {
			t.Fatalf("missing allowed verb %s in %+v", want, contract.RBAC.AllowedVerbs)
		}
	}
	for _, forbidden := range []string{"promote", "repair", "rebuild", "failback", "delete_storage", "cleanup_live_state"} {
		if !stringSliceContains(contract.RBAC.ForbiddenActions, forbidden) {
			t.Fatalf("missing forbidden action %s in %+v", forbidden, contract.RBAC.ForbiddenActions)
		}
	}

	kinds := map[string]ManagedVolumeCRDKind{}
	for _, resource := range contract.Resources {
		kinds[resource.Kind] = resource
	}
	for _, kind := range []string{SwBlockClusterKind, SwBlockVolumeKind} {
		resource, ok := kinds[kind]
		if !ok {
			t.Fatalf("missing resource %s", kind)
		}
		if resource.StatusFrom == "" || len(resource.StatusPaths) == 0 {
			t.Fatalf("resource %s missing status contract: %+v", kind, resource)
		}
		for _, want := range []string{"status.conditions", "status.observedAt", "status.evidenceRefs"} {
			if !stringSliceContains(resource.StatusPaths, want) {
				t.Fatalf("resource %s missing status path %s in %+v", kind, want, resource.StatusPaths)
			}
		}
		if !stringSliceContains(resource.NonClaims, "no_mutating_storage_actions") &&
			!stringSliceContains(resource.NonClaims, "no_promote_repair_rebuild_delete") {
			t.Fatalf("resource %s missing mutating-action non-claim: %+v", kind, resource.NonClaims)
		}
	}
}

func TestManagedVolumeCRDContract_ConditionVocabularyCoversProjection(t *testing.T) {
	contract := ManagedVolumeCRDContractDefinition()
	for _, condition := range []string{
		ConditionReady,
		ConditionRecovered,
		ConditionRecovering,
		ConditionBlocked,
		ConditionInvalid,
		ConditionCleanupRequired,
		ConditionEvidenceStale,
	} {
		if !stringSliceContains(contract.Conditions, condition) {
			t.Fatalf("missing condition %s in %+v", condition, contract.Conditions)
		}
	}

	cases := []ManagedVolumeFacts{
		{
			VolumeID: "pvc-ready",
			PVC:      &PVCFact{Phase: "Bound"},
			Authority: &AuthorityFact{
				PrimaryReplica: "r1",
				PublishTarget:  "127.0.0.1:3260",
			},
			CSIStages: []CSIStageFact{{NodeName: "m02", Target: "127.0.0.1:3260"}},
			Workload:  &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true},
		},
		{
			VolumeID: "pvc-blocked",
			PVC:      &PVCFact{Phase: "Bound"},
			Authority: &AuthorityFact{
				PrimaryReplica: "r1",
				PublishTarget:  "127.0.0.1:3260",
			},
			Replicas:  []ReplicaFact{{ReplicaID: "r1", KubernetesNode: "m01", Role: "primary", Observed: true}},
			CSIStages: []CSIStageFact{{NodeName: "m02", Target: "127.0.0.1:3260"}},
		},
		{
			VolumeID: "pvc-recovered",
			PVC:      &PVCFact{Phase: "Bound"},
			Authority: &AuthorityFact{
				PrimaryReplica:  "r2",
				PreviousPrimary: "r1",
				PublishTarget:   "192.168.1.184:3261",
			},
			HostPaths: []HostPathFact{{
				Protocol:       "iscsi",
				State:          HostPathStateActiveOptimized,
				MultipathReady: true,
				StaleFenced:    true,
			}},
			Workload: &WorkloadCheckFact{WriterVerified: true, ReaderVerified: true, SamePodUID: true},
		},
		{
			VolumeID:            "pvc-stale",
			EvidenceStale:       true,
			EvidenceStaleReason: ReasonEvidenceStale,
			EvidenceRefs:        []string{"product/unreachable.txt"},
			ProductStatus:       ObservationStatusUnavailable,
			ProductReason:       ReasonEvidenceStale,
		},
	}

	for _, facts := range cases {
		projection := ProjectManagedVolume(facts)
		for _, condition := range projection.Conditions {
			if !stringSliceContains(contract.Conditions, condition.Type) {
				t.Fatalf("projection emitted condition outside CRD contract: %s from %+v", condition.Type, projection)
			}
		}
	}
}

func TestManagedVolumeCRDContract_EventRulesMatchOperatorEvents(t *testing.T) {
	contract := ManagedVolumeCRDContractDefinition()
	rules := map[string]string{}
	for _, rule := range contract.EventRules {
		rules[rule.ConditionSeverity] = rule.KubernetesType
	}
	for severity, want := range map[string]string{
		"info":    "Normal",
		"warning": "Warning",
		"error":   "Warning",
	} {
		if got := rules[severity]; got != want {
			t.Fatalf("severity %s event type=%s want %s", severity, got, want)
		}
	}

	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID: "pvc-blocked",
		PVC:      &PVCFact{Phase: "Pending"},
	})
	operatorContract := ManagedVolumeOperatorContractFromProjection(projection)
	for _, event := range operatorContract.Events {
		if event.Type != "Warning" {
			t.Fatalf("blocked projection should produce warning event: %+v", operatorContract.Events)
		}
	}
}

func TestManagedVolumeCRDContract_EvidenceStaleEventIsWarning(t *testing.T) {
	projection := ProjectManagedVolume(ManagedVolumeFacts{
		VolumeID:            "pvc-stale",
		EvidenceStale:       true,
		EvidenceStaleReason: ReasonEvidenceStale,
		EvidenceRefs:        []string{"product/unreachable.txt"},
	})
	contract := ManagedVolumeOperatorContractFromProjection(projection)
	found := false
	for _, event := range contract.Events {
		if event.Reason == ReasonEvidenceStale && event.Type == "Warning" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing EvidenceStale warning event: %+v", contract.Events)
	}
}
