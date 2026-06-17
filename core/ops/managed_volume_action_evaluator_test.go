package ops

import (
	"strings"
	"testing"
)

func TestEvaluateManagedVolumeAction_AllowsDryRunWhenFactsPresent(t *testing.T) {
	evaluation := EvaluateManagedVolumeAction(ManagedVolumeActionReinstallExternalISCSI, ManagedVolumeFacts{
		Authority: &AuthorityFact{PublishTarget: "127.0.0.1:3260"},
		Replicas: []ReplicaFact{{
			ReplicaID:      "r1",
			KubernetesNode: "m02",
		}},
	})

	if evaluation.Decision != ManagedVolumeActionDecisionAllowed {
		t.Fatalf("decision=%s reason=%s missing=%v", evaluation.Decision, evaluation.Reason, evaluation.MissingFacts)
	}
	if evaluation.Mode != ManagedVolumeActionModeDryRun {
		t.Fatalf("mode=%s want %s", evaluation.Mode, ManagedVolumeActionModeDryRun)
	}
	if evaluation.SideEffectClass != ManagedVolumeSideEffectSafeK8S {
		t.Fatalf("side_effect_class=%s want %s", evaluation.SideEffectClass, ManagedVolumeSideEffectSafeK8S)
	}
	if evaluation.OwnerExecutor != "installer_or_operator" {
		t.Fatalf("owner_executor=%q", evaluation.OwnerExecutor)
	}
	if evaluation.MutationAllowed {
		t.Fatal("dry-run evaluation must not allow mutation")
	}
	if len(evaluation.InvariantRefs) == 0 || evaluation.EvidenceRequired == "" {
		t.Fatalf("evaluation missing invariant/evidence contract: %+v", evaluation)
	}
}

func TestEvaluateManagedVolumeAction_RejectsMissingRequiredFacts(t *testing.T) {
	evaluation := EvaluateManagedVolumeAction(ManagedVolumeActionReinstallExternalISCSI, ManagedVolumeFacts{
		Authority: &AuthorityFact{PublishTarget: "127.0.0.1:3260"},
	})

	if evaluation.Decision != ManagedVolumeActionDecisionRejected {
		t.Fatalf("decision=%s want rejected", evaluation.Decision)
	}
	if evaluation.Reason != ManagedVolumeActionRejectMissingFacts {
		t.Fatalf("reason=%s want %s", evaluation.Reason, ManagedVolumeActionRejectMissingFacts)
	}
	if len(evaluation.MissingFacts) != 1 || evaluation.MissingFacts[0] != "placement.replica_node" {
		t.Fatalf("missing facts=%v", evaluation.MissingFacts)
	}
	if evaluation.MutationAllowed {
		t.Fatal("rejected evaluation must not allow mutation")
	}
}

func TestEvaluateManagedVolumeAction_RejectsDisabledAuthorityMutation(t *testing.T) {
	evaluation := EvaluateManagedVolumeAction(ManagedVolumeActionRequestPromotion, ManagedVolumeFacts{
		Authority: &AuthorityFact{PrimaryReplica: "r1"},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r2",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   42,
		}},
	})

	if evaluation.Decision != ManagedVolumeActionDecisionRejected {
		t.Fatalf("decision=%s want rejected", evaluation.Decision)
	}
	if evaluation.Reason != ManagedVolumeActionRejectDisabled {
		t.Fatalf("reason=%s want %s", evaluation.Reason, ManagedVolumeActionRejectDisabled)
	}
	if evaluation.SideEffectClass != ManagedVolumeSideEffectAuthorityMutating {
		t.Fatalf("side_effect_class=%s want %s", evaluation.SideEffectClass, ManagedVolumeSideEffectAuthorityMutating)
	}
	if evaluation.MutationAllowed {
		t.Fatal("disabled authority action must not allow mutation")
	}
}

func TestEvaluateManagedVolumeAction_RejectsUnknownAction(t *testing.T) {
	evaluation := EvaluateManagedVolumeAction("delete.everything", ManagedVolumeFacts{})

	if evaluation.Decision != ManagedVolumeActionDecisionRejected {
		t.Fatalf("decision=%s want rejected", evaluation.Decision)
	}
	if evaluation.Reason != ManagedVolumeActionRejectUnknownAction {
		t.Fatalf("reason=%s want %s", evaluation.Reason, ManagedVolumeActionRejectUnknownAction)
	}
}

func TestManagedVolumeActionEvaluator_CoversEveryContractEntry(t *testing.T) {
	for _, entry := range ManagedVolumeActionContract() {
		evaluation := EvaluateManagedVolumeAction(entry.Type, factsSatisfyingActionContract(entry))
		if entry.PolicyGate == ActionPolicyDisabled {
			if evaluation.Decision != ManagedVolumeActionDecisionRejected || evaluation.Reason != ManagedVolumeActionRejectDisabled {
				t.Fatalf("disabled action %s evaluation=%+v", entry.Type, evaluation)
			}
			continue
		}
		if evaluation.Decision != ManagedVolumeActionDecisionAllowed {
			t.Fatalf("action %s should be evaluable with satisfying facts: %+v", entry.Type, evaluation)
		}
		if evaluation.Mode != entry.Mode || evaluation.SideEffectClass != entry.SideEffectClass || evaluation.OwnerExecutor != entry.OwnerExecutor {
			t.Fatalf("action %s evaluation boundary=%+v contract=%+v", entry.Type, evaluation, entry)
		}
	}
}

func TestRenderManagedVolumeActionEvaluationText_ShowsRejectedAction(t *testing.T) {
	text := RenderManagedVolumeActionEvaluationText(EvaluateManagedVolumeAction(ManagedVolumeActionRequestPromotion, ManagedVolumeFacts{
		Authority: &AuthorityFact{PrimaryReplica: "r1"},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r2",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   42,
		}},
	}))

	for _, want := range []string{
		"managed_volume_action_evaluation authority.request_promotion decision=rejected",
		"side_effect=authority_mutating",
		"executor=authority_recovery_executor",
		"reason=policy_disabled",
		"mutation_allowed=false",
		"managed_volume_action_evaluation_evidence_required authority.request_promotion promotion_readiness_evidence",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("rendered evaluation missing %q:\n%s", want, text)
		}
	}
}

func factsSatisfyingActionContract(entry ManagedVolumeActionContractEntry) ManagedVolumeFacts {
	facts := ManagedVolumeFacts{
		VolumeID:      "pvc-action",
		PVCName:       "demo-pvc",
		ProductReason: ReasonPublishTargetLoopbackCrossNode,
		EvidenceRefs:  []string{"action-evidence.txt"},
		PVC:           &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica: "r1",
			PublishTarget:  "127.0.0.1:3260",
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			ServerID:             "m02",
			KubernetesNode:       "m02",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   42,
		}},
		CSIStages: []CSIStageFact{{
			NodeName: "m01",
			Target:   "127.0.0.1:3260",
		}},
		HostPaths: []HostPathFact{{
			Protocol:    "iscsi",
			ALUAState:   "0x00",
			StaleFenced: true,
		}},
	}
	return facts
}
