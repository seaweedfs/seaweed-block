package ops

import "testing"

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
