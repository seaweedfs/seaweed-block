package ops

import "testing"

func TestReturnedReplicaExecutorContract_DisabledWhenPreflightReady(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(*ManagedVolumeFacts) {})

	contract := onlyReturnedReplicaExecutorContract(t, projection)
	if contract.Decision != ReturnedReplicaExecutorContractDisabled ||
		contract.Reason != ReturnedReplicaExecutorContractReasonExecutorDisabled ||
		contract.ExecutionEnabled ||
		contract.MutationAllowed {
		t.Fatalf("executor contract=%+v", contract)
	}
	if contract.PreflightDecision != ReturnedReplicaExecutorPreflightReady ||
		contract.PreflightReason != ReturnedReplicaExecutorPreflightReasonSatisfied {
		t.Fatalf("preflight handoff=%+v", contract)
	}
	if !containsActionFact(contract.AllowedMutationClass, "ack_eligibility") {
		t.Fatalf("allowed mutation classes=%+v", contract.AllowedMutationClass)
	}
	for _, want := range []string{"frontend_publication", "rebuild_traffic", "failback"} {
		if !containsActionFact(contract.ForbiddenMutationClass, want) {
			t.Fatalf("forbidden mutation classes missing %s: %+v", want, contract.ForbiddenMutationClass)
		}
	}
	for _, want := range []string{
		"ack_eligibility_known",
		"ack_eligible_true",
		"frontend_fenced_after_execution",
		"primary_unchanged",
		"durable_frontier_covered",
		"no_cross_volume_identity_change",
	} {
		if !containsActionFact(contract.TerminalEvidenceRequired, want) {
			t.Fatalf("terminal evidence missing %s: %+v", want, contract.TerminalEvidenceRequired)
		}
	}
}

func TestReturnedReplicaExecutorContract_BlockedWhenPreflightHolds(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Replicas[0].AckEligibilityKnown = false
	})

	contract := onlyReturnedReplicaExecutorContract(t, projection)
	if contract.Decision != ReturnedReplicaExecutorContractBlocked ||
		contract.Reason != ReturnedReplicaExecutorContractReasonPreflightNotReady ||
		contract.PreflightReason != ReturnedReplicaExecutorPreflightReasonAckEligibilityUnknown ||
		contract.ExecutionEnabled ||
		contract.MutationAllowed {
		t.Fatalf("executor contract=%+v", contract)
	}
	if len(contract.AllowedMutationClass) != 0 {
		t.Fatalf("blocked contract must not allow mutation classes: %+v", contract.AllowedMutationClass)
	}
	if !containsActionFact(contract.ForbiddenMutationClass, "ack_eligibility") {
		t.Fatalf("blocked contract should carry preflight forbidden facts: %+v", contract.ForbiddenMutationClass)
	}
}

func TestReturnedReplicaExecutorContract_DisablesRebuildTrafficWhenFrontierBehind(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Replicas[0].DurableFrontierLSN = 51
	})

	contract := onlyReturnedReplicaExecutorContract(t, projection)
	if contract.ActionType != ManagedVolumeActionRebuildReturned {
		t.Fatalf("action_type=%s want %s", contract.ActionType, ManagedVolumeActionRebuildReturned)
	}
	if contract.Decision != ReturnedReplicaExecutorContractDisabled ||
		contract.Reason != ReturnedReplicaExecutorContractReasonExecutorDisabled ||
		contract.ExecutionEnabled ||
		contract.MutationAllowed {
		t.Fatalf("executor contract=%+v", contract)
	}
	if !containsActionFact(contract.AllowedMutationClass, "rebuild_traffic") {
		t.Fatalf("rebuild contract should name rebuild_traffic as future envelope: %+v", contract.AllowedMutationClass)
	}
	for _, want := range []string{"ack_eligibility", "frontend_publication", "failback"} {
		if !containsActionFact(contract.ForbiddenMutationClass, want) {
			t.Fatalf("forbidden mutation classes missing %s: %+v", want, contract.ForbiddenMutationClass)
		}
	}
	for _, want := range []string{
		"frontend_fenced_before_rebuild",
		"primary_unchanged",
		"durable_frontier_caught_up",
		"no_frontend_publication",
		"no_cross_volume_identity_change",
	} {
		if !containsActionFact(contract.TerminalEvidenceRequired, want) {
			t.Fatalf("terminal evidence missing %s: %+v", want, contract.TerminalEvidenceRequired)
		}
	}
}

func onlyReturnedReplicaExecutorContract(t *testing.T, projection ManagedVolumeProjection) ReturnedReplicaExecutorContract {
	t.Helper()
	contracts := ReturnedReplicaExecutorContracts(projection)
	if len(contracts) != 1 {
		t.Fatalf("executor contracts=%+v", contracts)
	}
	return contracts[0]
}
