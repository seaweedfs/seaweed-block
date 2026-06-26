package ops

import "testing"

func TestReturnedReplicaExecutorPreflight_ReadyFromFencedFrontier(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(*ManagedVolumeFacts) {})

	preflights := ReturnedReplicaExecutorPreflights(projection)
	if len(preflights) != 1 {
		t.Fatalf("preflights=%+v", preflights)
	}
	preflight := preflights[0]
	if preflight.Decision != ReturnedReplicaExecutorPreflightReady || preflight.Reason != ReturnedReplicaExecutorPreflightReasonSatisfied {
		t.Fatalf("preflight=%+v", preflight)
	}
	if preflight.ReplicaID != "r1" || preflight.MutationAllowed {
		t.Fatalf("preflight must target r1 without mutation permission: %+v", preflight)
	}
	if preflight.RequiredFrontierLSN != 52 || preflight.DurableFrontierLSN != 52 {
		t.Fatalf("frontiers=%+v", preflight)
	}
}

func TestReturnedReplicaExecutorPreflight_HoldsWhenActionRejected(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Authority.RequiredFrontierKnown = false
	})

	preflight := onlyReturnedReplicaPreflight(t, projection)
	if preflight.Decision != ReturnedReplicaExecutorPreflightHold || preflight.Reason != ManagedVolumeActionRejectMissingFacts {
		t.Fatalf("preflight=%+v", preflight)
	}
	if preflight.MutationAllowed {
		t.Fatalf("hold must stay non-mutating: %+v", preflight)
	}
}

func TestReturnedReplicaExecutorPreflight_HoldsUnsafeFrontend(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Replicas[0].FrontendPrimaryReady = true
	})

	preflight := onlyReturnedReplicaPreflight(t, projection)
	if preflight.Decision != ReturnedReplicaExecutorPreflightHold || preflight.Reason != ReturnedReplicaExecutorPreflightReasonFrontendNotFenced {
		t.Fatalf("preflight=%+v", preflight)
	}
}

func TestReturnedReplicaExecutorPreflight_HoldsUnknownAckEligibility(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Replicas[0].AckEligibilityKnown = false
	})

	preflight := onlyReturnedReplicaPreflight(t, projection)
	if preflight.Decision != ReturnedReplicaExecutorPreflightHold || preflight.Reason != ReturnedReplicaExecutorPreflightReasonAckEligibilityUnknown {
		t.Fatalf("preflight=%+v", preflight)
	}
	if preflight.AckEligible || preflight.AckEligibilityKnown {
		t.Fatalf("unknown ACK eligibility must not be treated as a known false fact: %+v", preflight)
	}
	action := findManagedVolumeAction(projection.Actions, ManagedVolumeActionReintegrateReturned)
	if action == nil || action.Decision != ManagedVolumeActionDecisionAllowed {
		t.Fatalf("dry-run action should remain visible while executor preflight holds: %+v", projection.Actions)
	}
}

func TestReturnedReplicaExecutorPreflight_RebuildReadyWhenFrontierBehind(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Replicas[0].DurableFrontierLSN = 51
	})

	preflight := onlyReturnedReplicaPreflight(t, projection)
	if preflight.ActionType != ManagedVolumeActionRebuildReturned {
		t.Fatalf("action_type=%s want %s", preflight.ActionType, ManagedVolumeActionRebuildReturned)
	}
	if preflight.Decision != ReturnedReplicaExecutorPreflightReady || preflight.Reason != ReturnedReplicaExecutorPreflightReasonSatisfied {
		t.Fatalf("preflight=%+v", preflight)
	}
	if preflight.DurableFrontierLSN != 51 || preflight.RequiredFrontierLSN != 52 {
		t.Fatalf("frontiers=%+v", preflight)
	}
	if preflight.MutationAllowed {
		t.Fatalf("rebuild preflight must stay non-mutating: %+v", preflight)
	}
}

func TestReturnedReplicaExecutorPreflight_RebuildHoldsWhenDurableFrontierMissing(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Replicas[0].DurableFrontierKnown = false
		facts.Replicas[0].DurableFrontierLSN = 0
	})

	preflight := onlyReturnedReplicaPreflight(t, projection)
	if preflight.ActionType != ManagedVolumeActionRebuildReturned ||
		preflight.Decision != ReturnedReplicaExecutorPreflightHold ||
		preflight.Reason != ReturnedReplicaExecutorPreflightReasonDurableMissing {
		t.Fatalf("preflight=%+v", preflight)
	}
}

func TestReturnedReplicaExecutorPreflight_FailbackReadyAfterAckEligibility(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Replicas[0].AckEligible = true
	})

	preflight := onlyReturnedReplicaPreflight(t, projection)
	if preflight.ActionType != ManagedVolumeActionFailbackReturned {
		t.Fatalf("action_type=%s want %s", preflight.ActionType, ManagedVolumeActionFailbackReturned)
	}
	if preflight.Decision != ReturnedReplicaExecutorPreflightReady ||
		preflight.Reason != ReturnedReplicaExecutorPreflightReasonSatisfied ||
		preflight.MutationAllowed {
		t.Fatalf("failback preflight=%+v", preflight)
	}
	if !preflight.AckEligibilityKnown || !preflight.AckEligible || !preflight.FrontendFenced {
		t.Fatalf("failback preflight missing ACK/fencing facts: %+v", preflight)
	}
}

func TestReturnedReplicaExecutorPreflight_HoldsAmbiguousReturnedReplicas(t *testing.T) {
	projection := returnedReplicaPreflightProjection(t, func(facts *ManagedVolumeFacts) {
		facts.Authority.PreviousPrimary = ""
		facts.Replicas = append(facts.Replicas, ReplicaFact{
			ReplicaID:            "r3",
			Observed:             true,
			Role:                 "stale",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendPrimaryReady: false,
			AckEligibilityKnown:  true,
			AckEligible:          false,
			StalePrimaryFenced:   true,
		})
	})
	for i := range projection.Actions {
		if projection.Actions[i].Type == ManagedVolumeActionReintegrateReturned {
			projection.Actions[i].Target = ""
		}
	}

	preflight := onlyReturnedReplicaPreflight(t, projection)
	if preflight.Decision != ReturnedReplicaExecutorPreflightHold || preflight.Reason != ReturnedReplicaExecutorPreflightReasonAmbiguousReplica {
		t.Fatalf("preflight=%+v", preflight)
	}
}

func returnedReplicaPreflightProjection(t *testing.T, mutate func(*ManagedVolumeFacts)) ManagedVolumeProjection {
	t.Helper()
	facts := ManagedVolumeFacts{
		VolumeID: "pvc-returned",
		PVC:      &PVCFact{Phase: "Bound"},
		Authority: &AuthorityFact{
			PrimaryReplica:        "r2",
			PreviousPrimary:       "r1",
			RequiredFrontierKnown: true,
			RequiredFrontierLSN:   52,
		},
		Replicas: []ReplicaFact{{
			ReplicaID:            "r1",
			Observed:             true,
			Role:                 "previous_primary",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
			FrontendPrimaryReady: false,
			AckEligibilityKnown:  true,
			AckEligible:          false,
			StalePrimaryFenced:   true,
		}, {
			ReplicaID:            "r2",
			Observed:             true,
			Role:                 "primary",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   52,
		}},
		EvidenceRefs: []string{"returned-replica-summary.txt"},
	}
	mutate(&facts)
	return ProjectManagedVolume(facts)
}

func onlyReturnedReplicaPreflight(t *testing.T, projection ManagedVolumeProjection) ReturnedReplicaExecutorPreflight {
	t.Helper()
	preflights := ReturnedReplicaExecutorPreflights(projection)
	if len(preflights) != 1 {
		t.Fatalf("preflights=%+v", preflights)
	}
	return preflights[0]
}
