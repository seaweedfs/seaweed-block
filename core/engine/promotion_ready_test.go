package engine

import "testing"

func TestBuildPromotionReadyFact_ReadyOnlyFromTruthDomains(t *testing.T) {
	st := promotionReadyState()
	fact := BuildPromotionReadyFact(st)
	if !fact.Ready {
		t.Fatalf("fact should be ready: %+v", fact)
	}
	if fact.Reason != PromotionReadyReasonReady {
		t.Fatalf("reason=%q want %q", fact.Reason, PromotionReadyReasonReady)
	}
	if fact.Epoch != 2 || fact.EndpointVersion != 3 || fact.ReplicaID != "r2" {
		t.Fatalf("lineage mismatch: %+v", fact)
	}
}

func TestBuildPromotionReadyFact_RefusesPendingFence(t *testing.T) {
	st := promotionReadyState()
	st.Reachability.FencedEpoch = 1
	fact := BuildPromotionReadyFact(st)
	if fact.Ready {
		t.Fatalf("pending fence must not be promotion-ready: %+v", fact)
	}
	if fact.Reason != PromotionReadyReasonFencePending {
		t.Fatalf("reason=%q want %q", fact.Reason, PromotionReadyReasonFencePending)
	}
}

func TestBuildPromotionReadyFact_RefusesPostCloseAckGap(t *testing.T) {
	st := promotionReadyState()
	st.Recovery.RecoveryWindowClosed = true
	st.Recovery.PostCloseDurableAckKnown = false
	fact := BuildPromotionReadyFact(st)
	if fact.Ready {
		t.Fatalf("post-close durable ack gap must not be promotion-ready: %+v", fact)
	}
	if fact.Reason != PromotionReadyReasonPostCloseAckNeeded {
		t.Fatalf("reason=%q want %q", fact.Reason, PromotionReadyReasonPostCloseAckNeeded)
	}
}

func TestBuildPromotionReadyFact_RefusesNotCaughtUp(t *testing.T) {
	st := promotionReadyState()
	st.Recovery.Decision = DecisionCatchUp
	fact := BuildPromotionReadyFact(st)
	if fact.Ready {
		t.Fatalf("catch-up decision must not be promotion-ready: %+v", fact)
	}
	if fact.Reason != PromotionReadyReasonNotCaughtUp {
		t.Fatalf("reason=%q want %q", fact.Reason, PromotionReadyReasonNotCaughtUp)
	}
}

func promotionReadyState() *ReplicaState {
	return &ReplicaState{
		Identity: IdentityTruth{
			VolumeID:        "v1",
			ReplicaID:       "r2",
			Epoch:           2,
			EndpointVersion: 3,
			MemberPresent:   true,
		},
		Reachability: ReachabilityTruth{
			Status:      ProbeReachable,
			FencedEpoch: 2,
		},
		Recovery: RecoveryTruth{
			Decision:                 DecisionNone,
			DecisionReason:           "caught_up",
			RecoveryWindowClosed:     true,
			PostCloseDurableAckKnown: true,
		},
	}
}
