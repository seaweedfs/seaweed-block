package engine

const (
	PromotionReadyReasonReady              = "promotion_ready"
	PromotionReadyReasonNotMember          = "not_member"
	PromotionReadyReasonNotReachable       = "not_reachable"
	PromotionReadyReasonNotCaughtUp        = "not_caught_up"
	PromotionReadyReasonFencePending       = "fence_pending"
	PromotionReadyReasonPostCloseAckNeeded = "post_close_durable_ack_needed"
)

// PromotionReadyFact is a control-facing fact derived from the engine's truth
// domains. It intentionally does not reuse ReplicaProjection, which is
// operator-facing and must not be fed back into authority decisions.
type PromotionReadyFact struct {
	Ready           bool
	Reason          string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
}

func BuildPromotionReadyFact(st *ReplicaState) PromotionReadyFact {
	if st == nil {
		return PromotionReadyFact{Reason: PromotionReadyReasonNotMember}
	}
	fact := PromotionReadyFact{
		Reason:          PromotionReadyReasonReady,
		ReplicaID:       st.Identity.ReplicaID,
		Epoch:           st.Identity.Epoch,
		EndpointVersion: st.Identity.EndpointVersion,
	}
	switch {
	case !st.Identity.MemberPresent:
		fact.Reason = PromotionReadyReasonNotMember
	case st.Reachability.Status != ProbeReachable:
		fact.Reason = PromotionReadyReasonNotReachable
	case st.Recovery.Decision != DecisionNone:
		fact.Reason = PromotionReadyReasonNotCaughtUp
	case st.Identity.Epoch > st.Reachability.FencedEpoch:
		fact.Reason = PromotionReadyReasonFencePending
	case recoveredReplicaWaitingForPostCloseAck(st):
		fact.Reason = PromotionReadyReasonPostCloseAckNeeded
	default:
		fact.Ready = true
	}
	return fact
}
