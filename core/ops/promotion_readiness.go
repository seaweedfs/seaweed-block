package ops

import hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"

const (
	PromotionClaimBetaRecovery             = "beta-recovery"
	PromotionClaimControlledBestEffortDemo = "controlled-best-effort-demo"
	PromotionClaimStage2ISCSIALUAMultipath = "stage2-iscsi-alua-multipath"

	PromotionAckProfileBestEffort = "best-effort"
	PromotionAckProfileSyncQuorum = "sync-quorum"
	PromotionAckProfileSyncAll    = "sync-all"

	PromotionReasonReady                    = "promotion_ready"
	PromotionReasonCandidateNotReady        = "candidate_not_ready_for_primary"
	PromotionReasonReplicationRoleNotReady  = "replication_role_not_ready"
	PromotionReasonDurableFrontierMissing   = "durable_frontier_missing"
	PromotionReasonRequiredFrontierMissing  = "required_frontier_missing"
	PromotionReasonCandidateFrontierMissing = "candidate_frontier_missing"
	PromotionReasonCandidateFrontierBehind  = "candidate_frontier_behind"
	PromotionReasonReplicationAckProfileBad = "replication_ack_profile_unmet"
)

// PromotionReadinessInput is the ops-level, evidence-only contract for deciding
// whether a non-primary replica can be described as promotion-ready. It does not
// mutate authority. Runner gates and support bundles use it to avoid turning a
// live heartbeat or a vague "healthy" bit into an RF=2 recovery claim.
type PromotionReadinessInput struct {
	CandidateReplicaID string
	ClaimProfile       string
	AckProfile         string

	Observed  bool
	Reachable bool

	AuthorityRole   string
	ReplicationRole string

	DurableLatched     bool
	DurableOperational bool

	RequiredFrontierLSN    uint64
	RequiredFrontierKnown  bool
	CandidateFrontierLSN   uint64
	CandidateFrontierKnown bool
}

type PromotionReadinessReport struct {
	CandidateReplicaID string `json:"candidate_replica_id"`
	CandidateReady     bool   `json:"candidate_ready"`
	Reason             string `json:"reason"`
	ClaimProfile       string `json:"claim_profile"`
	AckProfile         string `json:"ack_profile"`

	RequiredFrontierLSN    uint64 `json:"required_frontier_lsn"`
	RequiredFrontierKnown  bool   `json:"required_frontier_known"`
	CandidateFrontierLSN   uint64 `json:"candidate_frontier_lsn"`
	CandidateFrontierKnown bool   `json:"candidate_frontier_known"`
	FrontierCovered        bool   `json:"frontier_covered"`
}

func EvaluatePromotionReadiness(in PromotionReadinessInput) PromotionReadinessReport {
	claimProfile := in.ClaimProfile
	if claimProfile == "" {
		claimProfile = PromotionClaimBetaRecovery
	}
	out := PromotionReadinessReport{
		CandidateReplicaID:     explicitUnavailable(in.CandidateReplicaID),
		Reason:                 PromotionReasonReady,
		ClaimProfile:           claimProfile,
		AckProfile:             explicitUnavailable(in.AckProfile),
		RequiredFrontierLSN:    in.RequiredFrontierLSN,
		RequiredFrontierKnown:  in.RequiredFrontierKnown,
		CandidateFrontierLSN:   in.CandidateFrontierLSN,
		CandidateFrontierKnown: in.CandidateFrontierKnown,
	}

	if !in.Observed || !in.Reachable || in.CandidateReplicaID == "" {
		out.Reason = PromotionReasonCandidateNotReady
		return out
	}
	if in.AuthorityRole == hostvolume.AuthorityRolePrimary {
		out.Reason = PromotionReasonCandidateNotReady
		return out
	}
	if in.ReplicationRole != hostvolume.ReplicationRoleReady {
		out.Reason = PromotionReasonReplicationRoleNotReady
		return out
	}
	if !promotionAckProfileAccepted(in.AckProfile, claimProfile) {
		out.Reason = PromotionReasonReplicationAckProfileBad
		return out
	}
	if !in.DurableLatched || !in.DurableOperational {
		out.Reason = PromotionReasonDurableFrontierMissing
		return out
	}
	if !in.RequiredFrontierKnown {
		out.Reason = PromotionReasonRequiredFrontierMissing
		return out
	}
	if !in.CandidateFrontierKnown {
		out.Reason = PromotionReasonCandidateFrontierMissing
		return out
	}
	if in.CandidateFrontierLSN < in.RequiredFrontierLSN {
		out.Reason = PromotionReasonCandidateFrontierBehind
		return out
	}
	out.CandidateReady = true
	out.FrontierCovered = true
	return out
}

func promotionAckProfileAccepted(ackProfile, claimProfile string) bool {
	switch ackProfile {
	case PromotionAckProfileSyncQuorum, PromotionAckProfileSyncAll:
		return true
	case PromotionAckProfileBestEffort:
		return claimProfile == PromotionClaimControlledBestEffortDemo
	default:
		return false
	}
}

func PromotionClaimProfileAccepted(claimProfile string) bool {
	switch claimProfile {
	case "", PromotionClaimBetaRecovery, PromotionClaimControlledBestEffortDemo, PromotionClaimStage2ISCSIALUAMultipath:
		return true
	default:
		return false
	}
}
