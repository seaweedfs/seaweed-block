package ops

import (
	"testing"

	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
)

func TestEvaluatePromotionReadiness_HeartbeatOnlyIsNotReady(t *testing.T) {
	report := EvaluatePromotionReadiness(PromotionReadinessInput{
		CandidateReplicaID: "r2",
		Observed:           true,
		Reachable:          true,
		AuthorityRole:      hostvolume.AuthorityRoleUnknown,
		ReplicationRole:    hostvolume.ReplicationRoleNotReady,
		AckProfile:         PromotionAckProfileSyncQuorum,
	})
	if report.CandidateReady {
		t.Fatalf("heartbeat-only candidate must not be promotion-ready: %+v", report)
	}
	if report.Reason != PromotionReasonReplicationRoleNotReady {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonReplicationRoleNotReady)
	}
}

func TestEvaluatePromotionReadiness_BetaRecoveryRejectsBestEffortAck(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.AckProfile = PromotionAckProfileBestEffort
	}))
	if report.CandidateReady {
		t.Fatalf("beta-facing recovery must not pass on best-effort ACK: %+v", report)
	}
	if report.Reason != PromotionReasonReplicationAckProfileBad {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonReplicationAckProfileBad)
	}
	if !report.CandidateFrontierKnown || report.CandidateFrontierLSN != report.RequiredFrontierLSN {
		t.Fatalf("frontier evidence should remain visible when ACK profile fails: %+v", report)
	}
}

func TestEvaluatePromotionReadiness_MissingDurableFrontierIsNotReady(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.DurableLatched = false
		in.DurableOperational = false
		in.RequiredFrontierKnown = false
		in.CandidateFrontierKnown = false
		in.CandidateFrontierLSN = 0
	}))
	if report.CandidateReady {
		t.Fatalf("candidate without frontier evidence must not be ready: %+v", report)
	}
	if report.Reason != PromotionReasonDurableFrontierMissing {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonDurableFrontierMissing)
	}
}

func TestEvaluatePromotionReadiness_MissingRequiredFrontierIsNotReady(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.RequiredFrontierKnown = false
		in.RequiredFrontierLSN = 0
		in.CandidateFrontierKnown = true
		in.CandidateFrontierLSN = 90
	}))
	if report.CandidateReady {
		t.Fatalf("candidate without writer-required frontier must not be ready: %+v", report)
	}
	if report.Reason != PromotionReasonRequiredFrontierMissing {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonRequiredFrontierMissing)
	}
}

func TestEvaluatePromotionReadiness_MissingCandidateFrontierIsNotReady(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.CandidateFrontierKnown = false
		in.CandidateFrontierLSN = 0
	}))
	if report.CandidateReady {
		t.Fatalf("candidate without own frontier must not be ready: %+v", report)
	}
	if report.Reason != PromotionReasonCandidateFrontierMissing {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonCandidateFrontierMissing)
	}
}

func TestEvaluatePromotionReadiness_EmptyVolumeFrontierZeroCanBeReadyWhenKnown(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.RequiredFrontierLSN = 0
		in.CandidateFrontierLSN = 0
		in.RequiredFrontierKnown = true
		in.CandidateFrontierKnown = true
	}))
	if !report.CandidateReady {
		t.Fatalf("known empty-volume frontier should be accepted: %+v", report)
	}
	if !report.FrontierCovered {
		t.Fatalf("frontier_covered should be true for known zero frontier: %+v", report)
	}
}

func TestEvaluatePromotionReadiness_BehindFrontierIsNotReady(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.RequiredFrontierLSN = 90
		in.CandidateFrontierLSN = 89
	}))
	if report.CandidateReady {
		t.Fatalf("candidate behind writer frontier must not be ready: %+v", report)
	}
	if report.Reason != PromotionReasonCandidateFrontierBehind {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonCandidateFrontierBehind)
	}
	if report.FrontierCovered {
		t.Fatalf("frontier_covered must be false when candidate is behind: %+v", report)
	}
}

func TestEvaluatePromotionReadiness_SyncQuorumCoveredFrontierIsReady(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(nil))
	if !report.CandidateReady {
		t.Fatalf("sync-quorum candidate with covered frontier should be ready: %+v", report)
	}
	if report.Reason != PromotionReasonReady {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonReady)
	}
	if !report.FrontierCovered {
		t.Fatalf("frontier_covered should be true: %+v", report)
	}
}

func TestEvaluatePromotionReadiness_Stage2ISCSIALUAMultipathAcceptsSyncQuorum(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.ClaimProfile = PromotionClaimStage2ISCSIALUAMultipath
		in.AckProfile = PromotionAckProfileSyncQuorum
	}))
	if !report.CandidateReady {
		t.Fatalf("stage2 multipath claim should accept covered sync-quorum frontier: %+v", report)
	}
	if report.ClaimProfile != PromotionClaimStage2ISCSIALUAMultipath {
		t.Fatalf("claim_profile=%q want %q", report.ClaimProfile, PromotionClaimStage2ISCSIALUAMultipath)
	}
}

func TestEvaluatePromotionReadiness_Stage2ISCSIALUAMultipathRejectsBestEffort(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.ClaimProfile = PromotionClaimStage2ISCSIALUAMultipath
		in.AckProfile = PromotionAckProfileBestEffort
	}))
	if report.CandidateReady {
		t.Fatalf("stage2 multipath claim must not pass on best-effort ACK: %+v", report)
	}
	if report.Reason != PromotionReasonReplicationAckProfileBad {
		t.Fatalf("reason=%q want %q", report.Reason, PromotionReasonReplicationAckProfileBad)
	}
}

func TestEvaluatePromotionReadiness_ControlledBestEffortDemoCanBeReadyWithExplicitProfile(t *testing.T) {
	report := EvaluatePromotionReadiness(promotionReadyInput(func(in *PromotionReadinessInput) {
		in.ClaimProfile = PromotionClaimControlledBestEffortDemo
		in.AckProfile = PromotionAckProfileBestEffort
	}))
	if !report.CandidateReady {
		t.Fatalf("controlled best-effort demo may be ready for its explicit narrow claim: %+v", report)
	}
	if report.ClaimProfile != PromotionClaimControlledBestEffortDemo {
		t.Fatalf("claim_profile=%q want %q", report.ClaimProfile, PromotionClaimControlledBestEffortDemo)
	}
}

func promotionReadyInput(mut func(*PromotionReadinessInput)) PromotionReadinessInput {
	in := PromotionReadinessInput{
		CandidateReplicaID:     "r2",
		Observed:               true,
		Reachable:              true,
		AuthorityRole:          hostvolume.AuthorityRoleUnknown,
		ReplicationRole:        hostvolume.ReplicationRoleReady,
		DurableLatched:         true,
		DurableOperational:     true,
		AckProfile:             PromotionAckProfileSyncQuorum,
		RequiredFrontierLSN:    90,
		RequiredFrontierKnown:  true,
		CandidateFrontierLSN:   90,
		CandidateFrontierKnown: true,
	}
	if mut != nil {
		mut(&in)
	}
	return in
}
