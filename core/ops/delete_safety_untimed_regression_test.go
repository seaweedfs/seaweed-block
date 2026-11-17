package ops

import (
	"testing"
	"time"
)

func TestDeleteSafety_UntimedCleanupCannotProveFreshness(t *testing.T) {
	decision := EvaluateSwBlockVolumeDeleteSafety(SwBlockVolumeDeleteSafetyFacts{
		DeleteRequested: true, FinalizerPresent: true,
		ObservedAt: time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC), MaxEvidenceAge: 15 * time.Minute,
		Cleanup: &CleanupEvidence{Status: ObservationStatusOK, EvidenceRef: "untimed-cleanup.txt"},
	})
	if decision.FinalizerReleaseAllowed || decision.Decision != ManagedVolumeActionDecisionUnknown {
		t.Fatalf("untimed cleanup accepted as fresh: %+v; require UNKNOWN and no finalizer release", decision)
	}
}
