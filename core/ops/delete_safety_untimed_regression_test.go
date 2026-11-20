package ops

import (
	"os"
	"testing"
	"time"
)

func TestDeleteSafety_UntimedCleanupCannotProveFreshness(t *testing.T) {
	if os.Getenv("SEAWEED_KNOWN_BUGS") != "1" {
		t.Skip("known bug #188 (open): reproduction runs with SEAWEED_KNOWN_BUGS=1; the fix PR removes this skip")
	}
	decision := EvaluateSwBlockVolumeDeleteSafety(SwBlockVolumeDeleteSafetyFacts{
		DeleteRequested: true, FinalizerPresent: true,
		ObservedAt: time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC), MaxEvidenceAge: 15 * time.Minute,
		Cleanup: &CleanupEvidence{Status: ObservationStatusOK, EvidenceRef: "untimed-cleanup.txt"},
	})
	if decision.FinalizerReleaseAllowed || decision.Decision != ManagedVolumeActionDecisionUnknown {
		t.Fatalf("untimed cleanup accepted as fresh: %+v; require UNKNOWN and no finalizer release", decision)
	}
}
