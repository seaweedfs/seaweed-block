package adapter

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

// T4d-1 typed-kind end-to-end pin: SessionCloseResult carries the
// engine-owned FailureKind; NormalizeSessionClose copies it into
// engine.SessionClosedFailed without re-derivation. Engine reads
// the typed field directly — no substring parsing.

func TestAdapter_SessionCloseResult_TypedKindEndToEnd(t *testing.T) {
	// Construct a failure result with each kind; verify it
	// propagates verbatim into engine.SessionClosedFailed via
	// NormalizeSessionClose.
	cases := []struct {
		name string
		kind engine.RecoveryFailureKind
	}{
		{"WALRecycled", engine.RecoveryFailureWALRecycled},
		{"Transport", engine.RecoveryFailureTransport},
		{"SubstrateIO", engine.RecoveryFailureSubstrateIO},
		{"TargetNotReached", engine.RecoveryFailureTargetNotReached},
		{"StartTimeout", engine.RecoveryFailureStartTimeout},
		{"SessionInvalidated", engine.RecoveryFailureSessionInvalidated},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := SessionCloseResult{
				ReplicaID:   "r1",
				SessionID:   42,
				Success:     false,
				FailureKind: c.kind,
				FailReason:  "diagnostic text — engine MUST NOT parse",
			}
			ev := NormalizeSessionClose(r)
			failedEv, ok := ev.(engine.SessionClosedFailed)
			if !ok {
				t.Fatalf("NormalizeSessionClose returned wrong event type: %T", ev)
			}
			if failedEv.FailureKind != c.kind {
				t.Errorf("FailureKind = %v, want %v (typed field MUST propagate)",
					failedEv.FailureKind, c.kind)
			}
			if failedEv.Reason != r.FailReason {
				t.Errorf("Reason should propagate as diagnostic-only text")
			}
		})
	}
}

// TestAdapter_SessionCloseResult_SuccessIgnoresFailureKind — sanity:
// successful close maps to SessionClosedCompleted (no FailureKind on
// success path).
func TestAdapter_SessionCloseResult_SuccessIgnoresFailureKind(t *testing.T) {
	r := SessionCloseResult{
		ReplicaID:   "r1",
		SessionID:   42,
		Success:     true,
		AchievedLSN: 100,
		FailureKind: engine.RecoveryFailureWALRecycled, // should be ignored on success
	}
	ev := NormalizeSessionClose(r)
	if _, ok := ev.(engine.SessionClosedCompleted); !ok {
		t.Errorf("Success=true must map to SessionClosedCompleted, got %T", ev)
	}
}
