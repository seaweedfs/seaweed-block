package transport

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

// T4c-pre-B transport-side tests for unified recovery dispatch.
//
// Goal: pin the dispatch contract — wal_delta routes to catch-up,
// full_extent routes to rebuild, partial_lba is rejected (Stage 2),
// unknown kind is rejected. Policy validation rejects malformed
// envelopes.
//
// We do not exercise the catch-up / rebuild loops themselves — those
// are tested by their own files. We test ONLY the dispatch + policy
// validation surfaces introduced by recovery_session.go.

func TestValidateRecoveryPolicy_Empty(t *testing.T) {
	err := validateRecoveryPolicy(engine.RecoveryContentWALDelta, engine.RecoveryRuntimePolicy{})
	if err == nil {
		t.Fatal("zero-value policy must fail validation")
	}
	if !strings.Contains(err.Error(), "missing cancellation mode") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestValidateRecoveryPolicy_MissingDurationClass(t *testing.T) {
	policy := engine.RecoveryRuntimePolicy{
		CancellationMode: engine.CancelOnTimeout,
		Timeout:          1 * time.Second,
	}
	err := validateRecoveryPolicy(engine.RecoveryContentWALDelta, policy)
	if err == nil {
		t.Fatal("missing duration class must fail validation")
	}
	if !strings.Contains(err.Error(), "missing expected duration class") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestValidateRecoveryPolicy_WALDeltaTimeoutMustBePositive(t *testing.T) {
	// CancelOnTimeout with Timeout==0 is a programmer error — without
	// a positive timeout the cancellation will never fire.
	policy := engine.RecoveryRuntimePolicy{
		CancellationMode:      engine.CancelOnTimeout,
		ExpectedDurationClass: engine.DurationShort,
		Timeout:               0,
	}
	err := validateRecoveryPolicy(engine.RecoveryContentWALDelta, policy)
	if err == nil {
		t.Fatal("wal_delta + CancelOnTimeout + Timeout==0 must fail validation")
	}
	if !strings.Contains(err.Error(), "Timeout > 0") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestValidateRecoveryPolicy_FullExtentZeroTimeoutAllowed(t *testing.T) {
	// Memo §7a.1a: full_extent uses CancelOnProgressStall; Timeout=0
	// is the documented default (rebuilds may run 30min+).
	policy := engine.DefaultRuntimePolicyFor(engine.RecoveryContentFullExtent)
	if err := validateRecoveryPolicy(engine.RecoveryContentFullExtent, policy); err != nil {
		t.Fatalf("full_extent default policy must validate: %v", err)
	}
}

func TestValidateRecoveryPolicy_AllDefaultsPass(t *testing.T) {
	for _, kind := range []engine.RecoveryContentKind{
		engine.RecoveryContentWALDelta,
		engine.RecoveryContentFullExtent,
		engine.RecoveryContentPartialLBA,
	} {
		p := engine.DefaultRuntimePolicyFor(kind)
		if err := validateRecoveryPolicy(kind, p); err != nil {
			t.Errorf("default policy for kind=%q rejected by validation: %v", kind, err)
		}
	}
}

// Dispatch contract: BlockExecutor.StartRecoverySession routes
// partial_lba to an explicit Stage-2 error. We do NOT test the
// successful wal_delta / full_extent paths here because those would
// require a real replica connection — they are covered by the
// existing catchup_sender_test.go / rebuild_sender_test.go.
func TestStartRecoverySession_PartialLBARejected(t *testing.T) {
	exec := &BlockExecutor{}
	policy := engine.DefaultRuntimePolicyFor(engine.RecoveryContentPartialLBA)
	err := exec.StartRecoverySession(
		"r1", 1, 1, 1, 100,
		engine.RecoveryContentPartialLBA, policy,
	)
	if err == nil {
		t.Fatal("partial_lba must return Stage-2 error in T4c-pre-B")
	}
	if !strings.Contains(err.Error(), "Stage 2") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestStartRecoverySession_UnknownKindRejected(t *testing.T) {
	exec := &BlockExecutor{}
	policy := engine.DefaultRuntimePolicyFor(engine.RecoveryContentWALDelta)
	err := exec.StartRecoverySession(
		"r1", 1, 1, 1, 100,
		engine.RecoveryContentKind("nonsense-kind"), policy,
	)
	if err == nil {
		t.Fatal("unknown content kind must be rejected")
	}
	if !strings.Contains(err.Error(), "unknown content kind") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestStartRecoverySession_PolicyValidationFiresFirst(t *testing.T) {
	// Policy validation must run before content-kind dispatch — a
	// malformed policy on a valid kind should still surface as a
	// policy error, not a dispatch error.
	exec := &BlockExecutor{}
	err := exec.StartRecoverySession(
		"r1", 1, 1, 1, 100,
		engine.RecoveryContentFullExtent,
		engine.RecoveryRuntimePolicy{}, // zero policy
	)
	if err == nil {
		t.Fatal("zero policy must be rejected even for valid kind")
	}
	if !strings.Contains(err.Error(), "missing cancellation mode") {
		t.Errorf("expected policy validation error first, got: %v", err)
	}
}
