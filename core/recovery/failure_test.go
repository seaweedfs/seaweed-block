package recovery

import (
	"errors"
	"testing"
)

// TestFailure_RetryableMatrix pins each FailureKind's retryability
// classification. Layer-3 retry budget at the engine reads this
// matrix; changing it MUST be a deliberate ledger update.
func TestFailure_RetryableMatrix(t *testing.T) {
	cases := []struct {
		kind      FailureKind
		retryable bool
	}{
		{FailureUnknown, false},
		{FailureWire, true},
		{FailureProtocol, false},
		{FailureSubstrate, true},
		{FailureContract, true}, // retryable with caveat — engine re-probes
		{FailureCancelled, false},
		{FailureSingleFlight, false},
		{FailureWALRecycled, false},
	}
	for _, tc := range cases {
		f := newFailure(tc.kind, PhaseStartSession, errors.New("test"))
		if got := f.Retryable(); got != tc.retryable {
			t.Errorf("kind=%s: Retryable()=%v want %v", tc.kind, got, tc.retryable)
		}
	}
}

func TestFailure_NilSafeAndUnwrap(t *testing.T) {
	var nilF *Failure
	if nilF.Retryable() {
		t.Error("nil Failure: Retryable() want false")
	}
	if got := nilF.Error(); got == "" {
		t.Error("nil Failure: Error() should return placeholder, not empty")
	}

	cause := errors.New("underlying cause")
	f := newFailure(FailureWire, PhaseBarrierResp, cause)
	if got := f.Unwrap(); !errors.Is(got, cause) {
		t.Errorf("Unwrap mismatch: got %v want %v", got, cause)
	}
	if !errors.Is(f, cause) {
		t.Error("errors.Is(Failure, cause) should be true via Unwrap")
	}
}

func TestFailure_AsFailureExtraction(t *testing.T) {
	cause := errors.New("underlying")
	f := newFailure(FailureSubstrate, PhaseBaseLane, cause)

	var asErr error = f
	got := AsFailure(asErr)
	if got == nil {
		t.Fatal("AsFailure on a Failure: want non-nil")
	}
	if got.Kind != FailureSubstrate {
		t.Errorf("AsFailure kind: got %s want Substrate", got.Kind)
	}

	// Plain errors return nil.
	if AsFailure(cause) != nil {
		t.Error("AsFailure on plain error: want nil")
	}
	if AsFailure(nil) != nil {
		t.Error("AsFailure on nil: want nil")
	}
}

// TestFailure_KindStringStable ensures the String() rendering is
// stable for log/parse use. Don't change without a ledger note.
func TestFailure_KindStringStable(t *testing.T) {
	cases := map[FailureKind]string{
		FailureUnknown:      "Unknown",
		FailureWire:         "Wire",
		FailureProtocol:     "Protocol",
		FailureSubstrate:    "Substrate",
		FailureContract:     "Contract",
		FailureCancelled:    "Cancelled",
		FailureSingleFlight: "SingleFlight",
		FailureWALRecycled:  "WALRecycled",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Errorf("FailureKind(%d).String()=%q want %q", int(k), got, want)
		}
	}
}
