package transport

// G7-redo priority #3 [retry] Option A — boundary mapping pin for
// `recovery.Failure(PinUnderRetention)` → `engine.RecoveryFailurePinUnderRetention`.
//
// The bug this test prevents: before Option A, the boundary mapper
// (`classifyRecoveryFailure`) only switched on `*storage.RecoveryFailure`.
// A `*recovery.Failure(PinUnderRetention)` returned by the sender's
// `coord.SetPinFloor` path fell through to the catch-all
// `RecoveryFailureTransport` (retryable). The engine then re-emitted
// `Start*`, the same pin violation surfaced again on the next ack,
// and the lineage spun until the budget exhausted — masking the
// real "fresh probe + new lineage" signal.

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/recovery"
)

// TestClassifier_RecoveryPinUnderRetention_MapsToEngineKind pins the
// headline mapping. A typed `*recovery.Failure` with kind
// `FailurePinUnderRetention` MUST classify to
// `engine.RecoveryFailurePinUnderRetention` (NOT the
// `RecoveryFailureTransport` fallback).
func TestClassifier_RecoveryPinUnderRetention_MapsToEngineKind(t *testing.T) {
	// Failure fields are all exported; package-private newFailure is
	// the production constructor — for a boundary mapping test we
	// build one directly.
	pinErr := &recovery.Failure{
		Kind:       recovery.FailurePinUnderRetention,
		Phase:      recovery.PhasePinUpdate,
		Underlying: errors.New("synthetic: floor=240 < primaryS=250"),
	}
	got := classifyRecoveryFailure(pinErr)
	if got != engine.RecoveryFailurePinUnderRetention {
		t.Errorf("classifyRecoveryFailure(PinUnderRetention) = %v, want RecoveryFailurePinUnderRetention",
			got)
	}
	// Counter-pin: explicitly NOT the retryable Transport fallback —
	// this is the bug the new mapping closes.
	if got == engine.RecoveryFailureTransport {
		t.Fatal("FAIL: PinUnderRetention silently fell back to Transport (retryable). " +
			"Engine would re-emit Start* on the same lineage. See G7-redo priority #3 Option A.")
	}
}

// TestClassifier_RecoveryFailure_OtherKinds_StillFallThrough pins
// the deferred-scope guarantee: only PinUnderRetention is mapped in
// Option A. Other recovery kinds (Wire, Protocol, Substrate, Contract,
// Cancelled, SingleFlight, Unknown) MUST continue to fall through to
// the existing Transport default until Option B kickoff lands.
//
// This test exists so that adding a new recovery kind to the mapper
// requires consciously updating both the production switch and this
// test — no silent drift.
func TestClassifier_RecoveryFailure_OtherKinds_StillFallThrough(t *testing.T) {
	// Only PinUnderRetention is mapped today. Every OTHER kind must
	// fall through to RecoveryFailureTransport. If Option B lands and
	// adds more mappings, this list shrinks and this test must be
	// updated to assert the new kinds explicitly.
	deferredKinds := []recovery.FailureKind{
		recovery.FailureWire,
		recovery.FailureProtocol,
		recovery.FailureSubstrate,
		recovery.FailureContract,
		recovery.FailureCancelled,
		recovery.FailureSingleFlight,
		// recovery.FailureWALRecycled is NOT in this list because
		// substrate-side already maps it (via *storage.RecoveryFailure
		// path); it never reaches the recovery.Failure switch.
		recovery.FailureUnknown,
	}
	for _, k := range deferredKinds {
		err := &recovery.Failure{
			Kind:       k,
			Phase:      recovery.PhaseSendStart,
			Underlying: errors.New("scaffold"),
		}
		got := classifyRecoveryFailure(err)
		if got != engine.RecoveryFailureTransport {
			t.Errorf("recovery.Failure(%s) classified to %v; Option A scope wants Transport fallback "+
				"until Option B kickoff lands explicit mappings",
				k, got)
		}
	}
}
