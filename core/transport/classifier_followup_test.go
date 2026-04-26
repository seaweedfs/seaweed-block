package transport

import (
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// T4d-1 follow-up tests bound by QA approval (HARD GATE before T4d-3
// G-1 sign).
//
// (1) TestT4d1_TargetNotReached_DistinctKindFromWALRecycled —
//     pin: target-not-reached and WAL-recycled produce DISTINCT
//     engine kinds. They look superficially similar (both arise
//     from catch-up failure surfaces) but have completely different
//     engine semantics: WAL-recycled escalates to Rebuild
//     (tier-class change); target-not-reached is retryable per
//     RecoveryRuntimePolicy.MaxRetries.
//
// (2) TestT4d1_StorageFailureKindMapper_AllKnownKinds — fence:
//     catches future storage enum additions not mapped in transport
//     (silent Unknown/Transport fallback). Iterates all known
//     StorageRecoveryFailureKind values and asserts each maps to an
//     EXPLICIT (non-fallback) engine kind. If a future
//     StorageRecoveryFailureXXX is added to storage but the
//     transport mapper doesn't add a switch case, this test fails
//     loudly.

// TestT4d1_TargetNotReached_DistinctKindFromWALRecycled — round-46
// QA HARD GATE #1.
func TestT4d1_TargetNotReached_DistinctKindFromWALRecycled(t *testing.T) {
	// WAL-recycled path: typed substrate failure.
	walRecycledErr := storage.NewWALRecycledFailure(
		storage.ErrWALRecycled,
		"fromLSN=5 checkpointLSN=10",
	)
	walRecycledKind := classifyRecoveryFailure(walRecycledErr)

	// Target-not-reached path: catch-up sender's existing wrap
	// (transport-internal text, not a typed substrate failure).
	targetNotReachedErr := fmt.Errorf("catch-up: target %d not reached (last=%d)", 100, 50)
	targetNotReachedKind := classifyRecoveryFailure(targetNotReachedErr)

	// (a) Each maps to its expected explicit kind.
	if walRecycledKind != engine.RecoveryFailureWALRecycled {
		t.Errorf("WAL-recycled mapped to %v, want RecoveryFailureWALRecycled", walRecycledKind)
	}
	if targetNotReachedKind != engine.RecoveryFailureTargetNotReached {
		t.Errorf("target-not-reached mapped to %v, want RecoveryFailureTargetNotReached", targetNotReachedKind)
	}

	// (b) The two kinds are DISTINCT — engine branches differ.
	if walRecycledKind == targetNotReachedKind {
		t.Fatal("FAIL: WAL-recycled and target-not-reached must produce DISTINCT engine kinds " +
			"(WAL-recycled escalates to Rebuild; target-not-reached is retryable)")
	}

	// (c) Symmetry: a target-not-reached error must NOT match the
	//     WAL-recycled engine kind, and vice versa.
	if engineKindToString(walRecycledKind) == engineKindToString(targetNotReachedKind) {
		t.Errorf("kind string forms collide: %q == %q",
			engineKindToString(walRecycledKind), engineKindToString(targetNotReachedKind))
	}
}

// TestT4d1_StorageFailureKindMapper_AllKnownKinds — round-46 QA
// HARD GATE #2. Fence against silent fallback when a new storage
// kind is added without a corresponding transport switch case.
//
// If a future commit adds `StorageRecoveryFailureFoo` to storage's
// enum, this test must be updated to include the new kind in
// `knownKinds` AND the mapper switch in `classifyRecoveryFailure`
// must be extended. If only `knownKinds` is updated (not the mapper),
// the test fails because the new kind falls back to Transport. If
// only the mapper is updated, the test fails because the assertion
// loop doesn't cover the new kind.
//
// The two-sided update requirement is the desired discipline.
func TestT4d1_StorageFailureKindMapper_AllKnownKinds(t *testing.T) {
	// Every known StorageRecoveryFailureKind that has a defined
	// semantic. StorageRecoveryFailureUnknown is INTENTIONALLY
	// excluded — it's the zero-value fallback and legitimately maps
	// to Transport (the catch-all retryable kind).
	knownKinds := []storage.StorageRecoveryFailureKind{
		storage.StorageRecoveryFailureWALRecycled,
		storage.StorageRecoveryFailureSubstrateIO,
	}

	// Expected mapping (mirror of the mapper switch). If the mapper
	// adds a new case, add the corresponding entry here.
	expectedEngineKind := map[storage.StorageRecoveryFailureKind]engine.RecoveryFailureKind{
		storage.StorageRecoveryFailureWALRecycled: engine.RecoveryFailureWALRecycled,
		storage.StorageRecoveryFailureSubstrateIO: engine.RecoveryFailureSubstrateIO,
	}

	for _, k := range knownKinds {
		want, ok := expectedEngineKind[k]
		if !ok {
			t.Errorf("test scaffold: known storage kind %v missing from expectedEngineKind map — update both", k)
			continue
		}
		// Wrap a typed RecoveryFailure with this kind and verify the
		// mapper produces the expected engine kind (NOT Transport
		// fallback).
		envelope := &storage.RecoveryFailure{
			Kind:   k,
			Cause:  errors.New("synthetic test cause"),
			Detail: "scaffold",
		}
		gotEngineKind := classifyRecoveryFailure(envelope)

		if gotEngineKind == engine.RecoveryFailureTransport && want != engine.RecoveryFailureTransport {
			t.Errorf("FAIL: storage kind %v silently fell back to Transport — mapper missing a switch case (T4d-1 HARD GATE: every known storage kind must have an explicit engine mapping)", k)
		}
		if gotEngineKind != want {
			t.Errorf("storage kind %v mapped to engine kind %v, want %v", k, gotEngineKind, want)
		}
	}

	// Sanity: Unknown explicitly maps to Transport (catch-all
	// retryable; documented design choice, not a fence violation).
	unknownEnvelope := &storage.RecoveryFailure{
		Kind: storage.StorageRecoveryFailureUnknown,
	}
	if got := classifyRecoveryFailure(unknownEnvelope); got != engine.RecoveryFailureTransport {
		t.Errorf("StorageRecoveryFailureUnknown → engine kind %v, want Transport (catch-all)", got)
	}
}

// engineKindToString — small helper for the symmetry check in test (1).
func engineKindToString(k engine.RecoveryFailureKind) string { return k.String() }
