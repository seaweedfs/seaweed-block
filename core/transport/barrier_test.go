package transport

// Completion oracle: recover(a,b) band — NOT recover(a) closure.
// See sw-block/design/recover-semantics-adjustment-plan.md §8.1.

import (
	"encoding/binary"
	"strings"
	"testing"
)

// lineageSample is the canonical non-zero lineage used by tests that
// exercise the post-T4b-1 full-lineage-echo wire shape.
func lineageSample() RecoveryLineage {
	return RecoveryLineage{
		SessionID:       42,
		Epoch:           7,
		EndpointVersion: 3,
		TargetLSN:       1000,
	}
}

// TestBarrierResp_FullLineageEcho_RoundTrip is the T4b-1 primary
// pin: every field of an encoded BarrierResponse survives decode
// byte-exact, and the byte layout matches the architect-specified
// [32B lineage][8B achievedLSN] order with the lineage slab itself
// in (SessionID, Epoch, EndpointVersion, TargetLSN) order.
//
// Required test per mini-plan §2 T4b-1.
func TestBarrierResp_FullLineageEcho_RoundTrip(t *testing.T) {
	cases := []struct {
		name        string
		lineage     RecoveryLineage
		achievedLSN uint64
	}{
		{
			name:        "typical",
			lineage:     lineageSample(),
			achievedLSN: 12345,
		},
		{
			name: "largest_valid",
			lineage: RecoveryLineage{
				SessionID:       ^uint64(0),
				Epoch:           ^uint64(0),
				EndpointVersion: ^uint64(0),
				TargetLSN:       ^uint64(0),
			},
			achievedLSN: ^uint64(0),
		},
		{
			name: "mins_all_one",
			lineage: RecoveryLineage{
				SessionID:       1,
				Epoch:           1,
				EndpointVersion: 1,
				TargetLSN:       1,
			},
			achievedLSN: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := BarrierResponse{Lineage: tc.lineage, AchievedLSN: tc.achievedLSN}
			buf := EncodeBarrierResp(want)
			if len(buf) != barrierRespSize {
				t.Fatalf("encoded length: got %d want %d", len(buf), barrierRespSize)
			}
			got, err := DecodeBarrierResp(buf)
			if err != nil {
				t.Fatalf("DecodeBarrierResp: %v", err)
			}
			if got != want {
				t.Fatalf("roundtrip: got %+v want %+v", got, want)
			}
		})
	}
}

// TestBarrierResp_ShortPayload_Rejected — fail-closed fence per
// round-21: any payload < 40 bytes MUST be rejected with an error,
// never silently decoded into a partial/nil-filled struct that
// callers could count toward durability.
//
// Required test per mini-plan §2 T4b-1.
func TestBarrierResp_ShortPayload_Rejected(t *testing.T) {
	// Loop covers every short length from 0 through 39 (barrierRespSize-1).
	for n := 0; n < barrierRespSize; n++ {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, n)
			// Populate with a valid-looking prefix so the test fails
			// only because of length, not because of zero-lineage.
			for i := 0; i < n; i++ {
				buf[i] = byte((i + 1) & 0xFF)
			}
			_, err := DecodeBarrierResp(buf)
			if err == nil {
				t.Fatalf("%d-byte payload must be rejected, got nil", n)
			}
			if !strings.Contains(err.Error(), "short barrier response") {
				t.Fatalf("unexpected error shape: %v", err)
			}
		})
	}
}

// TestBarrierResp_ZeroedLineage_Rejected — catches the
// partially-initialized-struct drift case where the payload is
// technically 40 bytes but one or more lineage fields are zero.
// Per round-21 such a response MUST be rejected — accepting it
// would let a silent heap-reuse or uninitialized-struct bug count
// as a valid ack.
//
// Required test per mini-plan §2 T4b-1.
func TestBarrierResp_ZeroedLineage_Rejected(t *testing.T) {
	zeroed := []struct {
		name string
		l    RecoveryLineage
	}{
		{"all_zero", RecoveryLineage{}},
		{"zero_sessionID", RecoveryLineage{Epoch: 1, EndpointVersion: 1, TargetLSN: 1}},
		{"zero_epoch", RecoveryLineage{SessionID: 1, EndpointVersion: 1, TargetLSN: 1}},
		{"zero_endpointVersion", RecoveryLineage{SessionID: 1, Epoch: 1, TargetLSN: 1}},
		{"zero_targetLSN", RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1}},
	}
	for _, tc := range zeroed {
		t.Run(tc.name, func(t *testing.T) {
			buf := EncodeBarrierResp(BarrierResponse{Lineage: tc.l, AchievedLSN: 99})
			_, err := DecodeBarrierResp(buf)
			if err == nil {
				t.Fatalf("lineage with zero field must be rejected, got nil (lineage=%+v)", tc.l)
			}
			if !strings.Contains(err.Error(), "zero-valued lineage") {
				t.Fatalf("unexpected error shape: %v", err)
			}
		})
	}
}

// TestBarrierResp_MalformedFieldOrder_Rejected — byte-level fence
// proving the field-order contract. We craft a 40-byte payload
// whose lineage slab is intentionally scrambled vs the canonical
// (SessionID, Epoch, EndpointVersion, TargetLSN) order AND whose
// "zero" field shifts to a different position than expected.
// DecodeBarrierResp must reject; the hazard this pins is "someone
// reorders the fields at encode time but the test still passes
// because numeric equality accidentally matches."
//
// Specific shape: swap SessionID and Epoch byte positions so a
// caller that reads the first 8 bytes as Epoch (the swap target)
// would see a sensible non-zero value, but reading the second 8
// bytes as Epoch (correct position) would see zero → rejected.
//
// Required test per mini-plan §2 T4b-1.
func TestBarrierResp_MalformedFieldOrder_Rejected(t *testing.T) {
	buf := make([]byte, barrierRespSize)
	// SessionID position (bytes 0-7): write a non-zero value.
	binary.BigEndian.PutUint64(buf[0:8], 42)
	// Epoch position (bytes 8-15): write ZERO — simulates a
	// reordering bug where Epoch accidentally got put where
	// TargetLSN should be, leaving Epoch's slot uninitialized.
	binary.BigEndian.PutUint64(buf[8:16], 0)
	// EndpointVersion position (bytes 16-23): non-zero.
	binary.BigEndian.PutUint64(buf[16:24], 3)
	// TargetLSN position (bytes 24-31): non-zero.
	binary.BigEndian.PutUint64(buf[24:32], 1000)
	// AchievedLSN (bytes 32-39): non-zero.
	binary.BigEndian.PutUint64(buf[32:40], 5000)

	_, err := DecodeBarrierResp(buf)
	if err == nil {
		t.Fatal("malformed field order (Epoch slot zero) must be rejected, got nil")
	}
	// The specific failure mode is zero-valued Epoch — that's the
	// assertion. The decode layer catches it via the zero-lineage
	// rule, which is the same fail-closed mechanism round-21
	// demands for ANY field-ordering bug that leaves a lineage
	// slot at zero.
	if !strings.Contains(err.Error(), "zero-valued lineage") {
		t.Fatalf("expected zero-lineage rejection (field-order fence), got: %v", err)
	}

	// Also: a fully-populated-but-reordered payload (every slot
	// nonzero, but in the wrong order) must at minimum not
	// silently round-trip the wrong values. We encode via the
	// canonical path then decode; equal order ⇒ equal struct.
	// This catches a drift where a future refactor changes the
	// encode order without updating the decode order.
	orig := BarrierResponse{
		Lineage: RecoveryLineage{
			SessionID: 100, Epoch: 200, EndpointVersion: 300, TargetLSN: 400,
		},
		AchievedLSN: 500,
	}
	encoded := EncodeBarrierResp(orig)
	decoded, err := DecodeBarrierResp(encoded)
	if err != nil {
		t.Fatalf("canonical encode/decode failed: %v", err)
	}
	if decoded != orig {
		t.Fatalf("canonical round-trip drift: got %+v want %+v (if this fails, encode and decode disagree on field order)",
			decoded, orig)
	}
	// Explicit byte-position assertion: SessionID at [0:8], Epoch
	// at [8:16]. If anyone ever silently swaps, this trips.
	if got := binary.BigEndian.Uint64(encoded[0:8]); got != orig.Lineage.SessionID {
		t.Fatalf("byte positions [0:8]: got %d want %d (SessionID moved?)", got, orig.Lineage.SessionID)
	}
	if got := binary.BigEndian.Uint64(encoded[8:16]); got != orig.Lineage.Epoch {
		t.Fatalf("byte positions [8:16]: got %d want %d (Epoch moved?)", got, orig.Lineage.Epoch)
	}
	if got := binary.BigEndian.Uint64(encoded[16:24]); got != orig.Lineage.EndpointVersion {
		t.Fatalf("byte positions [16:24]: got %d want %d (EndpointVersion moved?)", got, orig.Lineage.EndpointVersion)
	}
	if got := binary.BigEndian.Uint64(encoded[24:32]); got != orig.Lineage.TargetLSN {
		t.Fatalf("byte positions [24:32]: got %d want %d (TargetLSN moved?)", got, orig.Lineage.TargetLSN)
	}
	if got := binary.BigEndian.Uint64(encoded[32:40]); got != orig.AchievedLSN {
		t.Fatalf("byte positions [32:40]: got %d want %d (AchievedLSN moved?)", got, orig.AchievedLSN)
	}
}
