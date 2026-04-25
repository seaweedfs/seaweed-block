package transport

import (
	"bytes"
	"strings"
	"testing"
)

// T4c-1 wire tests — `EncodeProbeReq` / `DecodeProbeReq` /
// `EncodeProbeResp` / `DecodeProbeResp` round-trip + fail-closed
// fences. Mirrors T4b-1's `TestBarrierResp_*` shape.
//
// Pins per round-26 architect (Item C.3) + round-21 uniform rule:
//   - 32B ProbeReq body, 56B ProbeResponse body
//   - both directions reject short / zeroed / field-misordered payloads
//   - lineage field order inside the slab is canonical
//     (SessionID, Epoch, EndpointVersion, TargetLSN)

func sampleProbeLineage() RecoveryLineage {
	return RecoveryLineage{
		SessionID:       42,
		Epoch:           7,
		EndpointVersion: 3,
		TargetLSN:       1000,
	}
}

// --- ProbeReq direction ---

func TestProbeReq_FullLineage_RoundTrip(t *testing.T) {
	want := ProbeRequest{Lineage: sampleProbeLineage()}
	got, err := DecodeProbeReq(EncodeProbeReq(want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != want {
		t.Errorf("round-trip mismatch:\n want=%+v\n  got=%+v", want, got)
	}
}

func TestProbeReq_ExactSize(t *testing.T) {
	buf := EncodeProbeReq(ProbeRequest{Lineage: sampleProbeLineage()})
	if len(buf) != probeReqSize {
		t.Errorf("EncodeProbeReq length = %d, want %d (architect-locked wire size)", len(buf), probeReqSize)
	}
}

func TestProbeReq_ShortPayload_Rejected(t *testing.T) {
	short := make([]byte, probeReqSize-1)
	_, err := DecodeProbeReq(short)
	if err == nil {
		t.Fatal("31-byte payload must be rejected")
	}
	if !strings.Contains(err.Error(), "short probe request") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestProbeReq_ZeroedLineage_Rejected(t *testing.T) {
	cases := []struct {
		name string
		l    RecoveryLineage
	}{
		{"all_zero", RecoveryLineage{}},
		{"zero_sessionID", RecoveryLineage{Epoch: 1, EndpointVersion: 1, TargetLSN: 1}},
		{"zero_epoch", RecoveryLineage{SessionID: 1, EndpointVersion: 1, TargetLSN: 1}},
		{"zero_endpointVersion", RecoveryLineage{SessionID: 1, Epoch: 1, TargetLSN: 1}},
		{"zero_targetLSN", RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeProbeReq(EncodeProbeReq(ProbeRequest{Lineage: tc.l}))
			if err == nil {
				t.Fatalf("zeroed lineage %+v must be rejected", tc.l)
			}
			if !strings.Contains(err.Error(), "zero-valued lineage field") {
				t.Errorf("wrong error: %v", err)
			}
		})
	}
}

// TestProbeReq_MalformedFieldOrder_Rejected pins canonical lineage
// field order (SessionID, Epoch, EndpointVersion, TargetLSN). A
// payload encoded with a swapped pair MUST be rejected by the
// zero-lineage check when the swap zeroes a field — this is the
// same fence shape as T4b-1's
// `TestBarrierResp_MalformedFieldOrder_Rejected`.
func TestProbeReq_MalformedFieldOrder_Rejected(t *testing.T) {
	// Build a payload where the canonical-order fields are
	// {SessionID:5, Epoch:0, EndpointVersion:3, TargetLSN:99} —
	// epoch slot zero would be caught by the strict-decode rule.
	// The malformed field order (e.g. swapping TargetLSN ↔ Epoch)
	// is detected indirectly via the zero-lineage rule when a slot
	// ends up zeroed.
	good := EncodeProbeReq(ProbeRequest{Lineage: sampleProbeLineage()})
	// Zero out the Epoch slot (bytes 8-16) to simulate a misordered
	// encode that placed a zero in the epoch position.
	bad := make([]byte, len(good))
	copy(bad, good)
	for i := 8; i < 16; i++ {
		bad[i] = 0
	}
	if _, err := DecodeProbeReq(bad); err == nil {
		t.Fatal("epoch-zeroed payload must be rejected")
	}
}

// --- ProbeResponse direction ---

func TestProbeResp_FullLineageEcho_RoundTrip(t *testing.T) {
	want := ProbeResponse{
		Lineage:   sampleProbeLineage(),
		SyncedLSN: 500,
		WalTail:   100,
		WalHead:   1000,
	}
	got, err := DecodeProbeResp(EncodeProbeResp(want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != want {
		t.Errorf("round-trip mismatch:\n want=%+v\n  got=%+v", want, got)
	}
}

func TestProbeResp_ExactSize(t *testing.T) {
	buf := EncodeProbeResp(ProbeResponse{
		Lineage:   sampleProbeLineage(),
		SyncedLSN: 1, WalTail: 1, WalHead: 1,
	})
	if len(buf) != probeRespSize {
		t.Errorf("EncodeProbeResp length = %d, want %d (architect-locked wire size)", len(buf), probeRespSize)
	}
}

func TestProbeResp_ShortPayload_Rejected(t *testing.T) {
	short := make([]byte, probeRespSize-1)
	_, err := DecodeProbeResp(short)
	if err == nil {
		t.Fatal("55-byte payload must be rejected")
	}
	if !strings.Contains(err.Error(), "short probe response") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestProbeResp_ZeroedLineage_Rejected(t *testing.T) {
	// All R/S/H values present but lineage zeroed.
	buf := EncodeProbeResp(ProbeResponse{
		Lineage:   RecoveryLineage{},
		SyncedLSN: 1, WalTail: 1, WalHead: 1,
	})
	_, err := DecodeProbeResp(buf)
	if err == nil {
		t.Fatal("zeroed lineage must be rejected")
	}
	if !strings.Contains(err.Error(), "zero-valued lineage field") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestProbeResp_MalformedFieldOrder_Rejected(t *testing.T) {
	// Same fence shape as ProbeReq: zero out a lineage slot inside
	// the encoded payload.
	good := EncodeProbeResp(ProbeResponse{
		Lineage:   sampleProbeLineage(),
		SyncedLSN: 5, WalTail: 1, WalHead: 5,
	})
	bad := make([]byte, len(good))
	copy(bad, good)
	// Zero the EndpointVersion slot (bytes 16-24).
	for i := 16; i < 24; i++ {
		bad[i] = 0
	}
	if _, err := DecodeProbeResp(bad); err == nil {
		t.Fatal("endpointVersion-zeroed payload must be rejected")
	}
}

// Regression fence: lineage slab inside ProbeResponse must occupy
// bytes [0..32), with SyncedLSN at [32..40), WalTail at [40..48),
// WalHead at [48..56). Same byte-position fence as T4b-1.
func TestProbeResp_ByteLayoutFence(t *testing.T) {
	resp := ProbeResponse{
		Lineage:   RecoveryLineage{SessionID: 1, Epoch: 2, EndpointVersion: 3, TargetLSN: 4},
		SyncedLSN: 0xAA,
		WalTail:   0xBB,
		WalHead:   0xCC,
	}
	buf := EncodeProbeResp(resp)
	expectedLineage := EncodeLineage(resp.Lineage)
	if !bytes.Equal(buf[0:32], expectedLineage) {
		t.Errorf("lineage slab not at bytes [0..32):\n want=%x\n  got=%x",
			expectedLineage, buf[0:32])
	}
	if buf[32+7] != 0xAA {
		t.Errorf("SyncedLSN low byte not at offset 39, got 0x%02x at offset 39", buf[39])
	}
	if buf[40+7] != 0xBB {
		t.Errorf("WalTail low byte not at offset 47, got 0x%02x at offset 47", buf[47])
	}
	if buf[48+7] != 0xCC {
		t.Errorf("WalHead low byte not at offset 55, got 0x%02x at offset 55", buf[55])
	}
}
