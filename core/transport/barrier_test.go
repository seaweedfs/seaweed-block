package transport

import "testing"

// TestBarrierResponse_RoundtripPreservesAchievedLSN locks the wire
// contract: AchievedLSN encoded by the replica is the exact value
// the primary reads. This is the institution-boundary check for the
// data-sync seam — if encode/decode ever drifts, the primary would
// silently report a different completion frontier than the replica
// actually reached.
func TestBarrierResponse_RoundtripPreservesAchievedLSN(t *testing.T) {
	cases := []uint64{0, 1, 42, 1 << 32, ^uint64(0)}
	for _, want := range cases {
		buf := EncodeBarrierResp(BarrierResponse{AchievedLSN: want})
		got, err := DecodeBarrierResp(buf)
		if err != nil {
			t.Fatalf("decode %d: %v", want, err)
		}
		if got.AchievedLSN != want {
			t.Fatalf("roundtrip: want AchievedLSN=%d, got %d", want, got.AchievedLSN)
		}
	}
}

// TestBarrierResponse_DecodeShortPayloadFailsClosed ensures a
// truncated barrier response is never silently treated as
// AchievedLSN=0 (or any other guess). The primary must surface the
// error to the engine so the session is reported as failed, not
// completed at a fabricated frontier.
func TestBarrierResponse_DecodeShortPayloadFailsClosed(t *testing.T) {
	for n := 0; n < barrierRespSize; n++ {
		buf := make([]byte, n)
		if _, err := DecodeBarrierResp(buf); err == nil {
			t.Fatalf("decode %d-byte payload: want error, got nil", n)
		}
	}
}
