package nvme

import (
	"errors"
	"testing"
)

func TestStatusError_StaleLineageTuple(t *testing.T) {
	se := NewStaleLineage()
	if se.SCT != SCTPathRelated {
		t.Fatalf("SCT=0x%x want PathRelated (0x3)", se.SCT)
	}
	if se.SC != SCPathAsymAccessTransition {
		t.Fatalf("SC=0x%02x want ANATransition (0x03)", se.SC)
	}
}

func TestStatusError_BackendClosedTuple(t *testing.T) {
	se := NewBackendClosed()
	if se.SCT != SCTPathRelated {
		t.Fatalf("SCT=0x%x want PathRelated", se.SCT)
	}
	if se.SC != SCPathAsymAccessInaccessible {
		t.Fatalf("SC=0x%02x want ANAInaccessible (0x02)", se.SC)
	}
	// Distinct from stale-lineage so operators can tell them apart.
	if se.SC == SCPathAsymAccessTransition {
		t.Fatal("closed-backend must NOT use ANATransition SC (would conflate two fail modes)")
	}
}

func TestStatusError_InvalidField_CarriesCallerSC(t *testing.T) {
	se := NewInvalidField(SCLBAOutOfRange, "LBA past end")
	if se.SCT != SCTGeneric {
		t.Fatalf("SCT=0x%x want Generic (0x0)", se.SCT)
	}
	if se.SC != SCLBAOutOfRange {
		t.Fatalf("SC=0x%02x want LBAOutOfRange (0x80)", se.SC)
	}
}

func TestStatusError_ErrorFormat_Stable(t *testing.T) {
	se := NewStaleLineage()
	msg := se.Error()
	if msg == "" {
		t.Fatal("Error() empty")
	}
	if want := "nvme:"; !contains(msg, want) {
		t.Fatalf("msg=%q should contain %q", msg, want)
	}
}

func TestStatusError_AsError(t *testing.T) {
	var err error = NewStaleLineage()
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatal("errors.As must unwrap *StatusError")
	}
	if se.SC != SCPathAsymAccessTransition {
		t.Fatalf("unwrapped SC=0x%02x", se.SC)
	}
}

// T2 invariant: stale-lineage and closed-backend map to
// DIFFERENT wire bits, so Linux's nvme driver can route retries
// vs hard failures differently.
func TestStatusError_StaleAndClosed_DistinctOnWire(t *testing.T) {
	stale := NewStaleLineage().EncodeStatusField()
	closed := NewBackendClosed().EncodeStatusField()
	if stale == closed {
		t.Fatalf("stale (0x%04x) and closed (0x%04x) produce identical wire bits — would conflate fail modes",
			stale, closed)
	}
}

// Sanity on the encoder: SCT sits in bits 9-11, SC in bits 1-8,
// Phase Tag (bit 0) stays 0 at this layer.
func TestStatusError_EncodeStatusField_LayoutCorrect(t *testing.T) {
	se := &StatusError{SCT: 0x3, SC: 0x03} // stale-lineage tuple
	got := se.EncodeStatusField()
	// Expected: (SC << 1) | (SCT << 9) = (0x03 << 1) | (0x3 << 9) = 0x0606
	if got != 0x0606 {
		t.Fatalf("encoded=0x%04x want 0x0606 (SC=0x03 SCT=0x3)", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
