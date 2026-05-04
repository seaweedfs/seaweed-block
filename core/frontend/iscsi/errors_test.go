package iscsi

import (
	"errors"
	"testing"
)

func TestSCSIError_ErrorFormat(t *testing.T) {
	err := NewCheckConditionStaleLineage()
	msg := err.Error()
	if msg == "" {
		t.Fatal("Error() must return non-empty string")
	}
	// Stable prefix so log aggregators can filter.
	if !contains(msg, "iscsi:") || !contains(msg, "status=0x02") {
		t.Fatalf("unexpected format: %q", msg)
	}
}

func TestSCSIError_IsConstructedWithExpectedTuple(t *testing.T) {
	err := NewCheckConditionStaleLineage()
	if err.Status != StatusCheckCondition {
		t.Fatalf("Status=0x%02x want 0x02", err.Status)
	}
	if err.SenseKey != SenseNotReady {
		t.Fatalf("SenseKey=0x%02x want 0x%02x (NotReady)", err.SenseKey, SenseNotReady)
	}
	if err.ASC != ASCStaleLineage || err.ASCQ != ASCQStaleLineage {
		t.Fatalf("ASC/ASCQ=(%02x,%02x) want (%02x,%02x)",
			err.ASC, err.ASCQ, ASCStaleLineage, ASCQStaleLineage)
	}
}

func TestSCSIError_IllegalRequest_CarriesCallerASC(t *testing.T) {
	err := NewCheckConditionIllegalRequest(ASCLBAOutOfRange, 0x00, "offset > device")
	if err.SenseKey != SenseIllegalRequest {
		t.Fatalf("SenseKey=0x%02x want 0x%02x (IllegalRequest)", err.SenseKey, SenseIllegalRequest)
	}
	if err.ASC != ASCLBAOutOfRange {
		t.Fatalf("ASC=0x%02x want 0x%02x (LBAOutOfRange)", err.ASC, ASCLBAOutOfRange)
	}
}

func TestSCSIError_AsError(t *testing.T) {
	var err error = NewCheckConditionStaleLineage()
	var scsiErr *SCSIError
	if !errors.As(err, &scsiErr) {
		t.Fatal("errors.As must unwrap SCSIError")
	}
	if scsiErr.Status != StatusCheckCondition {
		t.Fatalf("unwrapped Status=0x%02x", scsiErr.Status)
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
