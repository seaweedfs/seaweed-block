package recovery

import (
	"testing"
)

func TestReceiver_checkMonotonic_T4(t *testing.T) {
	r := &Receiver{recvFromLSN: 10, appliedWalLSN: 10}

	if err := r.checkMonotonic(11); err != nil {
		t.Fatalf("first forward: %v", err)
	}
	r.appliedWalLSN = 11

	if err := r.checkMonotonic(11); err == nil {
		t.Fatal("exact duplicate: want error")
	} else {
		f := AsFailure(err)
		if f == nil || f.Kind != FailureProtocol {
			t.Fatalf("duplicate: got %v", err)
		}
	}

	r.appliedWalLSN = 20
	if err := r.checkMonotonic(22); err == nil {
		t.Fatal("gap: want error")
	} else {
		f := AsFailure(err)
		if f == nil || f.Kind != FailureContract {
			t.Fatalf("gap: got %v", err)
		}
	}

	r.appliedWalLSN = 30
	if err := r.checkMonotonic(29); err == nil {
		t.Fatal("backward: want error")
	} else {
		f := AsFailure(err)
		if f == nil || f.Kind != FailureProtocol {
			t.Fatalf("backward: got %v", err)
		}
	}
}

func TestReceiver_checkMonotonic_boundaryDup(t *testing.T) {
	r := &Receiver{recvFromLSN: 5, appliedWalLSN: 5}
	err := r.checkMonotonic(5)
	if err == nil {
		t.Fatal("lsn==fromLSN before any apply: want Protocol failure")
	}
	f := AsFailure(err)
	if f == nil || f.Kind != FailureProtocol {
		t.Fatalf("want FailureProtocol, got %v", err)
	}
}
