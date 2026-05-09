package main

import "testing"

func TestNVMeControllerIDFromReplicaID_DenseReplicaSuffixes(t *testing.T) {
	tests := map[string]uint16{
		"r1":            1,
		"r2":            2,
		"replica3":      3,
		"r0":            1,
		"r65519":        uint16(maxNVMeControllerID),
		"r65520":        uint16(maxNVMeControllerID),
		"r999999999999": uint16(maxNVMeControllerID),
	}
	for replicaID, want := range tests {
		if got := nvmeControllerIDFromReplicaID(replicaID); got != want {
			t.Fatalf("nvmeControllerIDFromReplicaID(%q)=%d want %d", replicaID, got, want)
		}
	}
}

func TestNVMeControllerIDFromReplicaID_HashFallbackIsStableAndNonReserved(t *testing.T) {
	a := nvmeControllerIDFromReplicaID("east-primary")
	b := nvmeControllerIDFromReplicaID("east-primary")
	if a != b {
		t.Fatalf("hash fallback unstable: %d vs %d", a, b)
	}
	if a == 0 || a == 0xffff {
		t.Fatalf("hash fallback returned reserved CNTLID %d", a)
	}
}
