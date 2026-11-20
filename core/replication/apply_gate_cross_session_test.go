package replication

import (
	"bytes"
	"testing"
)

// Both sessions are initialized before either writes the contested LBA.
// This fixes the ordering without relying on a probabilistic goroutine race.
func TestApplyGate_OverlappingSessionsNeverRegressBytes(t *testing.T) {
	for _, live := range []bool{false, true} {
		name := "recovery_then_older_recovery"
		if live {
			name = "live_then_older_recovery"
		}
		t.Run(name, func(t *testing.T) {
			gate, store := newGate(t)
			older, newer := recoveryLineage(10, 120), recoveryLineage(11, 120)
			if err := gate.ApplyRecovery(older, 0, makeData(1), 1); err != nil {
				t.Fatal(err)
			}
			if err := gate.ApplyRecovery(newer, 1, makeData(2), 2); err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0xBB}, blockSize)
			apply := gate.ApplyRecovery
			if live {
				apply = gate.ApplyLive
			}
			if err := apply(newer, 5, want, 120); err != nil {
				t.Fatal(err)
			}
			if err := gate.ApplyRecovery(older, 5, bytes.Repeat([]byte{0xAA}, blockSize), 95); err != nil {
				t.Fatal(err)
			}
			got, err := store.Read(5)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("cross-session stale recovery regressed bytes: got %x want %x", got[:8], want[:8])
			}
			if !gate.SessionRecoveryCoverage(older.SessionID, 5) {
				t.Fatal("stale recovery did not advance its own coverage")
			}
		})
	}
}
