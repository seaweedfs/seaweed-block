package main

import "testing"

// The volume answers every promotion probe with its own --replication-ack,
// in the canonical form the master compares (best_effort, sync_quorum,
// sync_all).
func TestHostConfigAnswersTheVolumesOwnAckProfile(t *testing.T) {
	for flag, want := range map[string]string{"best-effort": "best_effort", "sync-quorum": "sync_quorum", "sync-all": "sync_all"} {
		if got := hostConfig(flags{replicationAck: flag}, nil).AckProfile; got != want {
			t.Fatalf("--replication-ack=%s: AckProfile %q; want %q", flag, got, want)
		}
	}
}
