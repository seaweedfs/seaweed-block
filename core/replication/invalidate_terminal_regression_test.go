package replication

import "testing"

func TestReplicaPeer_InvalidatePreservesNeedsRebuildTerminalState(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)
	peer.SetState(ReplicaDegraded)
	peer.SetState(ReplicaNeedsRebuild)
	if got := peer.State(); got != ReplicaNeedsRebuild {
		t.Fatalf("setup state=%s", got)
	}
	peer.Invalidate("authority peer-down observation")
	invalidated := peer.State()
	peer.SetState(ReplicaHealthy)
	if invalidated != ReplicaNeedsRebuild || peer.State() != ReplicaNeedsRebuild {
		t.Fatalf("terminal state escaped: after Invalidate=%s after SetState(Healthy)=%s; want NeedsRebuild throughout", invalidated, peer.State())
	}
}
