package replication

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// #182: Invalidate still moves every non-terminal state to Degraded. A new
// peer starts Healthy; Unknown is only set by Close, after which Invalidate
// returns early, so it is not a reachable starting state here.
func TestReplicaPeer_InvalidateNonTerminalStatesBecomeDegraded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup []ReplicaState
		want  ReplicaState // state before Invalidate
	}{
		{"healthy", []ReplicaState{ReplicaHealthy}, ReplicaHealthy},
		{"degraded", []ReplicaState{ReplicaHealthy, ReplicaDegraded}, ReplicaDegraded},
		{"catching_up", []ReplicaState{ReplicaHealthy, ReplicaCatchingUp}, ReplicaCatchingUp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, _, _ := setupPeerWithRealReplica(t)
			for _, s := range tc.setup {
				peer.SetState(s)
			}
			if got := peer.State(); got != tc.want {
				t.Fatalf("setup state=%s want %s", got, tc.want)
			}
			peer.Invalidate("test invalidation")
			if got := peer.State(); got != ReplicaDegraded {
				t.Fatalf("after Invalidate from %s: state=%s want degraded", tc.want, got)
			}
		})
	}
}

// #182: on a NeedsRebuild peer, Invalidate keeps the state but still logs
// the reason, its only other effect.
func TestReplicaPeer_InvalidateOnNeedsRebuildStillLogsReason(t *testing.T) {
	peer, _, _ := setupPeerWithRealReplica(t)
	peer.SetState(ReplicaHealthy)
	peer.SetState(ReplicaDegraded)
	peer.SetState(ReplicaNeedsRebuild)
	if got := peer.State(); got != ReplicaNeedsRebuild {
		t.Fatalf("setup state=%s", got)
	}
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	peer.Invalidate("reason-182-marker")
	log.SetOutput(prev)
	if got := peer.State(); got != ReplicaNeedsRebuild {
		t.Fatalf("state=%s want needs_rebuild kept", got)
	}
	if !strings.Contains(buf.String(), "reason-182-marker") {
		t.Fatalf("invalidation reason not logged; log=%q", buf.String())
	}
}
