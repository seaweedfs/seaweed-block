package replication

import "testing"

// T4c-2 peer state machine tests. Pins the transition table inscribed
// in `replicaStateTransitionAllowed` per memo §2.2:
//
//   Healthy → CatchingUp        (probe → catch-up)
//   CatchingUp → Healthy         (catch-up done-ack)
//   CatchingUp → NeedsRebuild    (ErrWALRecycled OR retry exhausted)
//   Degraded → CatchingUp        (probe after transient drop)
//   Degraded → NeedsRebuild      (gap-too-large)
//   NeedsRebuild → (terminal in T4c)

func TestReplicaStateTransition_AllowedSet(t *testing.T) {
	allowed := []struct {
		prev, next ReplicaState
		label      string
	}{
		{ReplicaUnknown, ReplicaHealthy, "Unknown→Healthy (init)"},
		{ReplicaHealthy, ReplicaDegraded, "Healthy→Degraded (T4a)"},
		{ReplicaDegraded, ReplicaHealthy, "Degraded→Healthy (recovery completed)"},
		{ReplicaHealthy, ReplicaCatchingUp, "Healthy→CatchingUp (probe)"},
		{ReplicaCatchingUp, ReplicaHealthy, "CatchingUp→Healthy (done-ack)"},
		{ReplicaCatchingUp, ReplicaNeedsRebuild, "CatchingUp→NeedsRebuild (escalate)"},
		{ReplicaDegraded, ReplicaCatchingUp, "Degraded→CatchingUp (transient drop reprobe)"},
		{ReplicaDegraded, ReplicaNeedsRebuild, "Degraded→NeedsRebuild (gap-too-large)"},
	}
	for _, tc := range allowed {
		if !replicaStateTransitionAllowed(tc.prev, tc.next) {
			t.Errorf("%s: transition %s → %s rejected, want allowed",
				tc.label, tc.prev, tc.next)
		}
	}
}

func TestReplicaStateTransition_RejectedSet(t *testing.T) {
	rejected := []struct {
		prev, next ReplicaState
		label      string
	}{
		{ReplicaNeedsRebuild, ReplicaHealthy, "NeedsRebuild→Healthy (TERMINAL in T4c)"},
		{ReplicaNeedsRebuild, ReplicaCatchingUp, "NeedsRebuild→CatchingUp (TERMINAL in T4c)"},
		{ReplicaNeedsRebuild, ReplicaDegraded, "NeedsRebuild→Degraded (TERMINAL in T4c)"},
		{ReplicaHealthy, ReplicaNeedsRebuild, "Healthy→NeedsRebuild (must go through Degraded or CatchingUp first)"},
	}
	for _, tc := range rejected {
		if replicaStateTransitionAllowed(tc.prev, tc.next) {
			t.Errorf("%s: transition %s → %s allowed, want rejected",
				tc.label, tc.prev, tc.next)
		}
	}
}

func TestReplicaStateTransition_NeedsRebuildIsTerminalInT4c(t *testing.T) {
	for _, next := range []ReplicaState{
		ReplicaUnknown, ReplicaHealthy, ReplicaDegraded, ReplicaCatchingUp,
	} {
		if replicaStateTransitionAllowed(ReplicaNeedsRebuild, next) {
			t.Errorf("NeedsRebuild MUST be terminal in T4c; got transition to %s allowed", next)
		}
	}
	// Sanity: idempotent self-transition is fine.
	if !replicaStateTransitionAllowed(ReplicaNeedsRebuild, ReplicaNeedsRebuild) {
		t.Error("NeedsRebuild→NeedsRebuild self-transition should be idempotent-allowed")
	}
}

func TestReplicaStateTransition_StringForms(t *testing.T) {
	cases := []struct {
		state ReplicaState
		want  string
	}{
		{ReplicaHealthy, "healthy"},
		{ReplicaDegraded, "degraded"},
		{ReplicaCatchingUp, "catching_up"},
		{ReplicaNeedsRebuild, "needs_rebuild"},
		{ReplicaUnknown, "unknown"},
	}
	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("ReplicaState(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}
