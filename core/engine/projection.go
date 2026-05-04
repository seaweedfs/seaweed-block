package engine

// Mode is the operator-visible mode derived from truth domains.
type Mode string

const (
	ModeHealthy    Mode = "healthy"
	ModeDegraded   Mode = "degraded"
	ModeRecovering Mode = "recovering"
	ModeIdle       Mode = "idle"
)

// ReplicaProjection is the operator-facing view derived from state.
// Intentionally descriptive. Must NEVER be reused as control truth.
type ReplicaProjection struct {
	Mode             Mode
	RecoveryDecision RecoveryDecision
	SessionKind      SessionKind
	SessionPhase     SessionPhase
	Epoch            uint64
	EndpointVersion  uint64
	R                uint64
	S                uint64
	H                uint64
	Reason           string
}

// DeriveProjection computes the projection from current state.
// Pure function — no side effects, no mutation.
func DeriveProjection(st *ReplicaState) ReplicaProjection {
	p := ReplicaProjection{
		Epoch:            st.Identity.Epoch,
		EndpointVersion:  st.Identity.EndpointVersion,
		RecoveryDecision: st.Recovery.Decision,
		SessionKind:      st.Session.Kind,
		SessionPhase:     st.Session.Phase,
		R:                st.Recovery.R,
		S:                st.Recovery.S,
		H:                st.Recovery.H,
		Reason:           st.Recovery.DecisionReason,
	}

	switch {
	case !st.Identity.MemberPresent:
		p.Mode = ModeIdle
	case st.Session.Phase == PhaseRunning || st.Session.Phase == PhaseStarting:
		p.Mode = ModeRecovering
	case st.Publication.Healthy:
		p.Mode = ModeHealthy
	case st.Recovery.Decision == DecisionNone &&
		st.Reachability.Status == ProbeReachable &&
		(st.Identity.Epoch > st.Reachability.FencedEpoch ||
			recoveredReplicaWaitingForPostCloseAck(st)):
		// P14 S1: caught-up but fence not yet acked. Transitional
		// — not healthy yet, but nothing is wrong. G9C extends the
		// same transitional shape to a recovered replica waiting for
		// post-close durable ack / live-feed continuity evidence.
		p.Mode = ModeRecovering
	default:
		p.Mode = ModeDegraded
	}

	return p
}
