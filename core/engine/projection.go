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
	default:
		p.Mode = ModeDegraded
	}

	return p
}
