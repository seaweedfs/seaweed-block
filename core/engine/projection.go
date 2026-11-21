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
	Mode             Mode             // the engine mode (idle, recovering, healthy, degraded)
	RecoveryDecision RecoveryDecision // the recovery class the R/S/H boundaries call for
	SessionKind      SessionKind      // the kind of the current recovery session, if any
	SessionPhase     SessionPhase     // its phase
	Epoch            uint64           // the authority epoch
	EndpointVersion  uint64           // the endpoint version within the epoch
	R                uint64           // the replica's durable boundary (LSN)
	S                uint64           // the primary's recoverable start boundary (WAL tail LSN)
	H                uint64           // the primary's target boundary (head LSN)
	Reason           string           // why the engine is in this mode or decision
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
