// Package engine implements the V3 mini semantic core.
//
// The engine is a deterministic reducer: Apply(state, event) → (state, commands, projection).
// It reasons about one volume × replica pair. No runtime, no goroutines, no TCP.
//
// Five truth domains:
//   - Identity: who this replica is (from master)
//   - Reachability: can primary talk to it (from probe/transport)
//   - Recovery: what recovery class is needed (from R/S/H boundaries)
//   - Session: is there an active recovery contract (from session lifecycle)
//   - Publication: what to project outward (derived, never fed back)
package engine

// ReplicaState is the full semantic state for one volume × replica.
type ReplicaState struct {
	Identity     IdentityTruth
	Reachability ReachabilityTruth
	Recovery     RecoveryTruth
	Session      SessionTruth
	Publication  PublicationTruth
}

// IdentityTruth: who this replica is. Authority: master.
type IdentityTruth struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	DataAddr        string
	CtrlAddr        string
	MemberPresent   bool
}

// ProbeStatus represents the current reachability state.
type ProbeStatus string

const (
	ProbeUnknown     ProbeStatus = "unknown"
	ProbeProbing     ProbeStatus = "probing"
	ProbeReachable   ProbeStatus = "reachable"
	ProbeUnreachable ProbeStatus = "unreachable"
)

// ContactKind represents the type of last successful contact.
type ContactKind string

const (
	ContactNone    ContactKind = "none"
	ContactShip    ContactKind = "ship"
	ContactBarrier ContactKind = "barrier"
	ContactProbe   ContactKind = "probe"
	ContactCtrlAck ContactKind = "ctrl_ack"
)

// ReachabilityTruth: can primary talk to the replica. Authority: primary.
type ReachabilityTruth struct {
	Status                  ProbeStatus
	LastContactKind         ContactKind
	ObservedEndpointVersion uint64
	TransportEpoch          uint64
}

// RecoveryDecision represents the recovery classification.
type RecoveryDecision string

const (
	DecisionUnknown RecoveryDecision = "unknown"
	DecisionNone    RecoveryDecision = "none"
	DecisionCatchUp RecoveryDecision = "catch_up"
	DecisionRebuild RecoveryDecision = "rebuild"
)

// RecoveryTruth: what recovery is needed. Authority: primary.
type RecoveryTruth struct {
	R              uint64           // replica durable/achieved boundary
	S              uint64           // primary recoverable start boundary (WAL tail)
	H              uint64           // primary target boundary (head)
	Decision       RecoveryDecision
	DecisionReason string
}

// SessionKind identifies the type of recovery session.
type SessionKind string

const (
	SessionNone    SessionKind = ""
	SessionCatchUp SessionKind = "catch_up"
	SessionRebuild SessionKind = "rebuild"
)

// SessionPhase tracks the lifecycle of a recovery session.
type SessionPhase string

const (
	PhaseNone      SessionPhase = "" // zero value = no session
	PhaseStarting  SessionPhase = "starting"
	PhaseRunning   SessionPhase = "running"
	PhaseCompleted SessionPhase = "completed"
	PhaseFailed    SessionPhase = "failed"
)

// SessionTruth: is there an active recovery contract. Authority: session owner.
type SessionTruth struct {
	SessionID     uint64
	Kind          SessionKind
	TargetLSN     uint64
	AchievedLSN   uint64
	Phase         SessionPhase
	FailureReason string
}

// PublicationTruth: what to project outward. Derived, never fed back.
type PublicationTruth struct {
	Publishable    bool
	Healthy        bool
	Degraded       bool
	NeedsAttention bool
}
