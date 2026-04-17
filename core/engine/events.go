package engine

// Event is the input vocabulary for the mini engine.
// Each event carries bounded facts from one authority domain.
type Event interface {
	eventKind() string
}

// --- Identity events (authority: master) ---

// AssignmentObserved: master assigned this replica to the volume.
type AssignmentObserved struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	DataAddr        string
	CtrlAddr        string
}

func (AssignmentObserved) eventKind() string { return "AssignmentObserved" }

// EndpointObserved: master updated the replica's endpoint.
type EndpointObserved struct {
	ReplicaID       string
	EndpointVersion uint64
	DataAddr        string
	CtrlAddr        string
}

func (EndpointObserved) eventKind() string { return "EndpointObserved" }

// ReplicaRemoved: master removed this replica from the roster.
type ReplicaRemoved struct {
	ReplicaID string
	Reason    string
}

func (ReplicaRemoved) eventKind() string { return "ReplicaRemoved" }

// --- Reachability events (authority: primary) ---

// ProbeSucceeded: primary confirmed transport contact with replica.
type ProbeSucceeded struct {
	ReplicaID       string
	EndpointVersion uint64
	TransportEpoch  uint64
}

func (ProbeSucceeded) eventKind() string { return "ProbeSucceeded" }

// ProbeFailed: primary could not reach the replica.
type ProbeFailed struct {
	ReplicaID       string
	EndpointVersion uint64
	TransportEpoch  uint64
	Reason          string
}

func (ProbeFailed) eventKind() string { return "ProbeFailed" }

// --- Recovery-fact events (authority: primary) ---

// RecoveryFactsObserved: primary reports the R/S/H boundaries.
type RecoveryFactsObserved struct {
	ReplicaID       string
	EndpointVersion uint64
	TransportEpoch  uint64
	R               uint64 // replica durable boundary
	S               uint64 // primary WAL tail (recoverable start)
	H               uint64 // primary head (target)
}

func (RecoveryFactsObserved) eventKind() string { return "RecoveryFactsObserved" }

// --- Session events (authority: session owner / executor) ---

// SessionPrepared: a recovery session has been planned but not started.
type SessionPrepared struct {
	ReplicaID string
	SessionID uint64
	Kind      SessionKind
	TargetLSN uint64
}

func (SessionPrepared) eventKind() string { return "SessionPrepared" }

// SessionStarted: the recovery session began execution.
type SessionStarted struct {
	ReplicaID string
	SessionID uint64
}

func (SessionStarted) eventKind() string { return "SessionStarted" }

// SessionProgressObserved: the session made progress.
type SessionProgressObserved struct {
	ReplicaID   string
	SessionID   uint64
	AchievedLSN uint64
}

func (SessionProgressObserved) eventKind() string { return "SessionProgressObserved" }

// SessionClosedCompleted: the session finished successfully.
// This is one of only two terminal session events.
type SessionClosedCompleted struct {
	ReplicaID   string
	SessionID   uint64
	AchievedLSN uint64
}

func (SessionClosedCompleted) eventKind() string { return "SessionClosedCompleted" }

// SessionClosedFailed: the session failed.
// This is one of only two terminal session events.
type SessionClosedFailed struct {
	ReplicaID string
	SessionID uint64
	Reason    string
}

func (SessionClosedFailed) eventKind() string { return "SessionClosedFailed" }

// SessionInvalidated: a session was invalidated due to stale state.
type SessionInvalidated struct {
	ReplicaID string
	SessionID uint64
	Reason    string
}

func (SessionInvalidated) eventKind() string { return "SessionInvalidated" }

// --- Fence events (authority: transport) ---

// FenceCompleted: a FenceAtEpoch command completed successfully —
// the replica's lineage gate has now observed this epoch via a
// barrier ack. Advances Reachability.FencedEpoch.
type FenceCompleted struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
}

func (FenceCompleted) eventKind() string { return "FenceCompleted" }

// FenceFailed: a FenceAtEpoch command did not complete (timeout,
// dial failure, replica unreachable mid-barrier). Reachability.
// FencedEpoch is NOT advanced; the next probe will re-trigger
// decide(), which will re-emit FenceAtEpoch if the engine still
// thinks the replica is caught up under the newer epoch.
type FenceFailed struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	Reason          string
}

func (FenceFailed) eventKind() string { return "FenceFailed" }

// EventKind returns the string name of an event for tracing.
func EventKind(e Event) string {
	if e == nil {
		return "<nil>"
	}
	return e.eventKind()
}
