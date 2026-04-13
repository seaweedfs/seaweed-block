package engine

// Command is an action the engine requests from the runtime.
// Commands are execution requests, not semantic conclusions.
type Command interface {
	commandKind() string
}

// ProbeReplica: request the runtime to probe the replica's reachability.
type ProbeReplica struct {
	ReplicaID string
	DataAddr  string
	CtrlAddr  string
}

func (ProbeReplica) commandKind() string { return "ProbeReplica" }

// StartCatchUp: request the runtime to start a catch-up session.
// May only be emitted from bounded R/S/H facts, not from transport errors.
type StartCatchUp struct {
	ReplicaID string
	TargetLSN uint64
}

func (StartCatchUp) commandKind() string { return "StartCatchUp" }

// StartRebuild: request the runtime to start a full rebuild session.
// May only be emitted from bounded R/S/H facts, not from transport errors.
type StartRebuild struct {
	ReplicaID string
	TargetLSN uint64
}

func (StartRebuild) commandKind() string { return "StartRebuild" }

// InvalidateSession: request the runtime to invalidate a stale session.
type InvalidateSession struct {
	ReplicaID string
	SessionID uint64
	Reason    string
}

func (InvalidateSession) commandKind() string { return "InvalidateSession" }

// PublishHealthy: declare this replica healthy for external consumption.
type PublishHealthy struct {
	ReplicaID string
}

func (PublishHealthy) commandKind() string { return "PublishHealthy" }

// PublishDegraded: declare this replica degraded for external consumption.
type PublishDegraded struct {
	ReplicaID string
	Reason    string
}

func (PublishDegraded) commandKind() string { return "PublishDegraded" }

// CommandKind returns the string name of a command for tracing.
func CommandKind(c Command) string {
	if c == nil {
		return "<nil>"
	}
	return c.commandKind()
}
