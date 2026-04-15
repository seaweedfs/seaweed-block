package adapter

// CommandExecutor is the interface that runtime muscles must satisfy.
// Each method corresponds to one engine command. The adapter calls
// these after engine.Apply() emits commands.
//
// Implementations wrap V2 transport muscles (WALShipper, RebuildTransport,
// etc.) but must NOT make semantic decisions. They execute and report.
//
// Session lifecycle: StartCatchUp/StartRebuild run asynchronously.
// The executor MUST call the registered OnSessionClose callback when
// the session completes or fails. Without this, terminal truth never
// reaches the engine and the replica stays stuck non-healthy.
type CommandExecutor interface {
	// SetOnSessionClose registers the callback for terminal session truth.
	// The adapter calls this during construction. The executor MUST call
	// it when any session completes or fails.
	SetOnSessionClose(fn OnSessionClose)

	// Probe dials the replica and collects transport/recovery facts.
	// Returns a ProbeResult with success/failure and R/S/H boundaries.
	// Must NOT decide recovery class — that's the engine's job.
	Probe(replicaID, dataAddr, ctrlAddr string, epoch, endpointVersion uint64) ProbeResult

	// StartCatchUp begins a catch-up session with the given sessionID and targetLSN.
	// The sessionID is assigned by the adapter (matches the engine's session truth).
	// Runs asynchronously; completion/failure MUST be reported via the
	// registered OnSessionClose callback using the SAME sessionID.
	StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error

	// StartRebuild begins a full rebuild session with the given sessionID and targetLSN.
	// Same contract as StartCatchUp.
	StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error

	// InvalidateSession cancels an active session.
	InvalidateSession(replicaID string, sessionID uint64, reason string)

	// PublishHealthy reports this replica as healthy.
	PublishHealthy(replicaID string)

	// PublishDegraded reports this replica as degraded.
	PublishDegraded(replicaID string, reason string)
}

// OnSessionClose is the callback signature for session completion/failure.
// The adapter registers this with the executor so terminal truth flows
// back through the engine's explicit close path.
type OnSessionClose func(SessionCloseResult)
