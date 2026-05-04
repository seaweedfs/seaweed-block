package volume

import (
	"sync"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// noopExecutor is the T0-scope CommandExecutor stub required by
// adapter.NewVolumeReplicaAdapter. It is NOT a data-path fake —
// T0 deliberately does not carry out fence / probe / session
// execution. Every method records its arguments for diagnostics
// and either returns success for inert operations or returns a
// bounded error for operations that would otherwise require a
// real runtime.
//
// Per T0 sketch §2.1 downgrade:
//
//	"T0 does NOT claim the adapter reaches ModeHealthy as proof
//	 of product readiness. T0 uses a noopExecutor that records
//	 commands but returns errors (or benign no-ops) for Probe /
//	 StartCatchUp / StartRebuild / Fence. This is EXPLICITLY a
//	 product-safe stub for T0 scope, NOT a data-path fake."
//
// This type moves to a real executor in G3 / G4+ tracks. It
// stays in core/host/volume as an embedded field on the Host so
// tests can inspect recorded commands.
type noopExecutor struct {
	mu sync.Mutex

	onFence adapter.OnFenceComplete
	onStart adapter.OnSessionStart
	onClose adapter.OnSessionClose

	commandLog []string
}

func newNoopExecutor() *noopExecutor {
	return &noopExecutor{}
}

// SetOnSessionStart / SetOnSessionClose / SetOnFenceComplete —
// adapter wires these during construction. We keep references
// but never invoke them from T0 code. A future real executor
// WILL invoke them when real session / fence results arrive.
func (e *noopExecutor) SetOnSessionStart(fn adapter.OnSessionStart)   { e.onStart = fn }
func (e *noopExecutor) SetOnSessionClose(fn adapter.OnSessionClose)   { e.onClose = fn }
func (e *noopExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) { e.onFence = fn }

// Probe returns a failed probe. That keeps the engine from
// considering the replica caught-up; Mode stays non-Healthy
// which is exactly the T0 acceptance claim (lineage known,
// runtime facts NOT known).
func (e *noopExecutor) Probe(replicaID, dataAddr, ctrlAddr string, sessionID, epoch, endpointVersion uint64) adapter.ProbeResult {
	e.record("Probe:" + replicaID)
	return adapter.ProbeResult{
		ReplicaID:       replicaID,
		Success:         false,
		EndpointVersion: endpointVersion,
		TransportEpoch:  epoch,
		FailReason:      "t0-noop-executor: probe not implemented in T0 scope",
	}
}

func (e *noopExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, fromLSN, frontierHint uint64) error {
	e.record("StartCatchUp:" + replicaID)
	return nil
}

func (e *noopExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, frontierHint uint64) error {
	e.record("StartRebuild:" + replicaID)
	return nil
}

func (e *noopExecutor) StartRecoverySession(
	replicaID string,
	sessionID, epoch, endpointVersion, frontierHint uint64,
	contentKind engine.RecoveryContentKind,
	policy engine.RecoveryRuntimePolicy,
) error {
	e.record("StartRecoverySession:" + replicaID + ":" + string(contentKind))
	return nil
}

func (e *noopExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {
	e.record("InvalidateSession:" + replicaID)
}

func (e *noopExecutor) PublishHealthy(replicaID string) {
	e.record("PublishHealthy:" + replicaID)
}

func (e *noopExecutor) PublishDegraded(replicaID string, reason string) {
	e.record("PublishDegraded:" + replicaID + ":" + reason)
}

func (e *noopExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	e.record("Fence:" + replicaID)
	// We deliberately do NOT fire onFence here. T0 does not
	// complete fences; projection stays non-Healthy.
	return nil
}

func (e *noopExecutor) record(s string) {
	e.mu.Lock()
	e.commandLog = append(e.commandLog, s)
	e.mu.Unlock()
}

// Commands returns a copy of the recorded command log. Used by
// tests to assert what reached the executor.
func (e *noopExecutor) Commands() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]string, len(e.commandLog))
	copy(cp, e.commandLog)
	return cp
}
