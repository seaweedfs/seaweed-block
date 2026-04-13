package adapter

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

// VolumeReplicaAdapter manages one volume × replica through the V3
// semantic engine. It is the single route from runtime observations
// to semantic decisions to command execution.
//
// The adapter loop:
//   1. Runtime observation arrives (assignment, probe result, session close)
//   2. Normalize into engine event(s)
//   3. Apply to engine state (under lock)
//   4. Collect emitted commands (under lock)
//   5. Release lock
//   6. Execute commands (outside lock — never call executor under mu)
//   7. Commands may produce async results → back to step 1
//
// There is exactly ONE route. No parallel convenience paths.
type VolumeReplicaAdapter struct {
	mu       sync.Mutex
	state    engine.ReplicaState
	executor CommandExecutor
	trace    []engine.TraceEntry // accumulated trace for diagnosis
	cmdLog   []string           // all commands executed

	// nextSessionID generates monotonic session IDs for this adapter.
	nextSessionID uint64
}

// NewVolumeReplicaAdapter creates a fresh adapter with the given executor.
// Registers the session close callback so terminal truth flows back
// through the engine's explicit close path.
func NewVolumeReplicaAdapter(exec CommandExecutor) *VolumeReplicaAdapter {
	a := &VolumeReplicaAdapter{
		executor:      exec,
		nextSessionID: 1,
	}
	// Wire the close callback: executor → adapter.OnSessionClose → engine.
	exec.SetOnSessionClose(func(result SessionCloseResult) {
		a.OnSessionClose(result)
	})
	return a
}

// OnAssignment processes a master assignment. This is the identity
// truth ingress path.
func (a *VolumeReplicaAdapter) OnAssignment(info AssignmentInfo) ApplyLog {
	ev := NormalizeAssignment(info)
	return a.applyAndExecute(ev)
}

// OnProbeResult processes a transport probe result. This produces
// reachability + recovery facts for the engine.
//
// Same-observation batch rule: ProbeSucceeded and RecoveryFactsObserved
// from the same probe are applied as one atomic batch under a single
// lock hold. No unrelated event can interleave between them.
//
// The adapter NEVER decides recovery class — it normalizes facts,
// the engine decides.
func (a *VolumeReplicaAdapter) OnProbeResult(result ProbeResult) ApplyLog {
	events := NormalizeProbe(result)
	return a.applyBatchAndExecute(events)
}

// OnSessionClose processes a terminal session result. This is one of
// only two paths that can produce terminal semantic truth.
func (a *VolumeReplicaAdapter) OnSessionClose(result SessionCloseResult) ApplyLog {
	ev := NormalizeSessionClose(result)
	return a.applyAndExecute(ev)
}

// Projection returns the current operator-facing projection.
func (a *VolumeReplicaAdapter) Projection() engine.ReplicaProjection {
	a.mu.Lock()
	defer a.mu.Unlock()
	return engine.DeriveProjection(&a.state)
}

// CommandLog returns all commands executed so far (for testing).
func (a *VolumeReplicaAdapter) CommandLog() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]string, len(a.cmdLog))
	copy(cp, a.cmdLog)
	return cp
}

// Trace returns all accumulated trace entries (for diagnosis).
func (a *VolumeReplicaAdapter) Trace() []engine.TraceEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]engine.TraceEntry, len(a.trace))
	copy(cp, a.trace)
	return cp
}

// --- Internal: the single route ---

// applyBatchAndExecute applies multiple same-observation events as one
// atomic batch under a single lock hold. No unrelated event can
// interleave between them. Commands are collected from all events,
// then executed outside the lock.
func (a *VolumeReplicaAdapter) applyBatchAndExecute(events []engine.Event) ApplyLog {
	if len(events) == 1 {
		return a.applyAndExecute(events[0])
	}

	a.mu.Lock()

	var log ApplyLog
	var cmds []engine.Command
	sessionIDs := make(map[string]uint64)

	// Apply all events in the batch under one lock hold.
	for _, ev := range events {
		result := engine.Apply(&a.state, ev)
		a.trace = append(a.trace, result.Trace...)
		log.Trace = append(log.Trace, result.Trace...)
		log.Projection = result.Projection

		for _, cmd := range result.Commands {
			kind := engine.CommandKind(cmd)
			a.cmdLog = append(a.cmdLog, kind)
			log.Commands = append(log.Commands, kind)
			cmds = append(cmds, cmd)
		}
	}

	// Feed session lifecycle events for any Start* commands.
	sessionEvents := a.buildSessionEvents(cmds, sessionIDs)
	for _, sev := range sessionEvents {
		sr := engine.Apply(&a.state, sev)
		a.trace = append(a.trace, sr.Trace...)
		for _, cmd := range sr.Commands {
			kind := engine.CommandKind(cmd)
			a.cmdLog = append(a.cmdLog, kind)
			log.Commands = append(log.Commands, kind)
			cmds = append(cmds, cmd)
		}
	}

	a.mu.Unlock()

	// Execute commands outside the lock.
	for _, cmd := range cmds {
		a.executeCommand(cmd, sessionIDs)
	}

	return log
}

// applyAndExecute is the ONE route: event → engine (under lock) → commands (outside lock).
func (a *VolumeReplicaAdapter) applyAndExecute(ev engine.Event) ApplyLog {
	// Step 1: Apply under lock, collect commands.
	a.mu.Lock()
	result := engine.Apply(&a.state, ev)
	a.trace = append(a.trace, result.Trace...)

	var log ApplyLog
	log.EventKind = engine.EventKind(ev)
	log.Projection = result.Projection
	log.Trace = result.Trace

	// Collect command kinds under lock.
	cmds := make([]engine.Command, len(result.Commands))
	copy(cmds, result.Commands)
	for _, cmd := range cmds {
		kind := engine.CommandKind(cmd)
		a.cmdLog = append(a.cmdLog, kind)
		log.Commands = append(log.Commands, kind)
	}

	// Feed session lifecycle events back into engine while still under lock
	// (these are synchronous, no external calls).
	// Also capture session IDs so executeCommand doesn't need to re-lock.
	sessionIDs := make(map[string]uint64) // command key → sessionID
	sessionEvents := a.buildSessionEvents(cmds, sessionIDs)
	for _, sev := range sessionEvents {
		sr := engine.Apply(&a.state, sev)
		a.trace = append(a.trace, sr.Trace...)
		for _, cmd := range sr.Commands {
			kind := engine.CommandKind(cmd)
			a.cmdLog = append(a.cmdLog, kind)
			log.Commands = append(log.Commands, kind)
			cmds = append(cmds, cmd)
		}
	}
	a.mu.Unlock()

	// Step 2: Execute commands OUTSIDE the lock.
	// Session IDs were captured under lock — no re-locking needed.
	for _, cmd := range cmds {
		a.executeCommand(cmd, sessionIDs)
	}

	return log
}

// buildSessionEvents creates SessionPrepared/Started events for
// Start* commands and captures the assigned session IDs in the map.
// Called under lock — no external calls, no race.
func (a *VolumeReplicaAdapter) buildSessionEvents(cmds []engine.Command, sessionIDs map[string]uint64) []engine.Event {
	var events []engine.Event
	for _, cmd := range cmds {
		switch c := cmd.(type) {
		case engine.StartCatchUp:
			a.nextSessionID++
			sid := a.nextSessionID
			sessionIDs[c.ReplicaID] = sid
			events = append(events,
				NormalizeSessionPrepared(c.ReplicaID, sid, engine.SessionCatchUp, c.TargetLSN),
				NormalizeSessionStarted(c.ReplicaID, sid),
			)
		case engine.StartRebuild:
			a.nextSessionID++
			sid := a.nextSessionID
			sessionIDs[c.ReplicaID] = sid
			events = append(events,
				NormalizeSessionPrepared(c.ReplicaID, sid, engine.SessionRebuild, c.TargetLSN),
				NormalizeSessionStarted(c.ReplicaID, sid),
			)
		}
	}
	return events
}

// executeCommand dispatches one engine command to the executor.
// Called OUTSIDE the lock. Session IDs were captured under lock by
// buildSessionEvents — no re-locking needed, no race.
func (a *VolumeReplicaAdapter) executeCommand(cmd engine.Command, sessionIDs map[string]uint64) {
	switch c := cmd.(type) {
	case engine.ProbeReplica:
		go func() {
			result := a.executor.Probe(c.ReplicaID, c.DataAddr, c.CtrlAddr)
			a.OnProbeResult(result)
		}()

	case engine.StartCatchUp:
		sid := sessionIDs[c.ReplicaID]
		err := a.executor.StartCatchUp(c.ReplicaID, sid, c.TargetLSN)
		if err != nil {
			a.OnSessionClose(SessionCloseResult{
				ReplicaID:  c.ReplicaID,
				SessionID:  sid,
				Success:    false,
				FailReason: fmt.Sprintf("start_catchup_failed: %v", err),
			})
		}

	case engine.StartRebuild:
		sid := sessionIDs[c.ReplicaID]
		err := a.executor.StartRebuild(c.ReplicaID, sid, c.TargetLSN)
		if err != nil {
			a.OnSessionClose(SessionCloseResult{
				ReplicaID:  c.ReplicaID,
				SessionID:  sid,
				Success:    false,
				FailReason: fmt.Sprintf("start_rebuild_failed: %v", err),
			})
		}

	case engine.InvalidateSession:
		a.executor.InvalidateSession(c.ReplicaID, c.SessionID, c.Reason)

	case engine.PublishHealthy:
		a.executor.PublishHealthy(c.ReplicaID)

	case engine.PublishDegraded:
		a.executor.PublishDegraded(c.ReplicaID, c.Reason)
	}
}

// ApplyLog records what happened during one adapter operation.
type ApplyLog struct {
	EventKind  string
	Commands   []string
	Projection engine.ReplicaProjection
	Trace      []engine.TraceEntry
}

// Merge appends another log's results.
func (l *ApplyLog) Merge(other ApplyLog) {
	l.Commands = append(l.Commands, other.Commands...)
	l.Trace = append(l.Trace, other.Trace...)
	l.Projection = other.Projection // last one wins
}
