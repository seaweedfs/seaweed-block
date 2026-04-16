package adapter

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

// VolumeReplicaAdapter manages one volume × replica through the V3
// semantic engine. It is the single route from runtime observations
// to semantic decisions to command execution.
//
// The adapter loop:
//  1. Runtime observation arrives (assignment, probe result, session close)
//  2. Normalize into engine event(s)
//  3. Apply to engine state (under lock)
//  4. Collect emitted commands (under lock)
//  5. Release lock
//  6. Execute commands (outside lock — never call executor under mu)
//  7. Commands may produce async results → back to step 1
//
// There is exactly ONE route. No parallel convenience paths.
type VolumeReplicaAdapter struct {
	mu             sync.Mutex
	state          engine.ReplicaState
	executor       CommandExecutor
	trace          []engine.TraceEntry // accumulated trace for diagnosis
	cmdLog         []string            // all commands executed
	startTimeout   time.Duration
	startWatchdogs map[uint64]*time.Timer

	// watchdogLog records lifecycle-visible watchdog events
	// (arm / clear / fire / fire-no-op) for diagnosis. Not an
	// operator surface; exists so tests and trace dumps can
	// prove the timer did or did not do a thing.
	watchdogLog []WatchdogEvent
}

// WatchdogKind names a point in the start-timeout watchdog's
// lifecycle. Used only in evidence/trace paths.
type WatchdogKind string

const (
	WatchdogArm         WatchdogKind = "arm"
	WatchdogClearStart  WatchdogKind = "clear_started"
	WatchdogClearClose  WatchdogKind = "clear_closed"
	WatchdogFire        WatchdogKind = "fire"
	WatchdogFireNoop    WatchdogKind = "fire_noop"
	WatchdogSupersede   WatchdogKind = "supersede"
)

// WatchdogEvent is one entry in the watchdog log. Kind says what
// happened; Detail gives the reason (e.g., "phase=running" when a
// fire was suppressed because a real start already arrived).
type WatchdogEvent struct {
	Kind      WatchdogKind
	SessionID uint64
	Detail    string
}

var sessionIDCounter atomic.Uint64

const defaultSessionStartTimeout = 5 * time.Second

type queuedCommand struct {
	cmd       engine.Command
	sessionID uint64
}

// NewVolumeReplicaAdapter creates a fresh adapter with the given executor.
// Registers lifecycle callbacks so runtime start/close truth flows back
// through the engine's explicit paths.
func NewVolumeReplicaAdapter(exec CommandExecutor) *VolumeReplicaAdapter {
	a := &VolumeReplicaAdapter{
		executor:       exec,
		startTimeout:   defaultSessionStartTimeout,
		startWatchdogs: make(map[uint64]*time.Timer),
	}
	exec.SetOnSessionStart(func(result SessionStartResult) {
		a.OnSessionStart(result)
	})
	// Wire the close callback: executor → adapter.OnSessionClose → engine.
	exec.SetOnSessionClose(func(result SessionCloseResult) {
		a.OnSessionClose(result)
	})
	return a
}

// OnAssignment processes a master assignment. This is the identity
// truth ingress path.
func (a *VolumeReplicaAdapter) OnAssignment(info AssignmentInfo) ApplyLog {
	return a.applyBatchAndExecute([]engine.Event{NormalizeAssignment(info)}, "AssignmentObserved")
}

// OnProbeResult processes a transport probe result. This produces
// reachability + recovery facts for the engine.
// The adapter NEVER decides recovery class — it normalizes facts,
// the engine decides.
func (a *VolumeReplicaAdapter) OnProbeResult(result ProbeResult) ApplyLog {
	return a.applyBatchAndExecute(NormalizeProbe(result), "batch:ProbeResult")
}

// OnSessionClose processes a terminal session result. This is one of
// only two paths that can produce terminal semantic truth. Clears the
// start-timeout watchdog so a late timer cannot fire after the
// session already has terminal truth.
func (a *VolumeReplicaAdapter) OnSessionClose(result SessionCloseResult) ApplyLog {
	a.clearStartWatchdog(result.SessionID, WatchdogClearClose)
	return a.applyBatchAndExecute([]engine.Event{NormalizeSessionClose(result)}, "SessionClose")
}

// OnSessionStart processes a runtime signal that a prepared session has
// actually begun executing. Clears the start-timeout watchdog so it
// cannot later kill a session that is legitimately running.
func (a *VolumeReplicaAdapter) OnSessionStart(result SessionStartResult) ApplyLog {
	a.clearStartWatchdog(result.SessionID, WatchdogClearStart)
	return a.applyBatchAndExecute([]engine.Event{NormalizeSessionStart(result)}, "SessionStart")
}

// OnRemoval processes a master removal event. The master has
// decided this adapter no longer manages this replica. Sessions
// are invalidated, recovery is cleared, and the publication state
// is withdrawn. This is the old primary's demotion path in P12.
func (a *VolumeReplicaAdapter) OnRemoval(replicaID, reason string) ApplyLog {
	return a.applyBatchAndExecute([]engine.Event{
		engine.ReplicaRemoved{ReplicaID: replicaID, Reason: reason},
	}, "ReplicaRemoved")
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

// applyBatchAndExecute is the ONE route: events from one observation are
// applied as one atomic batch under the adapter lock, then emitted commands are
// executed outside the lock.
func (a *VolumeReplicaAdapter) applyBatchAndExecute(events []engine.Event, eventKind string) ApplyLog {
	// Step 1: Apply under lock, collect commands.
	a.mu.Lock()
	var log ApplyLog
	if len(events) > 1 {
		kinds := make([]string, 0, len(events))
		for _, ev := range events {
			kinds = append(kinds, engine.EventKind(ev))
		}
		log.EventKind = "batch:" + strings.Join(kinds, ",")
	} else {
		log.EventKind = eventKind
	}

	var cmds []engine.Command
	for _, ev := range events {
		result := engine.Apply(&a.state, ev)
		a.trace = append(a.trace, result.Trace...)
		log.Projection = result.Projection
		log.Trace = append(log.Trace, result.Trace...)

		for _, cmd := range result.Commands {
			kind := engine.CommandKind(cmd)
			a.cmdLog = append(a.cmdLog, kind)
			log.Commands = append(log.Commands, kind)
			cmds = append(cmds, cmd)
		}
	}

	// Feed session lifecycle events back into engine while still under lock
	// (these are synchronous, no external calls).
	sessionEvents, queued := a.prepareQueuedCommands(cmds)
	for _, sev := range sessionEvents {
		sr := engine.Apply(&a.state, sev)
		a.trace = append(a.trace, sr.Trace...)
		log.Projection = sr.Projection
		log.Trace = append(log.Trace, sr.Trace...)
		for _, cmd := range sr.Commands {
			kind := engine.CommandKind(cmd)
			a.cmdLog = append(a.cmdLog, kind)
			log.Commands = append(log.Commands, kind)
			queued = append(queued, queuedCommand{cmd: cmd})
		}
	}
	a.mu.Unlock()

	// Step 2: Execute commands OUTSIDE the lock.
	// This prevents deadlock when executors call back into the adapter.
	for _, cmd := range queued {
		a.armStartWatchdog(cmd)
		a.executeCommand(cmd)
	}

	return log
}

// prepareQueuedCommands assigns session lineage for Start* commands, creates
// their synchronous engine events, and returns the execution queue.
func (a *VolumeReplicaAdapter) prepareQueuedCommands(cmds []engine.Command) ([]engine.Event, []queuedCommand) {
	var events []engine.Event
	var queued []queuedCommand
	for _, cmd := range cmds {
		switch c := cmd.(type) {
		case engine.StartCatchUp:
			sid := sessionIDCounter.Add(1)
			events = append(events,
				NormalizeSessionPrepared(c.ReplicaID, sid, engine.SessionCatchUp, c.TargetLSN),
			)
			queued = append(queued, queuedCommand{cmd: cmd, sessionID: sid})
		case engine.StartRebuild:
			sid := sessionIDCounter.Add(1)
			events = append(events,
				NormalizeSessionPrepared(c.ReplicaID, sid, engine.SessionRebuild, c.TargetLSN),
			)
			queued = append(queued, queuedCommand{cmd: cmd, sessionID: sid})
		default:
			queued = append(queued, queuedCommand{cmd: cmd})
		}
	}
	return events, queued
}

// executeCommand dispatches one engine command to the executor.
// Called OUTSIDE the lock. Executor may call back asynchronously
// through the registered OnSessionClose callback.
func (a *VolumeReplicaAdapter) executeCommand(q queuedCommand) {
	switch c := q.cmd.(type) {
	case engine.ProbeReplica:
		go func() {
			result := a.executor.Probe(c.ReplicaID, c.DataAddr, c.CtrlAddr, c.Epoch, c.EndpointVersion)
			a.OnProbeResult(result)
		}()

	case engine.StartCatchUp:
		err := a.executor.StartCatchUp(c.ReplicaID, q.sessionID, c.Epoch, c.EndpointVersion, c.TargetLSN)
		if err != nil {
			a.OnSessionClose(SessionCloseResult{
				ReplicaID:  c.ReplicaID,
				SessionID:  q.sessionID,
				Success:    false,
				FailReason: fmt.Sprintf("start_catchup_failed: %v", err),
			})
		}

	case engine.StartRebuild:
		err := a.executor.StartRebuild(c.ReplicaID, q.sessionID, c.Epoch, c.EndpointVersion, c.TargetLSN)
		if err != nil {
			a.OnSessionClose(SessionCloseResult{
				ReplicaID:  c.ReplicaID,
				SessionID:  q.sessionID,
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

// armStartWatchdog arms the bounded start-timeout timer for a
// StartCatchUp / StartRebuild command. Rules (locked by P10):
//
//   - arm only for the two Start* kinds
//   - never arm for sessionID=0 (non-session commands)
//   - at fire time, check both timer identity (so a superseded
//     timer bails) AND engine state (so only sessions still in
//     PhaseStarting are failed)
//   - fire that finds the engine beyond PhaseStarting is a no-op
//     with an explicit watchdog evidence record, not silent
func (a *VolumeReplicaAdapter) armStartWatchdog(q queuedCommand) {
	if q.sessionID == 0 || a.startTimeout <= 0 {
		return
	}
	switch q.cmd.(type) {
	case engine.StartCatchUp, engine.StartRebuild:
	default:
		return
	}

	var timer *time.Timer
	timer = time.AfterFunc(a.startTimeout, func() {
		a.mu.Lock()
		current := a.startWatchdogs[q.sessionID]
		if current == nil {
			// Cleared by real start or close before firing.
			a.mu.Unlock()
			return
		}
		if current != timer {
			// A newer timer for the same sessionID was armed —
			// this old one must not act on current state.
			a.mu.Unlock()
			return
		}
		delete(a.startWatchdogs, q.sessionID)
		activeSession := a.state.Session.SessionID
		activePhase := a.state.Session.Phase
		shouldFail := activeSession == q.sessionID && activePhase == engine.PhaseStarting
		if shouldFail {
			a.watchdogLog = append(a.watchdogLog, WatchdogEvent{
				Kind:      WatchdogFire,
				SessionID: q.sessionID,
				Detail:    "start_timeout; phase=starting",
			})
		} else {
			a.watchdogLog = append(a.watchdogLog, WatchdogEvent{
				Kind:      WatchdogFireNoop,
				SessionID: q.sessionID,
				Detail:    fmt.Sprintf("active=%d phase=%s", activeSession, activePhase),
			})
		}
		a.mu.Unlock()

		if !shouldFail {
			return
		}
		a.OnSessionClose(SessionCloseResult{
			ReplicaID:  replicaIDFromCommand(q.cmd),
			SessionID:  q.sessionID,
			Success:    false,
			FailReason: "start_timeout",
		})
	})

	a.mu.Lock()
	if old := a.startWatchdogs[q.sessionID]; old != nil {
		old.Stop()
		a.watchdogLog = append(a.watchdogLog, WatchdogEvent{
			Kind:      WatchdogSupersede,
			SessionID: q.sessionID,
			Detail:    "replaced prior timer",
		})
	}
	a.startWatchdogs[q.sessionID] = timer
	a.watchdogLog = append(a.watchdogLog, WatchdogEvent{
		Kind:      WatchdogArm,
		SessionID: q.sessionID,
		Detail:    "timeout=" + a.startTimeout.String(),
	})
	a.mu.Unlock()
}

// clearStartWatchdog stops and removes the timer for sessionID.
// Caller passes the reason so the evidence log distinguishes
// cleared-by-start from cleared-by-close.
func (a *VolumeReplicaAdapter) clearStartWatchdog(sessionID uint64, reason WatchdogKind) {
	if sessionID == 0 {
		return
	}
	a.mu.Lock()
	timer := a.startWatchdogs[sessionID]
	if timer != nil {
		delete(a.startWatchdogs, sessionID)
		a.watchdogLog = append(a.watchdogLog, WatchdogEvent{
			Kind:      reason,
			SessionID: sessionID,
		})
	}
	a.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

// WatchdogLog returns a snapshot of the watchdog lifecycle events
// recorded so far. Tests and diagnostic dumps use this to prove a
// timer was armed, cleared, fired, or superseded.
func (a *VolumeReplicaAdapter) WatchdogLog() []WatchdogEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]WatchdogEvent, len(a.watchdogLog))
	copy(cp, a.watchdogLog)
	return cp
}

func replicaIDFromCommand(cmd engine.Command) string {
	switch c := cmd.(type) {
	case engine.StartCatchUp:
		return c.ReplicaID
	case engine.StartRebuild:
		return c.ReplicaID
	case engine.InvalidateSession:
		return c.ReplicaID
	case engine.PublishHealthy:
		return c.ReplicaID
	case engine.PublishDegraded:
		return c.ReplicaID
	case engine.ProbeReplica:
		return c.ReplicaID
	default:
		return ""
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
