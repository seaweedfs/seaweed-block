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
	mu             sync.Mutex
	state          engine.ReplicaState
	executor       CommandExecutor
	trace          []engine.TraceEntry // accumulated trace for diagnosis
	cmdLog         []string            // all commands executed
	startTimeout   time.Duration
	startWatchdogs map[uint64]*time.Timer
	watchdogLog    []WatchdogEvent

	// fenceInFlight: at most one fence attempt outstanding per
	// fence LINEAGE — keyed by (replicaID, epoch, endpointVersion).
	// Value is the sessionID of the in-flight fence; used to match
	// the returning FenceResult to the live attempt and drop
	// callbacks for superseded attempts.
	//
	// IMPORTANT: keying by lineage (not by replicaID alone) prevents
	// a newer-epoch fence from being starved by an older in-flight
	// fence. If identity advances mid-fence, decide() emits a new
	// FenceAtEpoch at the new lineage — which occupies its own slot
	// and is dispatched in parallel with the older one. The older
	// callback is dropped by the engine as stale
	// (applyFenceCompleted checks e.Epoch >= Identity.Epoch); the
	// newer one advances FencedEpoch normally.
	//
	// Same-lineage duplicates (engine re-emits FenceAtEpoch at the
	// same (epoch, endpointVersion) on a later probe while the
	// first attempt is still pending) are dropped by the slot check.
	fenceInFlight map[fenceKey]uint64
}

// fenceKey is the lineage key for fence dedupe. Two fence attempts
// are "the same attempt" iff their (replicaID, epoch, endpointVersion)
// tuples are equal. Newer-epoch fences therefore never dedupe against
// older-epoch fences and cannot be starved by them.
type fenceKey struct {
	replicaID       string
	epoch           uint64
	endpointVersion uint64
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
		fenceInFlight:  make(map[fenceKey]uint64),
	}
	exec.SetOnSessionStart(func(result SessionStartResult) {
		a.OnSessionStart(result)
	})
	// Wire the close callback: executor → adapter.OnSessionClose → engine.
	exec.SetOnSessionClose(func(result SessionCloseResult) {
		a.OnSessionClose(result)
	})
	// Wire the fence callback: executor → adapter.OnFenceComplete → engine.
	exec.SetOnFenceComplete(func(result FenceResult) {
		a.OnFenceComplete(result)
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
// only two paths that can produce terminal semantic truth.
func (a *VolumeReplicaAdapter) OnSessionClose(result SessionCloseResult) ApplyLog {
	a.clearStartWatchdog(result.SessionID, WatchdogClearClose)
	return a.applyBatchAndExecute([]engine.Event{NormalizeSessionClose(result)}, "SessionClose")
}

// OnSessionStart processes a runtime signal that a prepared session has
// actually begun executing.
func (a *VolumeReplicaAdapter) OnSessionStart(result SessionStartResult) ApplyLog {
	a.clearStartWatchdog(result.SessionID, WatchdogClearStart)
	return a.applyBatchAndExecute([]engine.Event{NormalizeSessionStart(result)}, "SessionStart")
}

// OnFenceComplete processes a fence outcome from the transport.
// Releases the in-flight slot (keyed by lineage) and normalizes to
// FenceCompleted (success) or FenceFailed (failure) into the engine.
// Callbacks whose (replicaID, epoch, endpointVersion) are not tracked,
// or whose sessionID does not match the tracked attempt for that
// lineage (e.g. a late straggler after the slot was reassigned by an
// explicit clear), are dropped silently.
func (a *VolumeReplicaAdapter) OnFenceComplete(result FenceResult) ApplyLog {
	key := fenceKey{
		replicaID:       result.ReplicaID,
		epoch:           result.Epoch,
		endpointVersion: result.EndpointVersion,
	}
	a.mu.Lock()
	trackedSessionID, ok := a.fenceInFlight[key]
	if ok && trackedSessionID == result.SessionID {
		delete(a.fenceInFlight, key)
	}
	a.mu.Unlock()
	if !ok || trackedSessionID != result.SessionID {
		// Superseded / late callback: drop silently. The engine
		// already moved on (or never expected this attempt).
		return ApplyLog{EventKind: "fence_callback_dropped"}
	}
	var ev engine.Event
	if result.Success {
		ev = engine.FenceCompleted{
			ReplicaID:       result.ReplicaID,
			Epoch:           result.Epoch,
			EndpointVersion: result.EndpointVersion,
		}
	} else {
		ev = engine.FenceFailed{
			ReplicaID:       result.ReplicaID,
			Epoch:           result.Epoch,
			EndpointVersion: result.EndpointVersion,
			Reason:          result.FailReason,
		}
	}
	return a.applyBatchAndExecute([]engine.Event{ev}, "FenceComplete")
}

// acquireFenceSlot atomically claims the fence slot for the full
// fence lineage (replicaID, epoch, endpointVersion). Returns false
// if a fence at the SAME lineage is already running — the duplicate
// FenceAtEpoch command from the engine (e.g. re-emitted on a later
// probe while the first attempt is still pending) is dropped.
//
// Critically, a fence at a DIFFERENT lineage (newer epoch or newer
// endpoint) acquires its own slot and is dispatched — it is NOT
// starved by an older in-flight fence. This is the bounded-closure
// fix for the "newer-epoch fence dropped behind older in-flight"
// linger bug.
func (a *VolumeReplicaAdapter) acquireFenceSlot(replicaID string, epoch, endpointVersion, sessionID uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fenceKey{
		replicaID:       replicaID,
		epoch:           epoch,
		endpointVersion: endpointVersion,
	}
	if _, inFlight := a.fenceInFlight[key]; inFlight {
		return false
	}
	a.fenceInFlight[key] = sessionID
	return true
}

func (a *VolumeReplicaAdapter) releaseFenceSlot(replicaID string, epoch, endpointVersion uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fenceKey{
		replicaID:       replicaID,
		epoch:           epoch,
		endpointVersion: endpointVersion,
	}
	delete(a.fenceInFlight, key)
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
		case engine.FenceAtEpoch:
			// Fence mints its own sessionID so the lineage wire frame
			// carries a unique tuple. No SessionPrepared event —
			// fence does NOT go through the session lifecycle
			// (no Starting/Running/Completed phases), it's a
			// command + event pair.
			sid := sessionIDCounter.Add(1)
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

	case engine.FenceAtEpoch:
		// Dedupe keyed by full fence lineage (replicaID, epoch,
		// endpointVersion). Same-lineage duplicates (engine
		// re-emits on a later probe) are dropped. Different-
		// lineage attempts (e.g. newer epoch after identity
		// advance) claim their own slot — the older in-flight
		// fence does NOT starve them.
		if !a.acquireFenceSlot(c.ReplicaID, c.Epoch, c.EndpointVersion, q.sessionID) {
			return
		}
		err := a.executor.Fence(c.ReplicaID, q.sessionID, c.Epoch, c.EndpointVersion)
		if err != nil {
			// Synchronous dispatch failure — normalize to
			// FenceFailed and release this lineage's slot.
			a.releaseFenceSlot(c.ReplicaID, c.Epoch, c.EndpointVersion)
			a.OnFenceComplete(FenceResult{
				ReplicaID:       c.ReplicaID,
				SessionID:       q.sessionID,
				Epoch:           c.Epoch,
				EndpointVersion: c.EndpointVersion,
				Success:         false,
				FailReason:      fmt.Sprintf("fence_dispatch_failed: %v", err),
			})
		}
	}
}

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
			a.mu.Unlock()
			return
		}
		if current != timer {
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

// OnRemoval processes a master removal event. The master has
// decided this adapter no longer manages this replica. Sessions
// are invalidated, recovery is cleared, and the publication state
// is withdrawn. This is the old primary's demotion path.
func (a *VolumeReplicaAdapter) OnRemoval(replicaID, reason string) ApplyLog {
	return a.applyBatchAndExecute([]engine.Event{
		engine.ReplicaRemoved{ReplicaID: replicaID, Reason: reason},
	}, "ReplicaRemoved")
}

// WatchdogKind names a lifecycle point in the start-timeout watchdog.
type WatchdogKind string

const (
	WatchdogArm        WatchdogKind = "arm"
	WatchdogClearStart WatchdogKind = "clear_started"
	WatchdogClearClose WatchdogKind = "clear_closed"
	WatchdogFire       WatchdogKind = "fire"
	WatchdogFireNoop   WatchdogKind = "fire_noop"
	WatchdogSupersede  WatchdogKind = "supersede"
)

// WatchdogEvent records one lifecycle event from the start-timeout
// watchdog. Used by the ops inspection surface and tests.
type WatchdogEvent struct {
	Kind      WatchdogKind
	SessionID uint64
	Detail    string
}

// WatchdogLog returns a snapshot of the watchdog lifecycle events.
func (a *VolumeReplicaAdapter) WatchdogLog() []WatchdogEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]WatchdogEvent, len(a.watchdogLog))
	copy(cp, a.watchdogLog)
	return cp
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
