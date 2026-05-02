package adapter

import (
	"fmt"
	stdlog "log"
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
	fenceTimeout   time.Duration
	fenceWatchdogs map[fenceKey]*time.Timer
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
const defaultFenceTimeout = 15 * time.Second

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
		fenceTimeout:   defaultFenceTimeout,
		fenceWatchdogs: make(map[fenceKey]*time.Timer),
		fenceInFlight:  make(map[fenceKey]uint64),
	}
	exec.SetOnSessionStart(func(result SessionStartResult) {
		a.OnSessionStart(result)
	})
	// Wire the close callback: executor → adapter.OnSessionClose → engine.
	exec.SetOnSessionClose(func(result SessionCloseResult) {
		a.OnSessionClose(result)
	})
	if durableAckExec, ok := exec.(DurableAckCallbackSetter); ok {
		durableAckExec.SetOnDurableAck(func(result DurableAckResult) {
			a.OnDurableAck(result)
		})
	}
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

// OnDurableAck processes non-terminal durable progress from the executor.
func (a *VolumeReplicaAdapter) OnDurableAck(result DurableAckResult) ApplyLog {
	return a.applyBatchAndExecute([]engine.Event{NormalizeDurableAck(result)}, "DurableAck")
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
	timer := a.fenceWatchdogs[key]
	if ok && trackedSessionID == result.SessionID {
		delete(a.fenceWatchdogs, key)
		delete(a.fenceInFlight, key)
	}
	a.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
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
	delete(a.fenceWatchdogs, key)
	delete(a.fenceInFlight, key)
}

// armFenceWatchdog gives every dispatched fence a bounded local fate.
// If the executor never calls back with FenceCompleted/FenceFailed,
// the adapter synthesizes a FenceFailed("fence_timeout") for the same
// lineage. This closes the silent liveness hole where the caught-up
// route could otherwise wait forever with no explicit progress path.
func (a *VolumeReplicaAdapter) armFenceWatchdog(key fenceKey, sessionID uint64) {
	if sessionID == 0 || a.fenceTimeout <= 0 {
		return
	}

	var timer *time.Timer
	timer = time.AfterFunc(a.fenceTimeout, func() {
		a.mu.Lock()
		currentSessionID, ok := a.fenceInFlight[key]
		currentTimer := a.fenceWatchdogs[key]
		if !ok || currentSessionID != sessionID || currentTimer != timer {
			a.mu.Unlock()
			return
		}
		delete(a.fenceWatchdogs, key)
		a.mu.Unlock()

		a.OnFenceComplete(FenceResult{
			ReplicaID:       key.replicaID,
			SessionID:       sessionID,
			Epoch:           key.epoch,
			EndpointVersion: key.endpointVersion,
			Success:         false,
			FailReason:      "fence_timeout",
		})
	})

	a.mu.Lock()
	if old := a.fenceWatchdogs[key]; old != nil {
		old.Stop()
	}
	a.fenceWatchdogs[key] = timer
	a.mu.Unlock()
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
	// g7-debug: diagnostic logging for hardware bug investigation.
	// Trace which adapter (by Identity.ReplicaID) is processing what.
	a.mu.Lock()
	debugRID := a.state.Identity.ReplicaID
	a.mu.Unlock()
	stdlog.Printf("g7-debug: adapter[rid=%s].applyBatchAndExecute kind=%s nEvents=%d", debugRID, eventKind, len(events))
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

		// g7-debug: log engine state after each event so we can see
		// what decide() saw when it didn't emit.
		stdlog.Printf("g7-debug: adapter[rid=%s] post-event=%s state: R=%d S=%d H=%d Decision=%s Pinned=%t Reachable=%s Phase=%s SessionID=%d MemberPresent=%t Identity.Epoch=%d Identity.EV=%d nCmds=%d trace=%v",
			debugRID, engine.EventKind(ev),
			a.state.Recovery.R, a.state.Recovery.S, a.state.Recovery.H,
			a.state.Recovery.Decision, a.state.Recovery.RebuildPinned,
			a.state.Reachability.Status, a.state.Session.Phase, a.state.Session.SessionID,
			a.state.Identity.MemberPresent, a.state.Identity.Epoch, a.state.Identity.EndpointVersion,
			len(result.Commands), result.Trace)

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

	// g7-debug: report all commands collected (engine + session-prepared follow-ups).
	cmdKinds := make([]string, 0, len(queued))
	for _, q := range queued {
		cmdKinds = append(cmdKinds, engine.CommandKind(q.cmd))
	}
	stdlog.Printf("g7-debug: adapter[rid=%s] applyBatchAndExecute exit nQueued=%d cmds=%v", debugRID, len(queued), cmdKinds)

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
				NormalizeSessionPrepared(c.ReplicaID, sid, engine.SessionCatchUp, c.EffectiveFrontierHint()),
			)
			queued = append(queued, queuedCommand{cmd: cmd, sessionID: sid})
		case engine.StartRebuild:
			sid := sessionIDCounter.Add(1)
			events = append(events,
				NormalizeSessionPrepared(c.ReplicaID, sid, engine.SessionRebuild, c.EffectiveFrontierHint()),
			)
			queued = append(queued, queuedCommand{cmd: cmd, sessionID: sid})
		case engine.StartRecovery:
			// T4c-pre-B: forward-compatible dispatch path. Engine
			// emission of StartRecovery happens at T4c-3 muscle port;
			// today the legacy StartCatchUp / StartRebuild commands
			// remain the engine's emit shape. This case ensures the
			// adapter is ready when the migration lands.
			//
			// Map ContentKind to legacy SessionKind so the existing
			// session lifecycle (SessionPrepared / Starting / Running
			// / Completed) continues to work without churn:
			//   wal_delta   → SessionCatchUp
			//   full_extent → SessionRebuild
			//   partial_lba → SessionRebuild (Stage 2 placeholder)
			sid := sessionIDCounter.Add(1)
			sessionKind := engine.SessionCatchUp
			if c.ContentKind != engine.RecoveryContentWALDelta {
				sessionKind = engine.SessionRebuild
			}
			events = append(events,
				NormalizeSessionPrepared(c.ReplicaID, sid, sessionKind, c.EffectiveFrontierHint()),
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
		case engine.ProbeReplica:
			// T4c-1 (architect Option D): probe mints a transient
			// sessionID so the wire frame carries a full lineage
			// (round-26 symmetric-pair rule). Probe is non-mutating —
			// no SessionPrepared event, no executor session-table
			// registration, no OnSessionStart / OnSessionClose. The
			// sessionID is consumed only by `executor.Probe` to
			// construct the ProbeReq's RecoveryLineage.
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
		// T4c-1: pass the adapter-minted transient probe sessionID so
		// the executor can construct the ProbeReq's full lineage.
		probeSessionID := q.sessionID
		go func() {
			result := a.executor.Probe(c.ReplicaID, c.DataAddr, c.CtrlAddr, probeSessionID, c.Epoch, c.EndpointVersion)
			a.OnProbeResult(result)
		}()

	case engine.StartCatchUp:
		err := a.executor.StartCatchUp(c.ReplicaID, q.sessionID, c.Epoch, c.EndpointVersion, c.FromLSN, c.EffectiveFrontierHint())
		if err != nil {
			// T4d-1: synchronous dispatch failure → Transport kind.
			a.OnSessionClose(SessionCloseResult{
				ReplicaID:   c.ReplicaID,
				SessionID:   q.sessionID,
				Success:     false,
				FailureKind: engine.RecoveryFailureTransport,
				FailReason:  fmt.Sprintf("start_catchup_failed: %v", err),
			})
		}

	case engine.StartRebuild:
		err := a.executor.StartRebuild(c.ReplicaID, q.sessionID, c.Epoch, c.EndpointVersion, c.EffectiveFrontierHint())
		if err != nil {
			a.OnSessionClose(SessionCloseResult{
				ReplicaID:   c.ReplicaID,
				SessionID:   q.sessionID,
				Success:     false,
				FailureKind: engine.RecoveryFailureTransport,
				FailReason:  fmt.Sprintf("start_rebuild_failed: %v", err),
			})
		}

	case engine.StartRecovery:
		// T4c-pre-B unified dispatch: route through the executor's
		// StartRecoverySession entry. Policy is taken from the
		// command (engine populates via DefaultRuntimePolicyFor at
		// emit time). On synchronous dispatch failure, normalize to
		// SessionClose with Transport kind.
		err := a.executor.StartRecoverySession(
			c.ReplicaID,
			q.sessionID, c.Epoch, c.EndpointVersion, c.EffectiveFrontierHint(),
			c.ContentKind, c.RuntimePolicy,
		)
		if err != nil {
			a.OnSessionClose(SessionCloseResult{
				ReplicaID:   c.ReplicaID,
				SessionID:   q.sessionID,
				Success:     false,
				FailureKind: engine.RecoveryFailureTransport,
				FailReason:  fmt.Sprintf("start_recovery_failed[%s]: %v", c.ContentKind, err),
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
		key := fenceKey{
			replicaID:       c.ReplicaID,
			epoch:           c.Epoch,
			endpointVersion: c.EndpointVersion,
		}
		a.armFenceWatchdog(key, q.sessionID)
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
	case engine.StartCatchUp, engine.StartRebuild, engine.StartRecovery:
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
		// T4d-1: watchdog timeout → StartTimeout kind. Engine's
		// applySessionFailed bypasses retry for this kind.
		a.OnSessionClose(SessionCloseResult{
			ReplicaID:   replicaIDFromCommand(q.cmd),
			SessionID:   q.sessionID,
			Success:     false,
			FailureKind: engine.RecoveryFailureStartTimeout,
			FailReason:  "start_timeout",
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
	case engine.StartRecovery:
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
