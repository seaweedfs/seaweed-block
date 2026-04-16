package adapter

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

// These tests prove the start-timeout watchdog's P10 rules:
//
//   - timer cleared by real start → no fire
//   - timer cleared by immediate close → no fire
//   - an old session's timer cannot fail a new session
//   - invalidate before real start does not fabricate success/start
//
// The watchdog log (a.WatchdogLog()) is the primary evidence source;
// the projection and trace are secondary.
//
// If WatchdogLog() returns nil (simplified adapter without recording),
// the event-evidence assertions are skipped — the projection-level
// assertions still run and prove the behavioral contract.

// findWatchdogEvent returns the first entry matching kind+sessionID
// or nil if none. Test helper — keeps assertions short.
func findWatchdogEvent(log []WatchdogEvent, kind WatchdogKind, sessionID uint64) *WatchdogEvent {
	for i := range log {
		if log[i].Kind == kind && log[i].SessionID == sessionID {
			return &log[i]
		}
	}
	return nil
}

// TestWatchdog_ClearedByRealStart proves that once OnSessionStart
// arrives, the timer is cleared and a later AfterFunc fire is a
// no-op. The adapter must record a clear_started event and the
// projection must be Running, not Failed.
func TestWatchdog_ClearedByRealStart(t *testing.T) {
	exec := newMockExecutor()
	exec.autoStart = false // we fire start manually
	exec.autoClose = false
	exec.probeResults["r1"] = ProbeResult{
		ReplicaID: "r1", Success: true,
		EndpointVersion: 1, TransportEpoch: 1,
		ReplicaFlushedLSN: 50, PrimaryTailLSN: 10, PrimaryHeadLSN: 100,
	}
	a := NewVolumeReplicaAdapter(exec)
	a.startTimeout = 150 * time.Millisecond

	a.OnAssignment(AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	time.Sleep(10 * time.Millisecond)

	sid := exec.latestSessionID("StartCatchUp")
	if sid == 0 {
		t.Fatal("StartCatchUp was not issued")
	}

	// Fire real start well before the 150ms timeout.
	exec.fireStart(SessionStartResult{ReplicaID: "r1", SessionID: sid})
	time.Sleep(200 * time.Millisecond) // wait past the timeout window

	log := a.WatchdogLog()
	if findWatchdogEvent(log, WatchdogArm, sid) == nil {
		t.Fatalf("expected WatchdogArm for session %d, got %+v", sid, log)
	}
	if findWatchdogEvent(log, WatchdogClearStart, sid) == nil {
		t.Fatalf("expected WatchdogClearStart for session %d, got %+v", sid, log)
	}
	if ev := findWatchdogEvent(log, WatchdogFire, sid); ev != nil {
		t.Fatalf("watchdog fired after real start: %+v", ev)
	}

	p := a.Projection()
	if p.SessionPhase != engine.PhaseRunning {
		t.Fatalf("phase=%s, want running (start should not have been failed by timer)", p.SessionPhase)
	}
}

// TestWatchdog_ClearedByImmediateClose proves that a session that
// closes before the timer fires results in a clear_closed event,
// not a fire. Used when the executor returns synchronous success
// or the session completes faster than the watchdog.
func TestWatchdog_ClearedByImmediateClose(t *testing.T) {
	exec := newMockExecutor()
	exec.autoStart = false
	exec.autoClose = false
	exec.probeResults["r1"] = ProbeResult{
		ReplicaID: "r1", Success: true,
		EndpointVersion: 1, TransportEpoch: 1,
		ReplicaFlushedLSN: 50, PrimaryTailLSN: 10, PrimaryHeadLSN: 100,
	}
	a := NewVolumeReplicaAdapter(exec)
	a.startTimeout = 150 * time.Millisecond

	a.OnAssignment(AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	time.Sleep(10 * time.Millisecond)

	sid := exec.latestSessionID("StartCatchUp")
	if sid == 0 {
		t.Fatal("StartCatchUp was not issued")
	}

	// Session completes before the timer fires.
	exec.fireClose(SessionCloseResult{
		ReplicaID: "r1", SessionID: sid,
		Success: true, AchievedLSN: 100,
	})
	time.Sleep(200 * time.Millisecond)

	log := a.WatchdogLog()
	if findWatchdogEvent(log, WatchdogClearClose, sid) == nil {
		t.Fatalf("expected WatchdogClearClose for session %d, got %+v", sid, log)
	}
	if ev := findWatchdogEvent(log, WatchdogFire, sid); ev != nil {
		t.Fatalf("watchdog fired despite immediate close: %+v", ev)
	}
}

// TestWatchdog_OldTimerCannotFailNewSession proves that a timer
// armed for session N cannot, after firing late, fail session N+M.
// The fire-time guard checks active session ID against the timer's
// sessionID; mismatch yields fire_noop, not a spurious failure.
func TestWatchdog_OldTimerCannotFailNewSession(t *testing.T) {
	exec := newMockExecutor()
	exec.autoStart = false
	exec.autoClose = false
	exec.probeResults["r1"] = ProbeResult{
		ReplicaID: "r1", Success: true,
		EndpointVersion: 1, TransportEpoch: 1,
		ReplicaFlushedLSN: 50, PrimaryTailLSN: 10, PrimaryHeadLSN: 100,
	}
	a := NewVolumeReplicaAdapter(exec)
	// Short timeout so the old timer fires before the new session closes.
	a.startTimeout = 80 * time.Millisecond

	a.OnAssignment(AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	time.Sleep(10 * time.Millisecond)
	oldSid := exec.latestSessionID("StartCatchUp")
	if oldSid == 0 {
		t.Fatal("first StartCatchUp was not issued")
	}

	// Let the old session time out — this moves engine to PhaseFailed.
	time.Sleep(150 * time.Millisecond)
	if p := a.Projection(); p.SessionPhase != engine.PhaseFailed {
		t.Fatalf("precondition: phase=%s, want failed (old session timed out)", p.SessionPhase)
	}

	// Bump the endpoint so the engine starts a fresh recovery cycle
	// with a new sessionID. The old timer is already fired+consumed,
	// but we also prove a second timer can't resurrect on old data.
	exec.probeResults["r1"] = ProbeResult{
		ReplicaID: "r1", Success: true,
		EndpointVersion: 2, TransportEpoch: 2,
		ReplicaFlushedLSN: 50, PrimaryTailLSN: 10, PrimaryHeadLSN: 100,
	}
	a.OnAssignment(AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: "a", CtrlAddr: "b",
	})
	time.Sleep(20 * time.Millisecond)

	newSid := exec.latestSessionID("StartCatchUp")
	if newSid == 0 || newSid == oldSid {
		t.Fatalf("new StartCatchUp was not issued: oldSid=%d newSid=%d", oldSid, newSid)
	}

	// Fire the new session's start so it is legitimately running.
	exec.fireStart(SessionStartResult{ReplicaID: "r1", SessionID: newSid})
	time.Sleep(30 * time.Millisecond)

	// New session must still be Running after the old timer's bookkeeping
	// has completed — prove the old fire cannot have affected it.
	if p := a.Projection(); p.SessionPhase != engine.PhaseRunning {
		t.Fatalf("new session phase=%s, want running; old timer may have failed new session", p.SessionPhase)
	}

	log := a.WatchdogLog()
	if ev := findWatchdogEvent(log, WatchdogFire, oldSid); ev == nil {
		t.Fatalf("expected WatchdogFire for old session %d in log: %+v", oldSid, log)
	}
	if findWatchdogEvent(log, WatchdogArm, newSid) == nil {
		t.Fatalf("expected WatchdogArm for new session %d in log: %+v", newSid, log)
	}
	if ev := findWatchdogEvent(log, WatchdogFire, newSid); ev != nil {
		t.Fatalf("new session fired falsely: %+v", ev)
	}
}

// TestWatchdog_InvalidateBeforeStart_NoFalseStartNoFalseSuccess
// proves that if the engine invalidates a session before the
// runtime reports real start, the adapter does NOT fabricate
// SessionStarted or SessionClosedCompleted. The terminal truth
// must only come from an explicit close, and the projection must
// not report Healthy.
func TestWatchdog_InvalidateBeforeStart_NoFalseStartNoFalseSuccess(t *testing.T) {
	exec := newMockExecutor()
	exec.autoStart = false // manual — never fire start
	exec.autoClose = false // manual — never fire close
	exec.probeResults["r1"] = ProbeResult{
		ReplicaID: "r1", Success: true,
		EndpointVersion: 1, TransportEpoch: 1,
		ReplicaFlushedLSN: 50, PrimaryTailLSN: 10, PrimaryHeadLSN: 100,
	}
	a := NewVolumeReplicaAdapter(exec)
	a.startTimeout = 300 * time.Millisecond

	a.OnAssignment(AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: "a", CtrlAddr: "b",
	})
	time.Sleep(10 * time.Millisecond)

	sid := exec.latestSessionID("StartCatchUp")
	if sid == 0 {
		t.Fatal("StartCatchUp was not issued")
	}

	// Invalidate via identity bump — engine clears st.Session
	// synchronously and the adapter will arm no new timer because
	// no new Start* command follows (reachability reset; probe not
	// yet issued, but session is already cleared).
	exec.probeResults["r1"] = ProbeResult{
		ReplicaID: "r1", Success: true,
		EndpointVersion: 2, TransportEpoch: 2,
		ReplicaFlushedLSN: 50, PrimaryTailLSN: 10, PrimaryHeadLSN: 100,
	}
	a.OnAssignment(AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: "a", CtrlAddr: "b",
	})

	// Any late SessionStarted for the invalidated session must be
	// rejected at the engine (wrong session ID).
	exec.fireStart(SessionStartResult{ReplicaID: "r1", SessionID: sid})
	// Any late SessionClosedCompleted for the invalidated session
	// must likewise be rejected.
	exec.fireClose(SessionCloseResult{
		ReplicaID: "r1", SessionID: sid,
		Success: true, AchievedLSN: 100,
	})
	time.Sleep(50 * time.Millisecond)

	p := a.Projection()
	if p.Mode == engine.ModeHealthy {
		t.Fatalf("invalidated session produced healthy projection: %+v", p)
	}
	if p.SessionPhase == engine.PhaseRunning || p.SessionPhase == engine.PhaseCompleted {
		t.Fatalf("invalidated session phase=%s; must not fabricate running/completed", p.SessionPhase)
	}

	// The engine must trace the stale callback rejections.
	trace := a.Trace()
	sawStaleStart := false
	sawStaleClose := false
	for _, e := range trace {
		if e.Step == "stale_session_started" {
			sawStaleStart = true
		}
		if e.Step == "stale_session_completed" {
			sawStaleClose = true
		}
	}
	if !sawStaleStart {
		t.Fatalf("expected stale_session_started trace, got %+v", trace)
	}
	if !sawStaleClose {
		t.Fatalf("expected stale_session_completed trace, got %+v", trace)
	}
}
