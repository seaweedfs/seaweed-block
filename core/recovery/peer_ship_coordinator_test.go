package recovery

// Completion oracle: recover(a,b) band — NOT recover(a) closure.
// See sw-block/design/recover-semantics-adjustment-plan.md §8.1.

import "testing"

const r1 ReplicaID = "r1"
const r2 ReplicaID = "r2"

// CHK-PHASE-NEVER-STEADY-BEFORE-DRAIN: cannot transition until BOTH
// shipCursor ≥ target AND baseDone hold.
func TestCoordinator_PhaseTransitionRequiresDrainAndBaseDone(t *testing.T) {
	c := NewPeerShipCoordinator()
	if err := c.StartSession(r1, 7, /*from*/ 100, /*target*/ 200); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if got := c.Phase(r1); got != PhaseDrainingHistorical {
		t.Fatalf("phase after start: got %s want DrainingHistorical", got)
	}

	// Neither drained nor base done.
	if c.TryAdvanceToSteadyLive(r1) {
		t.Fatal("transition should fail: neither drained nor baseDone")
	}

	// Drained but base not done.
	if err := c.RecordShipped(r1, 200); err != nil {
		t.Fatalf("RecordShipped: %v", err)
	}
	if !c.BacklogDrained(r1) {
		t.Fatal("BacklogDrained should be true after RecordShipped(target)")
	}
	if c.TryAdvanceToSteadyLive(r1) {
		t.Fatal("transition should fail: drained but !baseDone")
	}

	// Base done but suppose we somehow regress drained — that can't happen
	// (cursor is monotonic), so test the symmetric: baseDone first, drained
	// not yet.
	c2 := NewPeerShipCoordinator()
	_ = c2.StartSession(r1, 8, 100, 200)
	_ = c2.MarkBaseDone(r1)
	if c2.TryAdvanceToSteadyLive(r1) {
		t.Fatal("transition should fail: baseDone but !drained")
	}

	// Now satisfy both on the original coordinator.
	if err := c.MarkBaseDone(r1); err != nil {
		t.Fatalf("MarkBaseDone: %v", err)
	}
	if !c.TryAdvanceToSteadyLive(r1) {
		t.Fatal("transition should succeed: drained ∧ baseDone")
	}
	if got := c.Phase(r1); got != PhaseSteadyLiveAllowed {
		t.Fatalf("phase after transition: got %s want SteadyLiveAllowed", got)
	}

	// Idempotent: calling again is harmless and returns false.
	if c.TryAdvanceToSteadyLive(r1) {
		t.Fatal("second TryAdvance should return false (already SteadyLiveAllowed)")
	}
}

// INV-SINGLE-FLIGHT-PER-REPLICA.
func TestCoordinator_SingleFlightPerReplica(t *testing.T) {
	c := NewPeerShipCoordinator()
	if err := c.StartSession(r1, 7, 100, 200); err != nil {
		t.Fatalf("first StartSession: %v", err)
	}
	if err := c.StartSession(r1, 8, 200, 300); err == nil {
		t.Fatal("second StartSession on same peer: want error")
	}
	// After EndSession, a fresh start should succeed.
	c.EndSession(r1)
	if err := c.StartSession(r1, 9, 200, 300); err != nil {
		t.Fatalf("StartSession after EndSession: %v", err)
	}
}

// CHK-BARRIER-BEFORE-CLOSE: CanEmitSessionComplete enforces the
// §IV.2.1 A-class conjunct (recover-semantics-adjustment-plan §1):
//
//	phase != Idle ∧ baseDone ∧ achieved ≥ target
//	∧ (walLegOkWitnessed → walLegOkAtBarrier)
func TestCoordinator_CanEmitSessionComplete(t *testing.T) {
	c := NewPeerShipCoordinator()
	_ = c.StartSession(r1, 7, 100, 200)

	// !baseDone: refuse (§IV.2.1 A-class).
	if c.CanEmitSessionComplete(r1, 250) {
		t.Fatal("!baseDone: should refuse (A-class conjunct)")
	}

	_ = c.MarkBaseDone(r1)

	if c.CanEmitSessionComplete(r1, 199) {
		t.Fatal("achievedLSN < target: should refuse")
	}
	if !c.CanEmitSessionComplete(r1, 200) {
		t.Fatal("achievedLSN == target ∧ baseDone: should permit (no probe witness = legacy collapse)")
	}
	if !c.CanEmitSessionComplete(r1, 250) {
		t.Fatal("achievedLSN > target: should permit")
	}
	// Idle peer: never permitted.
	if c.CanEmitSessionComplete(r2, 999) {
		t.Fatal("idle peer: CanEmitSessionComplete should refuse")
	}
}

// RecordShipped is monotonic; lower LSNs do not regress shipCursor.
func TestCoordinator_RecordShipped_Monotonic(t *testing.T) {
	c := NewPeerShipCoordinator()
	_ = c.StartSession(r1, 7, 100, 200)
	_ = c.RecordShipped(r1, 150)
	_ = c.RecordShipped(r1, 120) // regression attempt
	st, _ := c.Status(r1)
	if st.ShipCursor != 150 {
		t.Fatalf("ShipCursor=%d want 150 (monotonic)", st.ShipCursor)
	}
}

// CHK-NO-FAKE-LIVE-DURING-BACKLOG: while session is active (any
// non-Idle phase), RouteLocalWrite returns SessionLane.
// SteadyLiveAllowed is a publication-permission flag, not a routing
// gate — see RouteLocalWrite docstring + architect ruling.
func TestCoordinator_RouteLocalWrite(t *testing.T) {
	c := NewPeerShipCoordinator()

	// Idle peer routes to steady live.
	if got := c.RouteLocalWrite(r1, 999); got != RouteSteadyLive {
		t.Fatalf("idle peer: route=%v want SteadyLive", got)
	}

	_ = c.StartSession(r1, 7, 100, 200)

	// Draining: writes route to session lane (any LSN).
	for _, lsn := range []uint64{150, 200, 201, 500} {
		if got := c.RouteLocalWrite(r1, lsn); got != RouteSessionLane {
			t.Fatalf("draining lsn=%d: route=%v want SessionLane", lsn, got)
		}
	}

	// After SteadyLiveAllowed transition: still SessionLane (because
	// barrier hasn't run; session is open; new writes must reach
	// replica via the recover connection so barrier's AchievedLSN
	// reflects them).
	_ = c.RecordShipped(r1, 200)
	_ = c.MarkBaseDone(r1)
	if !c.TryAdvanceToSteadyLive(r1) {
		t.Fatal("transition failed")
	}
	if got := c.Phase(r1); got != PhaseSteadyLiveAllowed {
		t.Fatalf("phase: got %s want SteadyLiveAllowed", got)
	}
	for _, lsn := range []uint64{201, 500, 1000} {
		if got := c.RouteLocalWrite(r1, lsn); got != RouteSessionLane {
			t.Fatalf("SteadyLiveAllowed lsn=%d: route=%v want SessionLane (still in session)", lsn, got)
		}
	}

	// Only after EndSession (phase=Idle) does routing switch.
	c.EndSession(r1)
	for _, lsn := range []uint64{201, 500, 1000} {
		if got := c.RouteLocalWrite(r1, lsn); got != RouteSteadyLive {
			t.Fatalf("post-EndSession lsn=%d: route=%v want SteadyLive", lsn, got)
		}
	}
}

// INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN.
func TestCoordinator_MinPinAcrossActiveSessions(t *testing.T) {
	c := NewPeerShipCoordinator()

	if _, any := c.MinPinAcrossActiveSessions(); any {
		t.Fatal("no active sessions: anyActive should be false")
	}

	_ = c.StartSession(r1, 1, 100, 200)
	floor, any := c.MinPinAcrossActiveSessions()
	if !any || floor != 100 {
		t.Fatalf("one session r1: floor=%d any=%v want 100/true", floor, any)
	}

	_ = c.StartSession(r2, 2, 50, 300)
	floor, any = c.MinPinAcrossActiveSessions()
	if !any || floor != 50 {
		t.Fatalf("two sessions: floor=%d any=%v want 50/true (min)", floor, any)
	}

	// Advance r2's pin floor; min should now be r1's 100.
	_ = c.SetPinFloor(r2, 150, 0)
	floor, any = c.MinPinAcrossActiveSessions()
	if !any || floor != 100 {
		t.Fatalf("after advance r2 to 150: floor=%d want 100 (r1 is now lower)", floor)
	}

	// End r1; only r2 left at 150.
	c.EndSession(r1)
	floor, any = c.MinPinAcrossActiveSessions()
	if !any || floor != 150 {
		t.Fatalf("after end r1: floor=%d any=%v want 150/true", floor, any)
	}

	// End r2; back to no active.
	c.EndSession(r2)
	if _, any := c.MinPinAcrossActiveSessions(); any {
		t.Fatal("after end all: anyActive should be false")
	}
}

// INV-PIN-STABLE-WITHIN-SESSION: SetPinFloor monotonic.
// Uses primarySBoundary=0 (recycle gate disabled in this test);
// retention checks have their own test below.
func TestCoordinator_SetPinFloor_Monotonic(t *testing.T) {
	c := NewPeerShipCoordinator()
	_ = c.StartSession(r1, 7, 100, 200)
	if got := c.PinFloor(r1); got != 100 {
		t.Fatalf("initial pinFloor=%d want 100 (== fromLSN)", got)
	}
	if err := c.SetPinFloor(r1, 150, 0); err != nil {
		t.Fatalf("SetPinFloor(150): %v", err)
	}
	if got := c.PinFloor(r1); got != 150 {
		t.Fatalf("after advance: pinFloor=%d want 150", got)
	}
	// Regression attempt: ignored.
	if err := c.SetPinFloor(r1, 120, 0); err != nil {
		t.Fatalf("SetPinFloor(120): %v", err)
	}
	if got := c.PinFloor(r1); got != 150 {
		t.Fatalf("after regression attempt: pinFloor=%d want 150 (monotonic)", got)
	}
}

// INV-PIN-COMPATIBLE-WITH-RETENTION: floor < primarySBoundary →
// FailurePinUnderRetention, session must invalidate.
func TestCoordinator_SetPinFloor_RejectsBelowRetention(t *testing.T) {
	c := NewPeerShipCoordinator()
	_ = c.StartSession(r1, 7, 100, 500)

	// Healthy path: floor (200) ≥ primary's S (150).
	if err := c.SetPinFloor(r1, 200, 150); err != nil {
		t.Fatalf("healthy SetPinFloor: %v", err)
	}

	// Violation: primary's S has advanced to 250, replica's
	// proposed floor of 240 is below — session contract broken.
	err := c.SetPinFloor(r1, 240, 250)
	if err == nil {
		t.Fatal("SetPinFloor(floor=240, S=250): want PinUnderRetention error")
	}
	f := AsFailure(err)
	if f == nil {
		t.Fatalf("expected typed *Failure, got %T: %v", err, err)
	}
	if f.Kind != FailurePinUnderRetention {
		t.Errorf("Kind=%s want PinUnderRetention", f.Kind)
	}
	if f.Phase != PhasePinUpdate {
		t.Errorf("Phase=%s want pin-update", f.Phase)
	}
	if f.Retryable() {
		t.Error("PinUnderRetention should be Retryable=false (new lineage required)")
	}

	// pinFloor must NOT have advanced on the rejected call.
	if got := c.PinFloor(r1); got != 200 {
		t.Errorf("pinFloor after rejected SetPinFloor: got %d want 200 (unchanged)", got)
	}
}

// primarySBoundary=0 disables the retention check entirely (legacy
// path; useful for tests that don't model retention).
func TestCoordinator_SetPinFloor_ZeroBoundaryDisablesCheck(t *testing.T) {
	c := NewPeerShipCoordinator()
	_ = c.StartSession(r1, 7, 100, 500)
	// Even floor=1 (way below typical retention) succeeds when S=0.
	if err := c.SetPinFloor(r1, 1, 0); err != nil {
		t.Fatalf("SetPinFloor with S=0: %v", err)
	}
	// Monotonic: floor=1 doesn't actually advance from the initial 100.
	if got := c.PinFloor(r1); got != 100 {
		t.Errorf("pinFloor=%d want 100 (initial; floor=1 didn't advance)", got)
	}
}

// EndSession is idempotent and clears state.
func TestCoordinator_EndSession_Idempotent(t *testing.T) {
	c := NewPeerShipCoordinator()
	_ = c.StartSession(r1, 7, 100, 200)
	c.EndSession(r1)
	c.EndSession(r1) // second call should be a no-op (no panic)
	if got := c.Phase(r1); got != PhaseIdle {
		t.Fatalf("after EndSession: phase=%s want Idle", got)
	}
	if got := c.PinFloor(r1); got != 0 {
		t.Fatalf("after EndSession: pinFloor=%d want 0", got)
	}
}

// StartSession argument validation.
func TestCoordinator_StartSession_ArgValidation(t *testing.T) {
	c := NewPeerShipCoordinator()
	if err := c.StartSession(r1, 0, 100, 200); err == nil {
		t.Error("sessionID=0 should error")
	}
	if err := c.StartSession(r1, 1, 200, 100); err == nil {
		t.Error("targetLSN < fromLSN should error")
	}
	// Equal targets are legal (zero-LSN-progress sessions).
	if err := c.StartSession(r1, 1, 100, 100); err != nil {
		t.Errorf("targetLSN == fromLSN: %v", err)
	}
}

// Methods on idle peer error appropriately.
func TestCoordinator_IdlePeer_OperationsError(t *testing.T) {
	c := NewPeerShipCoordinator()
	if err := c.RecordShipped(r1, 100); err == nil {
		t.Error("RecordShipped on idle: want error")
	}
	if err := c.MarkBaseDone(r1); err == nil {
		t.Error("MarkBaseDone on idle: want error")
	}
	if err := c.SetPinFloor(r1, 100, 0); err == nil {
		t.Error("SetPinFloor on idle: want error")
	}
	if c.BacklogDrained(r1) {
		t.Error("BacklogDrained on idle: want false")
	}
}

// TestCoordinator_NextBarrierCut_MonotonicPerSession — Option B
// (plan §8.2.6): per-session counter starts at 1, increments per
// call within a session, resets to 0 on next StartSession.
func TestCoordinator_NextBarrierCut_MonotonicPerSession(t *testing.T) {
	c := NewPeerShipCoordinator()

	// Pre-session: error.
	if cut, err := c.NextBarrierCut(r1); err == nil || cut != 0 {
		t.Errorf("NextBarrierCut on idle: cut=%d err=%v want 0+error", cut, err)
	}

	if err := c.StartSession(r1, 7, 100, 200); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// First call: 1.
	cut, err := c.NextBarrierCut(r1)
	if err != nil {
		t.Fatalf("NextBarrierCut #1: %v", err)
	}
	if cut != 1 {
		t.Errorf("first cut=%d want 1", cut)
	}
	// Second call: 2.
	cut, err = c.NextBarrierCut(r1)
	if err != nil {
		t.Fatalf("NextBarrierCut #2: %v", err)
	}
	if cut != 2 {
		t.Errorf("second cut=%d want 2", cut)
	}

	// EndSession + new StartSession resets to 0; first call yields 1.
	c.EndSession(r1)
	if err := c.StartSession(r1, 8, 100, 200); err != nil {
		t.Fatalf("StartSession (second session): %v", err)
	}
	cut, err = c.NextBarrierCut(r1)
	if err != nil {
		t.Fatalf("NextBarrierCut after restart: %v", err)
	}
	if cut != 1 {
		t.Errorf("post-restart first cut=%d want 1 (per-session reset)", cut)
	}
}
