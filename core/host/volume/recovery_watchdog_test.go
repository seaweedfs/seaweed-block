package volume

import (
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// #128 rule 6: a primary-assigned replica that is not promotion-ready
// within its recover timeout reports an explicit unhealthy fact.

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func watchdogHost(t *testing.T, timeout time.Duration) (*Host, *fakeClock) {
	t.Helper()
	h := newTestVolumeHost(t)
	clk := &fakeClock{t: time.Unix(1000, 0)}
	h.wd = newRecoveryWatchdog(timeout)
	h.wd.now = clk.now
	return h, clk
}

func selfPrimaryFact(epoch uint64) *control.AssignmentFact {
	return &control.AssignmentFact{
		VolumeId: "v1", ReplicaId: "r1", Epoch: epoch, EndpointVersion: 1,
		DataAddr: "127.0.0.1:9201", CtrlAddr: "127.0.0.1:9101",
	}
}

func reportSlot(t *testing.T, h *Host) *control.HeartbeatSlot {
	t.Helper()
	rep := h.buildReport()
	if len(rep.Slots) != 1 {
		t.Fatalf("report slots: %d", len(rep.Slots))
	}
	return rep.Slots[0]
}

// The heartbeat of a primary-assigned, not-ready replica turns explicit
// unhealthy (Eligible=false, recovery_timeout) once the timeout elapses.
func TestRecoveryWatchdog_PrimaryNotReadyPastTimeoutReportsUnhealthy(t *testing.T) {
	h, clk := watchdogHost(t, 30*time.Second)
	h.applyFact(selfPrimaryFact(1))

	clk.advance(29 * time.Second)
	if s := reportSlot(t, h); !s.Eligible || s.UnhealthyReason != "" || s.ReadyForPrimary {
		t.Fatalf("inside the recover timeout: %+v; want eligible, no reason, not ready", s)
	}
	clk.advance(2 * time.Second)
	if s := reportSlot(t, h); s.Eligible || s.UnhealthyReason != UnhealthyRecoveryTimeout {
		t.Fatalf("past the recover timeout: eligible=%t reason=%q; want ineligible, %s", s.Eligible, s.UnhealthyReason, UnhealthyRecoveryTimeout)
	}
	if got := h.UnhealthyReason(); got != UnhealthyRecoveryTimeout {
		t.Fatalf("UnhealthyReason()=%q", got)
	}
}

// A supporting replica in a long catch-up is never flagged.
func TestRecoveryWatchdog_SupportingReplicaNotFlagged(t *testing.T) {
	h, clk := watchdogHost(t, 30*time.Second)
	h.applyFact(selfPrimaryFact(1))
	h.applyFact(&control.AssignmentFact{ // another replica is now primary
		VolumeId: "v1", ReplicaId: "r2", Epoch: 2, EndpointVersion: 1,
		DataAddr: "127.0.0.1:9301", CtrlAddr: "127.0.0.1:9302",
	})
	h.wd.supportingAdmitted(2)
	clk.advance(time.Hour)
	if s := reportSlot(t, h); !s.Eligible || s.UnhealthyReason != "" {
		t.Fatalf("supporting replica after an hour: %+v; want eligible, no reason", s)
	}
}

// recovery_timeout latches until a fresh ready fact clears it; once
// ready in this assignment the watchdog is disarmed; a new primary epoch
// re-arms it with a fresh clock.
func TestRecoveryWatchdog_ReadyClearsAndDisarmsNewEpochRearms(t *testing.T) {
	w := newRecoveryWatchdog(10 * time.Second)
	clk := &fakeClock{t: time.Unix(1000, 0)}
	w.now = clk.now

	w.primaryAssigned(1)
	clk.advance(11 * time.Second)
	if r, fired := w.reason(false, false); r != UnhealthyRecoveryTimeout || !fired {
		t.Fatalf("past timeout: %q fired=%t", r, fired)
	}
	if r, fired := w.reason(false, false); r != UnhealthyRecoveryTimeout || fired {
		t.Fatalf("latched: %q fired=%t; want the reason, fired only once", r, fired)
	}
	if r, _ := w.reason(true, false); r != "" {
		t.Fatalf("ready: %q; want cleared", r)
	}
	if r, _ := w.reason(false, false); r != "" {
		t.Fatalf("unready again in the same assignment: %q; want disarmed", r)
	}

	w.primaryAssigned(2) // new primary epoch: fresh clock
	clk.advance(9 * time.Second)
	if r, _ := w.reason(false, false); r != "" {
		t.Fatalf("new epoch inside timeout: %q", r)
	}
	clk.advance(2 * time.Second)
	if r, _ := w.reason(false, false); r != UnhealthyRecoveryTimeout {
		t.Fatalf("new epoch past timeout: %q", r)
	}
}

// The G1 role-admission latch of a primary-assigned replica is reported
// at once; it never applies to a replica without the primary assignment.
func TestRecoveryWatchdog_RoleAdmissionFailure(t *testing.T) {
	w := newRecoveryWatchdog(time.Hour)
	if r, _ := w.reason(false, true); r != "" {
		t.Fatalf("not primary-assigned: %q", r)
	}
	w.primaryAssigned(1)
	if r, _ := w.reason(false, true); r != UnhealthyRoleAdmission {
		t.Fatalf("primary-assigned with the G1 latch: %q; want %s", r, UnhealthyRoleAdmission)
	}
}

// /status carries the unhealthy reason, and a supporting replica's
// catch-up state.
func TestRecoveryWatchdog_StatusShowsReasonAndCatchUp(t *testing.T) {
	h, clk := watchdogHost(t, time.Second)
	h.applyFact(selfPrimaryFact(1))
	clk.advance(2 * time.Second)
	s := NewStatusServer(h.view)
	if got := s.statusProjection().UnhealthyReason; got != UnhealthyRecoveryTimeout {
		t.Fatalf("/status UnhealthyReason=%q; want %s", got, UnhealthyRecoveryTimeout)
	}

	h2 := newTestVolumeHost(t)
	h2.wd.supportingAdmitted(3)
	time.Sleep(20 * time.Millisecond)
	st := NewStatusServer(h2.view).statusProjection()
	if st.UnhealthyReason != "" || st.CatchUp == nil || st.CatchUp.ElapsedMs < 20 {
		t.Fatalf("supporting replica /status: reason=%q catchUp=%+v; want no reason and catch-up elapsed >= 20ms", st.UnhealthyReason, st.CatchUp)
	}
}

// Once ready in this assignment, a later not-ready on the same
// incarnation is a running primary degrading, not a boot-recovery stall:
// no recovery_timeout, however long it lasts (claude05, 21:24Z).
func TestRecoveryWatchdog_ReadyThenLaterNotReadyReportsNoTimeout(t *testing.T) {
	h, clk := watchdogHost(t, 10*time.Second)
	h.applyFact(selfPrimaryFact(1))
	clk.advance(time.Second)
	if r := h.unhealthyReason(true); r != "" {
		t.Fatalf("ready: %q", r)
	}
	clk.advance(time.Hour)
	if r := h.unhealthyReason(false); r != "" {
		t.Fatalf("not ready an hour after being ready, same incarnation: %q; want no recovery_timeout", r)
	}
	h.applyFact(selfPrimaryFact(1)) // the same assignment replayed: still disarmed
	clk.advance(time.Hour)
	if r := h.unhealthyReason(false); r != "" {
		t.Fatalf("same assignment replayed: %q; want still disarmed", r)
	}
}

// A new process incarnation re-arms the watchdog: its state is per
// process, so a restarted primary that stays not-ready past the timeout
// reports recovery_timeout even for the same primary epoch.
func TestRecoveryWatchdog_NewIncarnationRearms(t *testing.T) {
	old, clk := watchdogHost(t, 10*time.Second)
	old.applyFact(selfPrimaryFact(1))
	if r := old.unhealthyReason(true); r != "" {
		t.Fatalf("old process ready: %q", r)
	}

	restarted, clk2 := watchdogHost(t, 10*time.Second) // new process, same replica
	if restarted.incarnation == old.incarnation {
		t.Fatalf("restarted process kept incarnation %q", old.incarnation)
	}
	restarted.applyFact(selfPrimaryFact(1)) // same primary epoch
	clk2.advance(11 * time.Second)
	if s := reportSlot(t, restarted); s.Eligible || s.UnhealthyReason != UnhealthyRecoveryTimeout {
		t.Fatalf("restarted process past timeout: eligible=%t reason=%q; want ineligible, %s", s.Eligible, s.UnhealthyReason, UnhealthyRecoveryTimeout)
	}
	clk.advance(time.Hour)
	if r := old.unhealthyReason(false); r != "" {
		t.Fatalf("old process stays disarmed: %q", r)
	}
}
