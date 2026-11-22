package durable

// The automatic cut policy (B9 W3 ruling, snapshots-only scope), with
// explicit times.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

var t0 = time.Unix(1_700_000_000, 0)

func testPolicy() cutPolicy {
	return cutPolicy{interval: 30 * time.Second, minSpacing: time.Second, backoffMin: time.Second, backoffMax: 30 * time.Second}
}

func at(d time.Duration) time.Time { return t0.Add(d) }

// Only the operator's timer triggers an automatic cut; with it off,
// nothing is ever due.
func TestCutPolicy_OnlyTheTimerTriggers(t *testing.T) {
	pol := testPolicy()
	s := cutSchedule{lastSuccess: t0}
	if r, ok := pol.dueNR(s, at(29*time.Second)); ok {
		t.Fatalf("due (%s) before the interval", r)
	}
	if r, ok := pol.dueNR(s, at(30*time.Second)); !ok || r != "periodic" {
		t.Fatalf("at the interval: %q %v; want a periodic cut (a no-op cut still resets the timer)", r, ok)
	}
	pol.interval = 0
	if r, ok := pol.dueNR(s, at(1000*time.Hour)); ok {
		t.Fatalf("due (%s) with the timer off", r)
	}
}

func TestCutPolicy_AnySuccessfulCutResetsTheTimer(t *testing.T) {
	pol := testPolicy()
	s := cutSchedule{lastSuccess: t0}
	// An explicit Cut, Snapshot or Detach succeeded; the loop sees the
	// layer's new LastCutNs at 20 s and the timer restarts from there.
	s = s.observe(101, at(20*time.Second))
	if r, ok := pol.dueNR(s, at(45*time.Second)); ok {
		t.Fatalf("due (%s) 25 s after an explicit cut", r)
	}
	if _, ok := pol.dueNR(s, at(50*time.Second)); !ok {
		t.Fatal("not due 30 s after the explicit cut")
	}
	// The same report seen again does not move the timer.
	if got := s.observe(101, at(40*time.Second)); !got.lastSuccess.Equal(at(20 * time.Second)) {
		t.Fatalf("a repeated report moved the timer to %v", got.lastSuccess)
	}
	// An automatic success restarts it too.
	s = pol.after(s, at(50*time.Second), at(50*time.Second+200*time.Millisecond), nil, 102)
	if _, ok := pol.dueNR(s, at(79*time.Second)); ok {
		t.Fatal("due less than 30 s after an automatic success")
	}
}

func TestCutPolicy_MinimumSpacingBetweenAutomaticCuts(t *testing.T) {
	pol := testPolicy()
	pol.interval = 500 * time.Millisecond
	s := cutSchedule{lastSuccess: t0}
	s = pol.after(s, at(10*time.Second), at(10*time.Second+50*time.Millisecond), nil, 101)
	if r, ok := pol.dueNR(s, at(10*time.Second+900*time.Millisecond)); ok {
		t.Fatalf("automatic cut again after 0.9 s (%s)", r)
	}
	if _, ok := pol.dueNR(s, at(11*time.Second)); !ok {
		t.Fatal("timer cut not due 1 s after the last automatic cut")
	}
}

func TestCutPolicy_FailureBacksOffAndResetsNothing(t *testing.T) {
	pol := testPolicy()
	s := cutSchedule{lastSuccess: t0}
	fail := errors.New("remote unavailable")
	now := at(30 * time.Second)
	var waits []time.Duration
	for i := 0; i < 7; i++ {
		s = pol.after(s, now, now, fail, 0)
		waits = append(waits, s.backoff)
		if _, ok := pol.dueNR(s, now.Add(s.backoff-time.Millisecond)); ok {
			t.Fatalf("retry %d due before its backoff %s", i, s.backoff)
		}
		now = now.Add(s.backoff)
		if _, ok := pol.dueNR(s, now); !ok {
			t.Fatalf("retry %d not due after its backoff %s (the failure must not reset the timer)", i, s.backoff)
		}
	}
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if waits[i] != w*time.Second {
			t.Fatalf("backoffs %v, want %v seconds", waits, want)
		}
	}
	if !s.lastSuccess.Equal(t0) {
		t.Fatalf("a failure moved the last success to %v", s.lastSuccess)
	}
	s = pol.after(s, now, now, nil, 101)
	if s.backoff != 0 || !s.retryAt.IsZero() || !s.lastSuccess.Equal(now) {
		t.Fatalf("success did not clear the backoff: %+v", s)
	}
}

// A failed automatic cut backs off; a later explicit success makes that
// backoff obsolete, so the next timer cut is due one interval after the
// explicit cut, not at the old retry time.
func TestCutPolicy_ExplicitSuccessClearsFailedAutoBackoff(t *testing.T) {
	pol := testPolicy()
	pol.interval = time.Second
	s := cutSchedule{lastSuccess: t0}
	fail := errors.New("remote unavailable")
	now := at(time.Second)
	for s.backoff < pol.backoffMax {
		s = pol.after(s, now, now, fail, 100)
		now = now.Add(time.Millisecond)
	}
	retryAt := s.retryAt
	explicit := now.Add(2 * time.Second) // well before retryAt (30 s away)
	s = s.observe(200, explicit)
	next := explicit.Add(pol.interval)
	if !next.Before(retryAt) {
		t.Fatalf("test setup: next timer cut %v not before the old retry %v", next, retryAt)
	}
	if r, ok := pol.dueNR(s, next.Add(-time.Millisecond)); ok {
		t.Fatalf("due (%s) less than one interval after the explicit cut", r)
	}
	if r, ok := pol.dueNR(s, next); !ok || r != "periodic" {
		t.Fatalf("one interval after the explicit success: %q %v; want periodic (backoff %s, retry at %v)", r, ok, s.backoff, s.retryAt)
	}
	// The cut the failure already saw (it completed before the failure)
	// leaves the backoff in force.
	s2 := cutSchedule{lastSuccess: t0, seenSeq: 90}
	s2 = pol.after(s2, at(10*time.Second), at(10*time.Second), fail, 100)
	s2 = s2.observe(100, at(11*time.Second))
	if s2.retryAt.IsZero() {
		t.Fatal("a success the failure already saw cleared its backoff")
	}
}

// dueNR is due for a layer that is not reconciling.
func (pol cutPolicy) dueNR(s cutSchedule, now time.Time) (string, bool) {
	return pol.due(s, now, false)
}

func TestCutPolicy_NothingDueWhileReconciling(t *testing.T) {
	pol := testPolicy()
	s := cutSchedule{lastSuccess: t0}
	if r, ok := pol.due(s, at(time.Hour), true); ok {
		t.Fatalf("due (%s) while reconciling", r)
	}
	// The skipped tick is not a failure: no backoff once reconciled.
	if s.backoff != 0 || !s.retryAt.IsZero() {
		t.Fatalf("schedule changed by a skipped tick: %+v", s)
	}
	if r, ok := pol.due(s, at(time.Hour), false); !ok || r != "periodic" {
		t.Fatalf("after reconciliation: %q %v; want an immediate periodic cut", r, ok)
	}
}

func TestCutPolicy_StaggerPreservesExplicitTimerReset(t *testing.T) {
	pol := testPolicy()
	s := initialCutSchedule("v1", pol.interval, t0, true, 0)
	if !s.lastSuccess.Equal(t0) || !s.firstDue.After(t0) || s.firstDue.After(t0.Add(pol.interval)) {
		t.Fatal("phase fabricated success or exceeded first period", s)
	}
	if _, ok := pol.dueNR(s, s.firstDue.Add(-time.Nanosecond)); ok {
		t.Fatal("initial phase ignored")
	}
	if _, ok := pol.dueNR(s, s.firstDue); !ok {
		t.Fatal("first phased cut not due")
	}
	explicit := t0.Add(s.firstDue.Sub(t0) / 2)
	s = s.observe(5, explicit)
	if !s.lastSuccess.Equal(explicit) || !s.firstDue.IsZero() {
		t.Fatal("stagger hid explicit success", s)
	}
	if _, ok := pol.dueNR(s, explicit.Add(pol.interval-time.Nanosecond)); ok {
		t.Fatal("timer advanced before explicit success interval")
	}
	if _, ok := pol.dueNR(s, explicit.Add(pol.interval)); !ok {
		t.Fatal("explicit success did not restart timer")
	}
}

// The timer runs on this process's monotonic time and keys on the layer's
// CutSeq, never on a cut's wall stamp: a cut seen for the first time is a
// success at the moment it is observed, so the next timer cut is one
// interval after the observation; at start the layer's existing CutSeq is
// not new, so the startup phase stands.
func TestCutPolicy_TimerRunsOnObservationTime(t *testing.T) {
	pol := testPolicy()
	seen := at(20 * time.Second)
	s := cutSchedule{lastSuccess: t0}.observe(1, seen)
	if !s.lastSuccess.Equal(seen) {
		t.Fatalf("last success %v; want the observation time %v", s.lastSuccess, seen)
	}
	if _, ok := pol.dueNR(s, seen.Add(pol.interval-time.Nanosecond)); ok {
		t.Fatal("due before one interval after the observation")
	}
	if _, ok := pol.dueNR(s, seen.Add(pol.interval)); !ok {
		t.Fatal("not due one interval after the observation")
	}
	restart := initialCutSchedule("v1", pol.interval, t0, true, 7)
	if got := restart.observe(7, at(time.Second)); got.firstDue.IsZero() || !got.lastSuccess.Equal(t0) {
		t.Fatalf("the cut existing at start was taken in as new: %+v", got)
	}
}

// A cut refused because every host slot is held ran nothing: no backoff,
// no minimum spacing, the timer is not reset, and the retry comes after
// the volume's slot jitter. The wait is counted and its start kept until
// a cut succeeds. Any other error still backs off.
func TestCutPolicy_HostSlotBusyIsAWaitNotAFailure(t *testing.T) {
	pol := testPolicy()
	pol.slotJitter = 150 * time.Millisecond
	busy := fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrSlotsHeld)
	s := cutSchedule{lastSuccess: t0}
	now := at(30 * time.Second)
	for i := 0; i < 5; i++ {
		s = pol.after(s, now, now.Add(time.Millisecond), busy, 0)
		if s.backoff != 0 || !s.lastAuto.IsZero() || !s.lastSuccess.Equal(t0) {
			t.Fatalf("busy refusal %d treated as a cut or a failure: %+v", i, s)
		}
		if _, ok := pol.dueNR(s, now.Add(100*time.Millisecond)); ok {
			t.Fatalf("busy retry %d due before its jitter", i)
		}
		now = now.Add(time.Millisecond + pol.slotJitter)
		if _, ok := pol.dueNR(s, now); !ok {
			t.Fatalf("busy retry %d not due after its jitter", i)
		}
	}
	if s.slotWaits != 5 || !s.slotWaitSince.Equal(at(30*time.Second)) {
		t.Fatalf("waits %d since %v; want 5 since the first refusal", s.slotWaits, s.slotWaitSince)
	}
	s = pol.after(s, now, now, nil, 101)
	if !s.slotWaitSince.IsZero() || s.slotWaits != 5 {
		t.Fatalf("success must end the wait and keep the count: %+v", s)
	}
	refusedOther := fmt.Errorf("%w: not primary", storage.ErrChunkCaptureRefused)
	if s = pol.after(s, now, now, refusedOther, 101); s.backoff != time.Second {
		t.Fatalf("another refusal must back off: %+v", s)
	}
	tooLarge := fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrBusy)
	if s = pol.after(s, now, now, tooLarge, 101); s.backoff != 2*time.Second {
		t.Fatalf("a busy result that is not held slots must back off: %+v", s)
	}
}
