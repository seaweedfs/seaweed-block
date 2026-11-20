package durable

// The automatic cut policy (B9 W3 ruling, snapshots-only scope), with
// explicit times.

import (
	"errors"
	"testing"
	"time"
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
	// An explicit Cut, Snapshot or Detach succeeded at 20 s: the layer
	// reports it and the timer restarts from there.
	s = s.observe(at(20 * time.Second).UnixNano())
	if r, ok := pol.dueNR(s, at(45*time.Second)); ok {
		t.Fatalf("due (%s) 25 s after an explicit cut", r)
	}
	if _, ok := pol.dueNR(s, at(50*time.Second)); !ok {
		t.Fatal("not due 30 s after the explicit cut")
	}
	// An older report never moves the timer back.
	if got := s.observe(at(5 * time.Second).UnixNano()); !got.lastSuccess.Equal(at(20 * time.Second)) {
		t.Fatalf("older report moved the timer to %v", got.lastSuccess)
	}
	// An automatic success restarts it too.
	s = pol.after(s, at(50*time.Second), at(50*time.Second+200*time.Millisecond), nil)
	if _, ok := pol.dueNR(s, at(79*time.Second)); ok {
		t.Fatal("due less than 30 s after an automatic success")
	}
}

func TestCutPolicy_MinimumSpacingBetweenAutomaticCuts(t *testing.T) {
	pol := testPolicy()
	pol.interval = 500 * time.Millisecond
	s := cutSchedule{lastSuccess: t0}
	s = pol.after(s, at(10*time.Second), at(10*time.Second+50*time.Millisecond), nil)
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
		s = pol.after(s, now, now, fail)
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
	s = pol.after(s, now, now, nil)
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
		s = pol.after(s, now, now, fail)
		now = now.Add(time.Millisecond)
	}
	retryAt := s.retryAt
	explicit := now.Add(2 * time.Second) // well before retryAt (30 s away)
	s = s.observe(explicit.UnixNano())
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
	// A success older than the failure leaves the backoff in force.
	s2 := cutSchedule{lastSuccess: t0}
	s2 = pol.after(s2, at(10*time.Second), at(10*time.Second), fail)
	s2 = s2.observe(at(5 * time.Second).UnixNano())
	if s2.retryAt.IsZero() {
		t.Fatal("a success older than the failure cleared its backoff")
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
	s := initialCutSchedule("v1", pol.interval, t0, true)
	if !s.lastSuccess.Equal(t0) || s.initialDelay <= 0 {
		t.Fatal("phase fabricated a successful cut", s)
	}
	phase := s.initialDelay
	if _, ok := pol.dueNR(s, t0.Add(pol.interval)); ok {
		t.Fatal("initial phase ignored")
	}
	if _, ok := pol.dueNR(s, t0.Add(pol.interval+phase)); !ok {
		t.Fatal("first phased cut not due")
	}
	explicit := t0.Add(phase / 2)
	s = s.observe(explicit.UnixNano())
	if !s.lastSuccess.Equal(explicit) || s.initialDelay != 0 {
		t.Fatal("stagger hid explicit success", s)
	}
	if _, ok := pol.dueNR(s, explicit.Add(pol.interval-time.Nanosecond)); ok {
		t.Fatal("timer advanced before explicit success interval")
	}
	if _, ok := pol.dueNR(s, explicit.Add(pol.interval)); !ok {
		t.Fatal("explicit success did not restart timer")
	}
}
