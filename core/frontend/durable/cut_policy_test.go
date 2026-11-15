package durable

// The automatic cut policy (B9 W3 ruling), with explicit times.

import (
	"errors"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

func testPolicy() cutPolicy {
	return cutPolicy{interval: 30 * time.Second, threshold: 64, minSpacing: time.Second, backoffMin: time.Second, backoffMax: 30 * time.Second}
}

func at(d time.Duration) time.Time { return t0.Add(d) }

func TestCutPolicy_TimerAndThresholdWhicheverFirst(t *testing.T) {
	pol := testPolicy()
	s := cutSchedule{lastSuccess: t0}
	if r, ok := pol.due(s, at(29*time.Second), 63); ok {
		t.Fatalf("due (%s) before the interval and below the threshold", r)
	}
	if r, ok := pol.due(s, at(30*time.Second), 0); !ok || r != "periodic" {
		t.Fatalf("at the interval with nothing dirty: %q %v; want a periodic cut (a no-op cut still resets the timer)", r, ok)
	}
	if r, ok := pol.due(s, at(5*time.Second), 64); !ok || r != "threshold" {
		t.Fatalf("at the threshold: %q %v; want a threshold cut", r, ok)
	}
	pol.interval, pol.threshold = 0, 0
	if r, ok := pol.due(s, at(time.Hour), 1<<20); ok {
		t.Fatalf("due (%s) with timer and threshold disabled", r)
	}
}

func TestCutPolicy_AnySuccessfulCutResetsTheTimer(t *testing.T) {
	pol := testPolicy()
	s := cutSchedule{lastSuccess: t0}
	// An explicit Cut, Snapshot or Detach succeeded at 20 s: the layer
	// reports it and the timer restarts from there.
	s = s.observe(at(20 * time.Second).UnixNano())
	if r, ok := pol.due(s, at(45*time.Second), 0); ok {
		t.Fatalf("due (%s) 25 s after an explicit cut", r)
	}
	if _, ok := pol.due(s, at(50*time.Second), 0); !ok {
		t.Fatal("not due 30 s after the explicit cut")
	}
	// An older report never moves the timer back.
	if got := s.observe(at(5 * time.Second).UnixNano()); !got.lastSuccess.Equal(at(20 * time.Second)) {
		t.Fatalf("older report moved the timer to %v", got.lastSuccess)
	}
	// An automatic success restarts it too.
	s = pol.after(s, at(50*time.Second), at(50*time.Second+200*time.Millisecond), nil)
	if _, ok := pol.due(s, at(79*time.Second), 0); ok {
		t.Fatal("due less than 30 s after an automatic success")
	}
}

func TestCutPolicy_MinimumSpacingBetweenAutomaticCuts(t *testing.T) {
	pol := testPolicy()
	s := cutSchedule{lastSuccess: t0}
	s = pol.after(s, at(10*time.Second), at(10*time.Second+50*time.Millisecond), nil)
	if r, ok := pol.due(s, at(10*time.Second+900*time.Millisecond), 1000); ok {
		t.Fatalf("hot writer at the threshold cut again after 0.9 s (%s)", r)
	}
	if _, ok := pol.due(s, at(11*time.Second), 1000); !ok {
		t.Fatal("threshold cut not due 1 s after the last automatic cut")
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
		if _, ok := pol.due(s, now.Add(s.backoff-time.Millisecond), 1000); ok {
			t.Fatalf("retry %d due before its backoff %s", i, s.backoff)
		}
		now = now.Add(s.backoff)
		if _, ok := pol.due(s, now, 0); !ok {
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
