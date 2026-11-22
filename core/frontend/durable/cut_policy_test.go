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
// no minimum spacing, the timer is not reset, and the retry is due at the
// next poll (admission order is the host ticket order). The wait is counted
// and its start kept until a cut succeeds. Any other error still backs off.
func TestCutPolicy_HostSlotBusyIsAWaitNotAFailure(t *testing.T) {
	pol := testPolicy()
	busy := fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrNotHead)
	s := cutSchedule{lastSuccess: t0}
	now := at(30 * time.Second)
	for i := 0; i < 5; i++ {
		s = pol.after(s, now, now.Add(time.Millisecond), busy, 0)
		if s.backoff != 0 || !s.lastAuto.IsZero() || !s.lastSuccess.Equal(t0) {
			t.Fatalf("busy refusal %d treated as a cut or a failure: %+v", i, s)
		}
		now = now.Add(chunkSchedulePoll)
		if _, ok := pol.dueNR(s, now); !ok {
			t.Fatalf("busy retry %d not due at the next poll", i)
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

// A successful cut observed during a slot wait (an explicit cut) ends the
// wait; the count is kept, and the next timer cut is due one interval later.
func TestCutPolicy_SuccessDuringSlotWaitEndsTheWait(t *testing.T) {
	pol := testPolicy()
	pol.interval = 100 * time.Millisecond
	busy := fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrSlotsHeld)
	s := cutSchedule{lastSuccess: t0, seenSeq: 100}
	s = pol.after(s, at(time.Second), at(time.Second), busy, 100)
	s = s.observe(101, at(time.Second+10*time.Millisecond))
	if !s.retryAt.IsZero() || !s.slotWaitSince.IsZero() || s.slotWaits != 1 {
		t.Fatalf("explicit success left the slot wait: %+v", s)
	}
	if _, ok := pol.dueNR(s, at(time.Second+110*time.Millisecond)); !ok {
		t.Fatal("next timer cut not due one interval after the explicit cut")
	}
}

// Status ends a recorded wait once the layer's CutSeq moves past the one
// it was recorded under, also when the timer is no longer running.
func TestCutWaitSlot_StatusEndsTheWaitAfterALaterCut(t *testing.T) {
	w := &cutWaitSlot{}
	w.record(cutSchedule{seenSeq: 5, slotWaits: 3, slotWaitSince: at(time.Second)}, nil)
	if st := w.status(5); st.HostSlotWaitSinceNs == 0 || st.HostSlotWaits != 3 {
		t.Fatalf("wait not reported at its own CutSeq: %+v", st)
	}
	if st := w.status(6); st.HostSlotWaitSinceNs != 0 || st.HostSlotWaits != 3 {
		t.Fatalf("wait reported after a later cut: %+v", st)
	}
	w.dropped("storage: chunk capture refused: budget admission: hostlimit: no background slot")
	if st := w.status(6); st.TicketDropReason == "" {
		t.Fatalf("drop reason not reported: %+v", st)
	}
}

// V timer loops under the cut policy share one host slot (service T per
// cut) and wait in ticket order (earliest due first), every volume due at
// the same instant, and one urgent cut takes the slot mid-round. No volume
// waits longer than (V-1) x (T + poll) plus the urgent work; with V x T <= c
// every volume keeps the interval, and with V x T > c (overload) every
// volume still cuts in every round: none is starved.
func TestCutPolicy_TicketOrderWaitBound(t *testing.T) {
	for _, tc := range []struct {
		name string
		T    time.Duration
	}{{"V x T <= c", 1400 * time.Millisecond}, {"overload V x T > c", 8 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			const (
				V      = 10
				urgent = 3 * time.Second
				runFor = 20 * time.Minute
			)
			interval := time.Minute
			poll := chunkSchedulePoll
			busy := fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrNotHead)
			pol := cutPolicy{interval: interval, minSpacing: chunkCutMinSpacing, backoffMin: chunkCutBackoffMin, backoffMax: chunkCutBackoffMax}
			ss := make([]cutSchedule, V)
			cuts := make([]int, V)
			for i := range ss {
				ss[i] = cutSchedule{lastSuccess: t0}
			}
			dueOf := func(i int) time.Time { return ss[i].lastSuccess.Add(interval) }
			holder, start, seq := -1, time.Time{}, uint64(0)
			urgentAt, urgentUntil := t0.Add(interval+30*time.Second), time.Time{}
			var worst time.Duration
			for now := t0; now.Before(t0.Add(runFor)); now = now.Add(poll) {
				if holder >= 0 && holder < V && !now.Before(start.Add(tc.T)) {
					seq++
					ss[holder] = pol.after(ss[holder], start, start.Add(tc.T), nil, seq)
					cuts[holder]++
					holder = -1
				}
				if holder < 0 && !urgentAt.IsZero() && !now.Before(urgentAt) {
					holder, urgentUntil, urgentAt = V, now.Add(urgent), time.Time{}
				}
				if holder == V && !now.Before(urgentUntil) {
					holder = -1
				}
				head := -1
				for i := range ss {
					if _, ok := pol.dueNR(ss[i], now); ok && i != holder && (head < 0 || dueOf(i).Before(dueOf(head))) {
						head = i
					}
				}
				for i := range ss {
					if i == holder {
						continue
					}
					if _, ok := pol.dueNR(ss[i], now); !ok {
						continue
					}
					if i == head && holder < 0 {
						if !ss[i].slotWaitSince.IsZero() {
							worst = max(worst, now.Sub(ss[i].slotWaitSince))
						}
						holder, start = i, now
						continue
					}
					ss[i] = pol.after(ss[i], now, now, busy, seq)
				}
			}
			bound := (V-1)*(tc.T+poll) + urgent + poll
			if worst > bound {
				t.Fatalf("a volume waited %s for the slot; bound %s", worst, bound)
			}
			round := max(interval, time.Duration(V)*(tc.T+poll))
			for i, n := range cuts {
				if n < int(runFor/(round+tc.T))-2 {
					t.Fatalf("volume %d cut %d times in %s (round about %s)", i, n, runFor, round)
				}
			}
			t.Logf("worst slot wait %s (bound %s); cuts per volume %v", worst, bound, cuts)
		})
	}
}
