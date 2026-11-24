package durable

import (
	"errors"
	"testing"
	"time"
)

var schedT0 = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// Slots are a per-volume grid on the wall clock: the phase lies within one
// period, in whole seconds, and differs between volumes; every slot start
// is the phase plus whole periods, whatever the process start.
func TestSnapshotSchedule_SlotsAreAPerVolumeGrid(t *testing.T) {
	day := 24 * time.Hour
	phases := map[time.Duration]bool{}
	for _, vol := range []string{"v1", "v2", "v3", "a-much-longer-volume-name"} {
		a := newSnapshotSchedule(vol, day, schedT0)
		b := newSnapshotSchedule(vol, day, schedT0.Add(7*time.Hour+13*time.Second))
		if a.phase < 0 || a.phase >= day || a.phase%time.Second != 0 {
			t.Fatalf("%s: phase %v; want whole seconds within one period", vol, a.phase)
		}
		phases[a.phase] = true
		for _, at := range []time.Time{schedT0, schedT0.Add(30 * time.Hour), schedT0.Add(-5 * day)} {
			s := a.slotStart(at)
			if s.After(at) || at.Sub(s) >= day || (s.UnixNano()-int64(a.phase))%int64(day) != 0 {
				t.Fatalf("%s: slot of %v is %v; want the grid start at or before it", vol, at, s)
			}
			if !b.slotStart(at).Equal(s) {
				t.Fatalf("%s: a schedule started later puts %v in slot %v, not %v", vol, at, b.slotStart(at), s)
			}
		}
	}
	if len(phases) < 2 {
		t.Fatalf("every volume got the same phase %v", phases)
	}
}

// A run is due when no published scheduled snapshot names the current
// slot: the first decision is at once; a newest snapshot of an earlier slot
// makes the current one due; a newest of this slot (or a later one) is not
// due, and the next decision is the next slot's start.
func TestSnapshotSchedule_DueFromTheNewestSnapshot(t *testing.T) {
	day := 24 * time.Hour
	s := newSnapshotSchedule("v1", day, schedT0)
	if !s.due().Equal(schedT0) {
		t.Fatalf("first decision at %v; want at once (%v)", s.due(), schedT0)
	}
	slot := s.slotStart(schedT0)
	if at, due := s.decide(schedT0, time.Time{}); !due || !at.Equal(slot) {
		t.Fatalf("no scheduled snapshot: due=%v at %v; want due for %v", due, at, slot)
	}
	if at, due := s.decide(schedT0, slot.Add(-day)); !due || !at.Equal(slot) {
		t.Fatalf("newest of the previous slot: due=%v at %v; want due for %v", due, at, slot)
	}
	// The newest's time is read from its name: to the second.
	if _, due := s.decide(schedT0, slot.Truncate(time.Second)); due {
		t.Fatal("newest of this slot: due; want not due")
	}
	if !s.due().Equal(slot.Add(day)) {
		t.Fatalf("not due: next decision %v; want the next slot %v", s.due(), slot.Add(day))
	}
	if _, due := s.decide(schedT0, slot.Add(3*day)); due {
		t.Fatal("newest of a later slot (a clock behind): due; want not due")
	}
}

// After a success the slot is the newest and the next decision is the next
// slot's start (at once if that has already begun).
func TestSnapshotSchedule_SuccessMovesToTheNextSlot(t *testing.T) {
	day := 24 * time.Hour
	s := newSnapshotSchedule("v1", day, schedT0)
	slot, _ := s.decide(schedT0, time.Time{})
	s.record(schedT0.Add(time.Minute), slot, nil)
	if st := s.status(schedT0.Add(time.Minute)); st.NewestNs != slot.UnixNano() || st.LastOkNs == 0 || st.Attempts != 0 {
		t.Fatalf("after a success: %+v; want the slot as newest", st)
	}
	if !s.due().Equal(slot.Add(day)) {
		t.Fatalf("next decision %v; want the next slot %v", s.due(), slot.Add(day))
	}
	late := slot.Add(day + time.Hour) // a run that finished after the next slot began
	s.record(late, slot, nil)
	if !s.due().Equal(late) {
		t.Fatalf("a run finished inside the next slot: next decision %v; want at once (%v)", s.due(), late)
	}
}

// A restoring volume is skipped, counted by reason, and not treated as a
// failure; it is decided again within the backoff cap, not a whole period
// later.
func TestSnapshotSchedule_RestoringIsSkippedAndCounted(t *testing.T) {
	s := newSnapshotSchedule("v1", 24*time.Hour, schedT0)
	at := s.due()
	slot, _ := s.decide(at, time.Time{})
	s.record(at, slot, &SnapshotSkip{Reason: "restoring"})
	s.record(s.due(), slot, &SnapshotSkip{Reason: "restoring"})
	st := s.status(s.due())
	if st.Skipped["restoring"] != 2 || st.Attempts != 0 || st.LastFailure != nil {
		t.Fatalf("status %+v; want 2 restoring skips, no failure", st)
	}
	if gap := s.due().Sub(at); gap > 2*snapshotRetryMax {
		t.Fatalf("after two skips the next decision is %v later; want within the backoff cap", gap)
	}
}

// A failure (of the attempt, or of reading the newest snapshot) is recorded
// with its typed code and decided again after a backoff that doubles from
// one minute up to one hour; a success clears it.
func TestSnapshotSchedule_FailureIsTypedAndBacksOff(t *testing.T) {
	s := newSnapshotSchedule("v1", 24*time.Hour, schedT0)
	now := s.due()
	slot, _ := s.decide(now, time.Time{})
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 32 * time.Minute, time.Hour, time.Hour}
	for i, w := range want {
		s.record(now, slot, &SnapshotFailure{Code: "remote_unavailable", Err: errors.New("filer down")})
		if got := s.due().Sub(now); got != w {
			t.Fatalf("after failure %d the retry is %v later; want %v", i+1, got, w)
		}
		now = s.due()
	}
	st := s.status(now)
	if st.Attempts != len(want) || st.LastFailure == nil || st.LastFailure.Code != "remote_unavailable" {
		t.Fatalf("status %+v; want %d attempts and code remote_unavailable", st, len(want))
	}
	s.record(now, time.Time{}, errors.New("untyped"))
	if st := s.status(now); st.LastFailure.Code != "unknown" {
		t.Fatalf("untyped failure code %q; want unknown", st.LastFailure.Code)
	}
	s.record(now, slot, nil)
	if st := s.status(now); st.Attempts != 0 || st.LastFailure != nil || st.LastOkNs != now.UnixNano() {
		t.Fatalf("after a success: %+v; want the failure cleared and lastOk set", st)
	}
}

// Overdue is measured from the newest scheduled snapshot: once it is older
// than one period plus the retry cap; before the newest is read, from the
// start. The failure stays visible.
func TestSnapshotSchedule_OverdueFromTheNewest(t *testing.T) {
	day, grace := 24*time.Hour, snapshotRetryMax
	s := newSnapshotSchedule("v1", day, schedT0)
	if s.status(schedT0.Add(day + grace)).Overdue {
		t.Fatal("overdue at exactly one period plus the retry cap from the start")
	}
	if !s.status(schedT0.Add(day + grace + time.Second)).Overdue {
		t.Fatal("not overdue past one period plus the retry cap with nothing read")
	}
	old := schedT0.Add(-3 * day)
	s.decide(schedT0, old)
	if st := s.status(schedT0); !st.Overdue || st.NewestNs != old.UnixNano() {
		t.Fatalf("newest three periods old: %+v; want overdue at once", st)
	}
	recent := s.slotStart(schedT0)
	s.decide(schedT0, recent)
	if s.status(recent.Add(day + grace)).Overdue {
		t.Fatal("overdue within one period plus the retry cap of the newest")
	}
	s.record(recent.Add(day), time.Time{}, &SnapshotFailure{Code: "outcome_unknown"})
	if st := s.status(recent.Add(day + grace + time.Second)); !st.Overdue || st.LastFailure.Code != "outcome_unknown" {
		t.Fatalf("status %+v; want overdue with the failure visible", st)
	}
}

// A clock stepped back by more than a period: the newest lies in the
// future, no run is due until the clock passes it (no storm), and the
// status says the clock is suspect.
func TestSnapshotSchedule_ClockStepBackNoStorm(t *testing.T) {
	day := 24 * time.Hour
	s := newSnapshotSchedule("v1", day, schedT0)
	newest := s.slotStart(schedT0).Add(2 * day) // taken before the clock went back 2 days
	for _, now := range []time.Time{schedT0, schedT0.Add(day), schedT0.Add(day + 23*time.Hour)} {
		if _, due := s.decide(now, newest); due {
			t.Fatalf("clock %v behind the newest %v: due; want no run", now, newest)
		}
	}
	if st := s.status(schedT0); !st.ClockSuspect {
		t.Fatalf("newest two days ahead: %+v; want clock_suspect", st)
	}
	if _, due := s.decide(newest.Add(day), newest); !due {
		t.Fatal("clock one period past the newest: not due; want a run")
	}
	if st := s.status(newest.Add(day)); st.ClockSuspect {
		t.Fatal("clock past the newest: still clock_suspect")
	}
}
