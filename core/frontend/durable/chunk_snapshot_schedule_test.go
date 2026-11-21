package durable

import (
	"errors"
	"testing"
	"time"
)

var schedT0 = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// The first slot is the start plus the volume's offset, which lies within
// one period and differs between volumes; after a success the next slot is
// one period later, and a slot is never in the past.
func TestSnapshotSchedule_SlotsAreOffsetPerVolume(t *testing.T) {
	day := 24 * time.Hour
	offsets := map[time.Duration]bool{}
	for _, vol := range []string{"v1", "v2", "v3", "a-much-longer-volume-name"} {
		s := newSnapshotSchedule(vol, day, schedT0)
		due := s.due()
		off := due.Sub(schedT0)
		if off < 0 || off >= day {
			t.Fatalf("%s: first slot %v after the start, want within one period", vol, off)
		}
		offsets[off] = true
		s.record(due, nil)
		if s.due().Sub(due) != day {
			t.Fatalf("%s: after a success the next slot is %v later; want one period", vol, s.due().Sub(due))
		}
		late := due.Add(3*day + time.Hour) // an attempt that ran very late
		s.record(late, nil)
		if next := s.due(); !next.After(late) || next.Sub(late) > day || next.Sub(due)%day != 0 {
			t.Fatalf("%s: after a late success the next slot is %v; want the first slot after it", vol, next)
		}
	}
	if len(offsets) < 2 {
		t.Fatalf("every volume got the same offset %v", offsets)
	}
}

// A restoring volume is skipped, counted by reason, and not treated as a
// failure; it is retried within the backoff cap, not a whole period later.
func TestSnapshotSchedule_RestoringIsSkippedAndCounted(t *testing.T) {
	s := newSnapshotSchedule("v1", 24*time.Hour, schedT0)
	at := s.due()
	s.record(at, &SnapshotSkip{Reason: "restoring"})
	s.record(s.due(), &SnapshotSkip{Reason: "restoring"})
	st := s.status(s.due())
	if st.Skipped["restoring"] != 2 || st.Attempts != 0 || st.LastFailure != nil {
		t.Fatalf("status %+v; want 2 restoring skips, no failure", st)
	}
	if gap := s.due().Sub(at); gap > 2*snapshotRetryMax {
		t.Fatalf("after two skips the next attempt is %v later; want within the backoff cap", gap)
	}
}

// A failure is recorded with its typed code and retried after a backoff
// that doubles from one minute up to one hour; a success clears it and
// schedules the next slot.
func TestSnapshotSchedule_FailureIsTypedAndBacksOff(t *testing.T) {
	s := newSnapshotSchedule("v1", 24*time.Hour, schedT0)
	now := s.due()
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 32 * time.Minute, time.Hour, time.Hour}
	for i, w := range want {
		s.record(now, &SnapshotFailure{Code: "remote_unavailable", Err: errors.New("filer down")})
		if got := s.due().Sub(now); got != w {
			t.Fatalf("after failure %d the retry is %v later; want %v", i+1, got, w)
		}
		now = s.due()
	}
	st := s.status(now)
	if st.Attempts != len(want) || st.LastFailure == nil || st.LastFailure.Code != "remote_unavailable" {
		t.Fatalf("status %+v; want %d attempts and code remote_unavailable", st, len(want))
	}
	s.record(now, errors.New("untyped"))
	if st := s.status(now); st.LastFailure.Code != "unknown" {
		t.Fatalf("untyped failure code %q; want unknown", st.LastFailure.Code)
	}
	s.record(now, nil)
	st = s.status(now)
	if st.Attempts != 0 || st.LastFailure != nil || st.LastOkNs != now.UnixNano() {
		t.Fatalf("after a success: %+v; want the failure cleared and lastOk set", st)
	}
	if next := s.due(); !next.After(now) || next.Sub(now) > 24*time.Hour || next.Sub(s.origin)%(24*time.Hour) != 0 {
		t.Fatalf("after a success the next attempt is %v; want the next slot", next)
	}
}

// The schedule is overdue once no scheduled snapshot succeeded within one
// period plus the retry cap: counted from the start before any success,
// then from the last success; the failure stays visible.
func TestSnapshotSchedule_OverdueAfterPeriodPlusRetryCap(t *testing.T) {
	day, grace := 24*time.Hour, snapshotRetryMax
	s := newSnapshotSchedule("v1", day, schedT0)
	if s.status(schedT0.Add(day + grace)).Overdue {
		t.Fatal("overdue at exactly one period plus the retry cap")
	}
	if !s.status(schedT0.Add(day + grace + time.Second)).Overdue {
		t.Fatal("not overdue past one period plus the retry cap with no success")
	}
	ok := schedT0.Add(day)
	s.record(ok, nil)
	if s.status(ok.Add(day + grace)).Overdue {
		t.Fatal("overdue within one period plus the retry cap of a success")
	}
	s.record(ok.Add(day), &SnapshotFailure{Code: "outcome_unknown"})
	if st := s.status(ok.Add(day + grace + time.Second)); !st.Overdue || st.LastFailure.Code != "outcome_unknown" {
		t.Fatalf("status %+v; want overdue with the failure visible", st)
	}
}
