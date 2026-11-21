package storage

import (
	"testing"
	"time"
)

func TestPrecopySmokeTraceAmendment4(t *testing.T) {
	estimates := []time.Duration{1475267, 1465874, 1733371, 1733371, 1733371}
	tails := []time.Duration{140090, 130697, 398194, 398194, 398194}
	var previous *PrecopyAttempt
	for i, e := range estimates {
		r := PrecopyReport{Fixed: 1235177 * time.Microsecond, Estimate: e * time.Microsecond, TailEstimate: tails[i] * time.Microsecond, Reason: "tail_too_large"}
		if r.admitPrecopy(previous, i == 4) {
			if i > 2 || r.Reason != "no_progress" {
				t.Fatalf("late/wrong admission round%d: %+v", i+1, r)
			}
			return
		}
		previous = &PrecopyAttempt{Estimate: r.Estimate}
	}
	t.Fatal("smoke trace refused despite no meaningful progress within freeze bound")
}
func TestPrecopyFixedWindowDecays(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 1024, 0)
	now := time.Now()
	l.precopySamples = nil
	l.precopySamples = append(l.precopySamples, precopySample{at: now, fixed: 1235 * time.Millisecond})
	for i := 0; i < 5; i++ {
		l.precopySamples = append(l.precopySamples, precopySample{at: now, fixed: 100 * time.Millisecond, rate: 1e9})
	}
	if r := l.precopyEstimate(0, 0); r.Fixed != 100*time.Millisecond {
		t.Fatalf("old peak retained: %+v", r)
	}
	l.precopySamples = l.precopySamples[1:]
	l.precopySamples[0].at = now.Add(-16 * time.Minute)
	if r := l.precopyEstimate(0, 0); r.Fixed != 100*time.Millisecond || r.Reason == "samples_unknown" {
		t.Fatalf("available fresh samples were refused: %+v", r)
	}
	for i := range l.precopySamples {
		l.precopySamples[i].at = now.Add(-16 * time.Minute)
	}
	if r := l.precopyEstimate(0, 0); r.Reason != "samples_unknown" {
		t.Fatalf("expired samples admitted: %+v", r)
	}
	l.precopySamples = []precopySample{{at: now, fixed: 100 * time.Millisecond}}
	if r := l.precopyEstimate(0, 0); r.Fixed != 100*time.Millisecond || r.Reason != "fit" {
		t.Fatalf("single fresh fixed observation refused: %+v", r)
	}
}
func TestPrecopyFinalAttemptFreezeCeiling(t *testing.T) {
	for _, tc := range []struct {
		estimate time.Duration
		want     bool
	}{{4500 * time.Millisecond, true}, {5500 * time.Millisecond, false}} {
		r := PrecopyReport{Estimate: tc.estimate, TailEstimate: 3 * time.Second, Reason: "tail_too_large"}
		previous := &PrecopyAttempt{Estimate: 6 * time.Second}
		if got := r.admitPrecopy(previous, true); got != tc.want {
			t.Fatalf("final admission=%t want=%t: %+v", got, tc.want, r)
		}
		if tc.want && r.Reason != "final_attempt" {
			t.Fatalf("wrong final reason: %+v", r)
		}
	}
}

func TestPrecopyAmendment4AdmissionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name               string
		estimate, previous time.Duration
		final              bool
		reason             string
	}{
		{"fit", time.Second, 0, false, "fit"},
		{"ten-percent", 1800 * time.Millisecond, 2 * time.Second, false, "no_progress"},
		{"meaningful", 1800*time.Millisecond - 1, 2 * time.Second, false, "tail_too_large"},
		{"final-no-history", 4 * time.Second, 0, true, "final_attempt"},
		{"ceiling", 5 * time.Second, 0, true, "final_attempt"},
		{"over-ceiling", 5*time.Second + 1, 6 * time.Second, true, "freeze_budget_exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := PrecopyReport{Estimate: tc.estimate, Reason: "tail_too_large"}
			var prev *PrecopyAttempt
			if tc.previous > 0 {
				prev = &PrecopyAttempt{Estimate: tc.previous}
			}
			got := r.admitPrecopy(prev, tc.final)
			want := tc.reason == "fit" || tc.reason == "no_progress" || tc.reason == "final_attempt"
			if got != want || r.Reason != tc.reason {
				t.Fatalf("admit=%t want=%t reason=%s: %+v", got, want, tc.reason, r)
			}
		})
	}
	r := PrecopyReport{Reason: "samples_unknown", Estimate: time.Second}
	if r.admitPrecopy(&PrecopyAttempt{Estimate: time.Second}, true) {
		t.Fatal("unknown samples admitted")
	}
}
