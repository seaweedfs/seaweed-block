package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCandidateTailReportsCompleteFrozenBudget(t *testing.T) {
	for _, tc := range []struct {
		name            string
		fixed           time.Duration
		samples         int
		tail            uint64
		estimate, slack int64
		known           bool
	}{
		{"d05_fixed_does_not_fit", 5370 * time.Millisecond, 3, 1000, 5570000000, 0, true},
		{"small_complete_estimate", 200 * time.Millisecond, 3, 1000, 400000000, 4600000000, true},
		{"known_zero_tail", 200 * time.Millisecond, 1, 0, 300000000, 4700000000, true},
		{"zero_tail_without_fixed_sample", 0, 0, 0, 1200000000, 3800000000, true},
		{"ceiling_boundary_is_complete", 200 * time.Millisecond, 3, 600000, 60300000000, 0, true},
		{"saturated_tail_is_non_actionable", 200 * time.Millisecond, 3, 610000, 60000000000, 0, true},
		{"single_measured_rate", 200 * time.Millisecond, 1, 1000, 400000000, 4600000000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &chunkLayerImpl{}
			for i := 0; i < tc.samples; i++ {
				l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: tc.fixed, rate: 20000})
			}
			tail := l.estimateCandidateTail(tc.tail)
			b, err := json.Marshal(tail)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]json.RawMessage
			if err = json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]int64{"frozenEstimateNs": tc.estimate, "freezeLimitNs": 5000000000, "maxRefreezeQuiesceNs": tc.slack} {
				var value int64
				if raw, ok := got[key]; !ok {
					t.Errorf("missing %s: %s", key, b)
				} else if err = json.Unmarshal(raw, &value); err != nil || value != want {
					t.Errorf("%s=%s want %d", key, raw, want)
				}
			}
			var known bool
			if raw, ok := got["frozenEstimateKnown"]; !ok {
				t.Errorf("missing frozenEstimateKnown: %s", b)
			} else if err = json.Unmarshal(raw, &known); err != nil || known != tc.known {
				t.Errorf("known=%s want %v", raw, tc.known)
			}
		})
	}
}

func TestFrozenRefusalRetainsUnknownCompleteEstimate(t *testing.T) {
	l := &chunkLayerImpl{precopySamples: []precopySample{{at: time.Now(), fixed: 200 * time.Millisecond}}}
	r := l.precopyEstimate(1000, 0)
	now := time.Now()
	if r.admitFrozenAt(FrozenDetachOptions{StartedAt: now}, now) {
		t.Fatal("unknown rate admitted")
	}
	refusal, ok := frozenRefusal(r).(*FrozenRefusal)
	if !ok || refusal.Report.FrozenEstimateKnown || refusal.Report.MaxRefreezeQuiesce != 0 || refusal.Report.FrozenEstimate != 300*time.Millisecond {
		t.Fatalf("partial estimate became known after reason changed: %+v", r)
	}
}

func TestFrozenDrainObservationPreservesKnownBeforeReasonMutation(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 3)
	w.write(t, s, 1, 0x71)
	seedFrozenSamples(l, 200*time.Millisecond)
	before := f.stats()
	result, err := l.PrecopyDetachWithAdmission(frozenContext(t, time.Second, false), func(context.Context) (func(), error) { return nil, context.Canceled }, func() bool { t.Fatal("drain refusal stopped I/O"); return true })
	var refusal *FrozenRefusal
	if !errors.As(err, &refusal) || result.Precopy.Reason != "drain_refused" {
		t.Fatalf("drain path not witnessed: %+v %v", result.Precopy, err)
	}
	r := refusal.Report
	if !r.FrozenEstimateKnown || r.FrozenEstimate != r.Estimate || r.FreezeLimit != 5*time.Second || r.GuestFreezeElapsed < time.Second || r.RemainingFreeze <= 0 || r.RemainingFreeze+r.GuestFreezeElapsed != 5*time.Second {
		t.Fatalf("drain reason destroyed known observation: %+v", r)
	}
	after := f.stats()
	if after.puts != before.puts || after.cas != before.cas {
		t.Fatal("drain refusal did remote work")
	}
}
func TestFrozenSaturatedTailRemainsKnownButNonActionable(t *testing.T) {
	l := &chunkLayerImpl{}
	for i := 0; i < 3; i++ {
		l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: 200 * time.Millisecond, rate: 20000})
	}
	r := l.precopyEstimate(610000, 0) // safe rate 10000 B/s -> 61s tail, beyond the existing ceiling.
	now := time.Now()
	if r.admitFrozenAt(FrozenDetachOptions{StartedAt: now}, now) || r.Estimate != 60*time.Second || r.TailEstimate != 60*time.Second || !r.FrozenEstimateKnown || r.FrozenEstimate != 60*time.Second || r.FreezeLimit != 5*time.Second || r.MaxRefreezeQuiesce != 0 {
		t.Fatalf("saturated sentinel became actionable or changed admission: %+v", r)
	}
}
