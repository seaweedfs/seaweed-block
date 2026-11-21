package storage

import (
	"encoding/json"
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
		{"zero_tail_without_fixed_sample", 0, 0, 0, 0, 0, false},
		{"partial_unknown_rate", 200 * time.Millisecond, 1, 1000, 300000000, 0, false},
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
	l := &chunkLayerImpl{precopySamples: []precopySample{{at: time.Now(), fixed: 200 * time.Millisecond, rate: 20000}}}
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
