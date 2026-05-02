package engine

import (
	"reflect"
	"testing"
)

func TestReplicaProgressFact_ClassifiesRecoveryNeed(t *testing.T) {
	tests := []struct {
		name string
		fact ReplicaProgressFact
		want RecoveryDecision
	}{
		{
			name: "missing primary bounds is unknown",
			fact: ReplicaProgressFact{
				ReplicaR:      10,
				ReplicaRKnown: true,
			},
			want: DecisionUnknown,
		},
		{
			name: "new peer with no trusted replica R needs rebuild",
			fact: ReplicaProgressFact{
				PrimaryS:           1,
				PrimaryH:           10,
				PrimaryBoundsKnown: true,
			},
			want: DecisionRebuild,
		},
		{
			name: "replica caught up",
			fact: ReplicaProgressFact{
				ReplicaR:           10,
				ReplicaRKnown:      true,
				PrimaryS:           1,
				PrimaryH:           10,
				PrimaryBoundsKnown: true,
			},
			want: DecisionNone,
		},
		{
			name: "gap within retained WAL is catch-up",
			fact: ReplicaProgressFact{
				ReplicaR:           5,
				ReplicaRKnown:      true,
				PrimaryS:           5,
				PrimaryH:           10,
				PrimaryBoundsKnown: true,
			},
			want: DecisionCatchUp,
		},
		{
			name: "gap below primary retained tail needs rebuild",
			fact: ReplicaProgressFact{
				ReplicaR:           4,
				ReplicaRKnown:      true,
				PrimaryS:           5,
				PrimaryH:           10,
				PrimaryBoundsKnown: true,
			},
			want: DecisionRebuild,
		},
		{
			name: "known empty volume is none not unknown",
			fact: ReplicaProgressFact{
				ReplicaR:           0,
				ReplicaRKnown:      true,
				PrimaryS:           0,
				PrimaryH:           0,
				PrimaryBoundsKnown: true,
			},
			want: DecisionNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyProgress(tt.fact); got != tt.want {
				t.Fatalf("ClassifyProgress()=%s want %s", got, tt.want)
			}
		})
	}
}

func TestReplicaProgressFact_SourceDoesNotChangeRecoveryClassification(t *testing.T) {
	base := ReplicaProgressFact{
		ReplicaR:           7,
		ReplicaRKnown:      true,
		PrimaryS:           5,
		PrimaryH:           10,
		PrimaryBoundsKnown: true,
		Confidence:         ProgressLiveWire,
	}
	want := DecisionCatchUp
	for _, source := range []ProgressSource{
		ProgressFromProbe,
		ProgressFromDurableAck,
		ProgressFromSessionClose,
		ProgressFromStartup,
	} {
		fact := base
		fact.Source = source
		if got := ClassifyProgress(fact); got != want {
			t.Fatalf("source=%s ClassifyProgress()=%s want %s", source, got, want)
		}
	}
}

func TestReplicaProgressFact_OnlyDurableAckCanAdvanceRecyclePin(t *testing.T) {
	base := ReplicaProgressFact{
		ReplicaR:      7,
		ReplicaRKnown: true,
	}
	for _, source := range []ProgressSource{
		ProgressFromProbe,
		ProgressFromSessionClose,
		ProgressFromStartup,
	} {
		fact := base
		fact.Source = source
		if CanAdvanceRecyclePin(fact) {
			t.Fatalf("source=%s must not advance recycle pin", source)
		}
	}
	base.Source = ProgressFromDurableAck
	if !CanAdvanceRecyclePin(base) {
		t.Fatal("durable ack fact with known R must advance recycle pin")
	}
	base.ReplicaRKnown = false
	if CanAdvanceRecyclePin(base) {
		t.Fatal("durable ack without known R must not advance recycle pin")
	}
}

func TestReplicaProgressFact_HasNoTargetLSNField(t *testing.T) {
	if _, ok := reflect.TypeOf(ReplicaProgressFact{}).FieldByName("TargetLSN"); ok {
		t.Fatal("ReplicaProgressFact must not carry TargetLSN; use PrimaryH/frontier facts instead")
	}
}

func TestRecoveryFactsObserved_ProgressFact_PreservesEngineNoBoundariesCompatibility(t *testing.T) {
	event := RecoveryFactsObserved{ReplicaID: "r1"}
	fact := event.ProgressFact()
	if fact.ReplicaRKnown || fact.PrimaryBoundsKnown {
		t.Fatalf("all-zero RecoveryFactsObserved must remain no-boundaries for engine compatibility: %+v", fact)
	}
	if got := ClassifyProgress(fact); got != DecisionUnknown {
		t.Fatalf("ClassifyProgress(all-zero event)=%s want %s", got, DecisionUnknown)
	}
}

func TestRecoveryFactsObserved_ProgressFact_ClassifiesWithCanonicalFact(t *testing.T) {
	event := RecoveryFactsObserved{
		ReplicaID:       "r1",
		EndpointVersion: 2,
		R:               5,
		S:               5,
		H:               9,
	}
	fact := event.ProgressFact()
	if fact.Source != ProgressFromProbe || fact.Confidence != ProgressLiveWire {
		t.Fatalf("source/confidence=(%s,%s), want (%s,%s)",
			fact.Source, fact.Confidence, ProgressFromProbe, ProgressLiveWire)
	}
	if got := ClassifyProgress(fact); got != DecisionCatchUp {
		t.Fatalf("ClassifyProgress()=%s want %s", got, DecisionCatchUp)
	}
}
