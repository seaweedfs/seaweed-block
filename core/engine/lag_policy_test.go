package engine

import "testing"

func TestLagPolicy_ProgressingLagKeepsFeeding(t *testing.T) {
	facts := []ReplicaProgressFact{
		progressFact(9, 5, 20),
		progressFact(10, 5, 25),
		progressFact(11, 5, 30),
	}

	got := EvaluateLagPolicy(LagPolicy{StalledSamples: 3}, facts)
	if got != LagDecisionKeepFeeding {
		t.Fatalf("EvaluateLagPolicy()=%s want %s", got, LagDecisionKeepFeeding)
	}
}

func TestLagPolicy_StalledWithinWALStartsCatchUp(t *testing.T) {
	facts := []ReplicaProgressFact{
		progressFact(10, 5, 20),
		progressFact(10, 5, 25),
		progressFact(10, 5, 30),
	}

	got := EvaluateLagPolicy(LagPolicy{StalledSamples: 3}, facts)
	if got != LagDecisionStartCatchUp {
		t.Fatalf("EvaluateLagPolicy()=%s want %s", got, LagDecisionStartCatchUp)
	}
}

func TestLagPolicy_BelowRetainedTailStartsRebuild(t *testing.T) {
	facts := []ReplicaProgressFact{
		progressFact(4, 5, 30),
	}

	got := EvaluateLagPolicy(LagPolicy{StalledSamples: 3}, facts)
	if got != LagDecisionStartRebuild {
		t.Fatalf("EvaluateLagPolicy()=%s want %s", got, LagDecisionStartRebuild)
	}
}

func TestLagPolicy_NearLiveTailKeepsFeeding(t *testing.T) {
	facts := []ReplicaProgressFact{
		progressFact(28, 5, 30),
	}

	got := EvaluateLagPolicy(LagPolicy{MaxLiveTailLag: 3, StalledSamples: 3}, facts)
	if got != LagDecisionKeepFeeding {
		t.Fatalf("EvaluateLagPolicy()=%s want %s", got, LagDecisionKeepFeeding)
	}
}

func TestLagPolicy_CaughtUpIsInSync(t *testing.T) {
	facts := []ReplicaProgressFact{
		progressFact(30, 5, 30),
	}

	got := EvaluateLagPolicy(LagPolicy{StalledSamples: 3}, facts)
	if got != LagDecisionInSync {
		t.Fatalf("EvaluateLagPolicy()=%s want %s", got, LagDecisionInSync)
	}
}

func TestLagPolicy_UnknownWithoutKnownFacts(t *testing.T) {
	facts := []ReplicaProgressFact{
		{
			ReplicaR:      10,
			ReplicaRKnown: true,
			Source:        ProgressFromProbe,
			Confidence:    ProgressLiveWire,
		},
	}

	got := EvaluateLagPolicy(LagPolicy{StalledSamples: 3}, facts)
	if got != LagDecisionUnknown {
		t.Fatalf("EvaluateLagPolicy()=%s want %s", got, LagDecisionUnknown)
	}
}

func TestLagPolicy_SourceDoesNotChangeLagClassification(t *testing.T) {
	facts := []ReplicaProgressFact{
		progressFactWithSource(10, 5, 20, ProgressFromProbe),
		progressFactWithSource(10, 5, 25, ProgressFromDurableAck),
		progressFactWithSource(10, 5, 30, ProgressFromSessionClose),
	}

	got := EvaluateLagPolicy(LagPolicy{StalledSamples: 3}, facts)
	if got != LagDecisionStartCatchUp {
		t.Fatalf("EvaluateLagPolicy()=%s want %s", got, LagDecisionStartCatchUp)
	}
}

func progressFact(replicaR, primaryS, primaryH uint64) ReplicaProgressFact {
	return progressFactWithSource(replicaR, primaryS, primaryH, ProgressFromDurableAck)
}

func progressFactWithSource(replicaR, primaryS, primaryH uint64, source ProgressSource) ReplicaProgressFact {
	return ReplicaProgressFact{
		ReplicaR:           replicaR,
		ReplicaRKnown:      true,
		PrimaryS:           primaryS,
		PrimaryH:           primaryH,
		PrimaryBoundsKnown: true,
		Source:             source,
		Confidence:         ProgressLiveWire,
	}
}
