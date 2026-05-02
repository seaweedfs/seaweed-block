package adapter

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

func TestProbeResult_ProgressFact_SuccessCarriesRSH(t *testing.T) {
	probe := ProbeResult{
		ReplicaID:         "r1",
		Success:           true,
		EndpointVersion:   7,
		TransportEpoch:    9,
		ReplicaFlushedLSN: 11,
		PrimaryTailLSN:    5,
		PrimaryHeadLSN:    20,
	}

	fact := probe.ProgressFact()
	if fact.ReplicaID != probe.ReplicaID {
		t.Fatalf("ReplicaID=%q want %q", fact.ReplicaID, probe.ReplicaID)
	}
	if fact.EndpointVersion != probe.EndpointVersion {
		t.Fatalf("EndpointVersion=%d want %d", fact.EndpointVersion, probe.EndpointVersion)
	}
	if fact.Source != engine.ProgressFromProbe {
		t.Fatalf("Source=%s want %s", fact.Source, engine.ProgressFromProbe)
	}
	if fact.Confidence != engine.ProgressLiveWire {
		t.Fatalf("Confidence=%s want %s", fact.Confidence, engine.ProgressLiveWire)
	}
	if !fact.ReplicaRKnown || !fact.PrimaryBoundsKnown {
		t.Fatalf("fact should carry known R/S/H: %+v", fact)
	}
	if fact.ReplicaR != probe.ReplicaFlushedLSN ||
		fact.PrimaryS != probe.PrimaryTailLSN ||
		fact.PrimaryH != probe.PrimaryHeadLSN {
		t.Fatalf("fact R/S/H=(%d,%d,%d), want (%d,%d,%d)",
			fact.ReplicaR, fact.PrimaryS, fact.PrimaryH,
			probe.ReplicaFlushedLSN, probe.PrimaryTailLSN, probe.PrimaryHeadLSN)
	}
}

func TestProbeResult_ProgressFact_FailureCarriesMissingFact(t *testing.T) {
	probe := ProbeResult{
		ReplicaID:       "r1",
		Success:         false,
		EndpointVersion: 7,
		FailReason:      "dial refused",
	}

	fact := probe.ProgressFact()
	if fact.Source != engine.ProgressFromProbe {
		t.Fatalf("Source=%s want %s", fact.Source, engine.ProgressFromProbe)
	}
	if fact.Confidence != engine.ProgressMissing {
		t.Fatalf("Confidence=%s want %s", fact.Confidence, engine.ProgressMissing)
	}
	if fact.ReplicaRKnown || fact.PrimaryBoundsKnown {
		t.Fatalf("failed probe must not carry trusted R/S/H: %+v", fact)
	}
}

func TestProbeResult_ProgressFact_AllZeroRemainsNoBoundaries(t *testing.T) {
	probe := ProbeResult{
		ReplicaID:       "r1",
		Success:         true,
		EndpointVersion: 1,
	}

	fact := probe.ProgressFact()
	if fact.ReplicaRKnown || fact.PrimaryBoundsKnown {
		t.Fatalf("all-zero probe remains no-boundaries for engine compatibility: %+v", fact)
	}
	if got := engine.ClassifyProgress(fact); got != engine.DecisionUnknown {
		t.Fatalf("ClassifyProgress(all-zero probe)=%s want %s", got, engine.DecisionUnknown)
	}
}
