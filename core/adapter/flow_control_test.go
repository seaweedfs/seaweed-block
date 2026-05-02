package adapter

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

func TestAdapter_FlowControlObservation_PrimaryFlushLagRecordsShapeWithoutCommands(t *testing.T) {
	exec := newMockExecutor()
	a := NewVolumeReplicaAdapter(exec)
	a.SetFlowControlPolicy(engine.FlowControlPolicy{
		MaxPrimaryFlushLag:   10,
		MaxReplicaDurableLag: 10,
		MaxRetentionPressure: 20,
	})

	before := len(a.CommandLog())
	got := a.OnFlowControlObservation(engine.FlowControlObservation{
		PrimaryDurableLSN:          10,
		PrimaryTailLSN:             5,
		PrimaryHeadLSN:             30,
		PrimaryBoundsKnown:         true,
		SlowestReplicaDurableLSN:   1,
		SlowestReplicaDurableKnown: true,
	})

	if got.Action != engine.FlowControlShapePrimaryWrites {
		t.Fatalf("Action=%s want %s", got.Action, engine.FlowControlShapePrimaryWrites)
	}
	if got.Reason != engine.FlowControlReasonPrimaryFlushLag {
		t.Fatalf("Reason=%s want %s", got.Reason, engine.FlowControlReasonPrimaryFlushLag)
	}
	if after := len(a.CommandLog()); after != before {
		t.Fatalf("flow-control observation emitted commands: before=%d after=%d log=%v",
			before, after, a.CommandLog())
	}
}

func TestAdapter_FlowControlObservation_ExplicitDurabilityFailureRecordsOnly(t *testing.T) {
	exec := newMockExecutor()
	a := NewVolumeReplicaAdapter(exec)
	a.SetFlowControlPolicy(engine.FlowControlPolicy{MaxPrimaryFlushLag: 10})

	got := a.OnFlowControlObservation(engine.FlowControlObservation{
		PrimaryDurableLSN:  10,
		PrimaryHeadLSN:     30,
		PrimaryBoundsKnown: true,
		ExplicitDurability: true,
	})

	if got.Action != engine.FlowControlFailDurabilityWrite {
		t.Fatalf("Action=%s want %s", got.Action, engine.FlowControlFailDurabilityWrite)
	}
	if len(a.CommandLog()) != 0 {
		t.Fatalf("flow-control verdict must not execute commands; log=%v", a.CommandLog())
	}
}

func TestAdapter_FlowControlObservation_ReplicaRetentionRiskRecordsDegradeWithoutPublishing(t *testing.T) {
	exec := newMockExecutor()
	a := NewVolumeReplicaAdapter(exec)
	a.SetFlowControlPolicy(engine.FlowControlPolicy{
		MaxReplicaDurableLag: 10,
		MaxRetentionPressure: 20,
	})

	got := a.OnFlowControlObservation(engine.FlowControlObservation{
		PrimaryDurableLSN:          95,
		PrimaryTailLSN:             70,
		PrimaryHeadLSN:             100,
		PrimaryBoundsKnown:         true,
		SlowestReplicaDurableLSN:   80,
		SlowestReplicaDurableKnown: true,
	})

	if got.Action != engine.FlowControlDegradeSlowReplica {
		t.Fatalf("Action=%s want %s", got.Action, engine.FlowControlDegradeSlowReplica)
	}
	if got.Reason != engine.FlowControlReasonReplicaRetentionRisk {
		t.Fatalf("Reason=%s want %s", got.Reason, engine.FlowControlReasonReplicaRetentionRisk)
	}
	if hasAdapterCommand(a, "PublishDegraded") {
		t.Fatalf("flow-control verdict must not publish degraded directly; log=%v", a.CommandLog())
	}
}

func TestAdapter_FlowControlVerdictReturnsLastRecordedVerdict(t *testing.T) {
	exec := newMockExecutor()
	a := NewVolumeReplicaAdapter(exec)
	if _, ok := a.FlowControlVerdict(); ok {
		t.Fatal("FlowControlVerdict observed=true before any observation")
	}

	a.SetFlowControlPolicy(engine.FlowControlPolicy{MaxSyncQuorumMisses: 2})
	want := a.OnFlowControlObservation(engine.FlowControlObservation{
		SyncQuorumMisses: 3,
	})

	got, ok := a.FlowControlVerdict()
	if !ok {
		t.Fatal("FlowControlVerdict observed=false after observation")
	}
	if got != want {
		t.Fatalf("FlowControlVerdict=%+v want %+v", got, want)
	}
}
