package engine

import "testing"

func TestBuildFlowControlFacts_PrimaryBoundariesDriveFlushAndRetentionPressure(t *testing.T) {
	got := BuildFlowControlFacts(FlowControlObservation{
		PrimaryDurableLSN:  40,
		PrimaryTailLSN:     30,
		PrimaryHeadLSN:     100,
		PrimaryBoundsKnown: true,
	})

	if got.PrimaryFlushLag != 60 {
		t.Fatalf("PrimaryFlushLag=%d want 60", got.PrimaryFlushLag)
	}
	if got.RetentionPressure != 70 {
		t.Fatalf("RetentionPressure=%d want 70", got.RetentionPressure)
	}
}

func TestBuildFlowControlFacts_SlowestDurableAckDrivesReplicaLag(t *testing.T) {
	got := BuildFlowControlFacts(FlowControlObservation{
		PrimaryDurableLSN:          90,
		PrimaryTailLSN:             20,
		PrimaryHeadLSN:             100,
		PrimaryBoundsKnown:         true,
		SlowestReplicaDurableLSN:   70,
		SlowestReplicaDurableKnown: true,
	})

	if got.ReplicaDurableLag != 30 {
		t.Fatalf("ReplicaDurableLag=%d want 30", got.ReplicaDurableLag)
	}
	if got.PrimaryFlushLag != 10 {
		t.Fatalf("PrimaryFlushLag=%d want 10", got.PrimaryFlushLag)
	}
}

func TestBuildFlowControlFacts_SessionDurableAckDrivesRecoveryBacklog(t *testing.T) {
	got := BuildFlowControlFacts(FlowControlObservation{
		PrimaryDurableLSN:   100,
		PrimaryTailLSN:      50,
		PrimaryHeadLSN:      120,
		PrimaryBoundsKnown:  true,
		SessionDurableLSN:   80,
		SessionDurableKnown: true,
	})

	if got.RecoveryBacklog != 40 {
		t.Fatalf("RecoveryBacklog=%d want 40", got.RecoveryBacklog)
	}
}

func TestBuildFlowControlFacts_UnknownPrimaryBoundsDoNotInventLSNPressure(t *testing.T) {
	got := BuildFlowControlFacts(FlowControlObservation{
		PrimaryHeadLSN:             100,
		SlowestReplicaDurableLSN:   10,
		SlowestReplicaDurableKnown: true,
		SessionDurableLSN:          20,
		SessionDurableKnown:        true,
		SyncQuorumMisses:           3,
		ExplicitDurability:         true,
	})

	if got.PrimaryFlushLag != 0 ||
		got.RetentionPressure != 0 ||
		got.ReplicaDurableLag != 0 ||
		got.RecoveryBacklog != 0 {
		t.Fatalf("LSN pressure with unknown primary bounds: %+v", got)
	}
	if got.SyncQuorumMisses != 3 {
		t.Fatalf("SyncQuorumMisses=%d want 3", got.SyncQuorumMisses)
	}
	if !got.ExplicitDurability {
		t.Fatal("ExplicitDurability=false want true")
	}
}

func TestBuildFlowControlFacts_FrontiersAheadOfHeadSaturateAtZero(t *testing.T) {
	got := BuildFlowControlFacts(FlowControlObservation{
		PrimaryDurableLSN:          101,
		PrimaryTailLSN:             102,
		PrimaryHeadLSN:             100,
		PrimaryBoundsKnown:         true,
		SlowestReplicaDurableLSN:   103,
		SlowestReplicaDurableKnown: true,
		SessionDurableLSN:          104,
		SessionDurableKnown:        true,
	})

	if got.PrimaryFlushLag != 0 ||
		got.RetentionPressure != 0 ||
		got.ReplicaDurableLag != 0 ||
		got.RecoveryBacklog != 0 {
		t.Fatalf("saturated facts=%+v want all LSN pressures zero", got)
	}
}
