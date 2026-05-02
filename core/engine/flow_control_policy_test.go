package engine

import "testing"

func TestFlowControl_PrimaryFlushLagShapesWritesNotReplicaDecision(t *testing.T) {
	policy := FlowControlPolicy{
		MaxPrimaryFlushLag:   10,
		MaxReplicaDurableLag: 10,
		MaxRetentionPressure: 10,
		MaxSyncQuorumMisses:  3,
		MaxRecoveryBacklog:   10,
	}
	facts := FlowControlFacts{
		PrimaryFlushLag:   11,
		ReplicaDurableLag: 99,
		RetentionPressure: 99,
	}

	got := EvaluateFlowControl(policy, facts)
	if got.Action != FlowControlShapePrimaryWrites {
		t.Fatalf("Action=%s want %s", got.Action, FlowControlShapePrimaryWrites)
	}
	if got.Reason != FlowControlReasonPrimaryFlushLag {
		t.Fatalf("Reason=%s want %s", got.Reason, FlowControlReasonPrimaryFlushLag)
	}
}

func TestFlowControl_ExplicitDurabilityWriteFailsWhenPrimaryCannotFlush(t *testing.T) {
	policy := FlowControlPolicy{MaxPrimaryFlushLag: 10}
	facts := FlowControlFacts{
		PrimaryFlushLag:    11,
		ExplicitDurability: true,
	}

	got := EvaluateFlowControl(policy, facts)
	if got.Action != FlowControlFailDurabilityWrite {
		t.Fatalf("Action=%s want %s", got.Action, FlowControlFailDurabilityWrite)
	}
	if got.Reason != FlowControlReasonPrimaryFlushLag {
		t.Fatalf("Reason=%s want %s", got.Reason, FlowControlReasonPrimaryFlushLag)
	}
}

func TestFlowControl_ReplicaRetentionRiskDegradesSlowReplica(t *testing.T) {
	policy := FlowControlPolicy{
		MaxReplicaDurableLag: 10,
		MaxRetentionPressure: 20,
	}
	facts := FlowControlFacts{
		ReplicaDurableLag: 11,
		RetentionPressure: 21,
	}

	got := EvaluateFlowControl(policy, facts)
	if got.Action != FlowControlDegradeSlowReplica {
		t.Fatalf("Action=%s want %s", got.Action, FlowControlDegradeSlowReplica)
	}
	if got.Reason != FlowControlReasonReplicaRetentionRisk {
		t.Fatalf("Reason=%s want %s", got.Reason, FlowControlReasonReplicaRetentionRisk)
	}
}

func TestFlowControl_ReplicaLagWithoutRetentionPressureKeepsFeeding(t *testing.T) {
	policy := FlowControlPolicy{
		MaxReplicaDurableLag: 10,
		MaxRetentionPressure: 20,
	}
	facts := FlowControlFacts{
		ReplicaDurableLag: 99,
		RetentionPressure: 20,
	}

	got := EvaluateFlowControl(policy, facts)
	if got.Action != FlowControlAccept {
		t.Fatalf("Action=%s want %s", got.Action, FlowControlAccept)
	}
}

func TestFlowControl_SyncQuorumMissDegradesReplica(t *testing.T) {
	policy := FlowControlPolicy{MaxSyncQuorumMisses: 3}
	facts := FlowControlFacts{SyncQuorumMisses: 4}

	got := EvaluateFlowControl(policy, facts)
	if got.Action != FlowControlDegradeSlowReplica {
		t.Fatalf("Action=%s want %s", got.Action, FlowControlDegradeSlowReplica)
	}
	if got.Reason != FlowControlReasonSyncQuorumMiss {
		t.Fatalf("Reason=%s want %s", got.Reason, FlowControlReasonSyncQuorumMiss)
	}
}

func TestFlowControl_RecoveryBacklogUnderRetentionPressureDegradesReplica(t *testing.T) {
	policy := FlowControlPolicy{
		MaxRecoveryBacklog:   10,
		MaxRetentionPressure: 20,
	}
	facts := FlowControlFacts{
		RecoveryBacklog:   11,
		RetentionPressure: 21,
	}

	got := EvaluateFlowControl(policy, facts)
	if got.Action != FlowControlDegradeSlowReplica {
		t.Fatalf("Action=%s want %s", got.Action, FlowControlDegradeSlowReplica)
	}
	if got.Reason != FlowControlReasonRecoveryBacklog {
		t.Fatalf("Reason=%s want %s", got.Reason, FlowControlReasonRecoveryBacklog)
	}
}

func TestFlowControl_HealthyPressureAccepts(t *testing.T) {
	policy := FlowControlPolicy{
		MaxPrimaryFlushLag:   10,
		MaxReplicaDurableLag: 10,
		MaxRetentionPressure: 20,
		MaxSyncQuorumMisses:  3,
		MaxRecoveryBacklog:   10,
	}
	facts := FlowControlFacts{
		PrimaryFlushLag:   10,
		ReplicaDurableLag: 10,
		RetentionPressure: 20,
		SyncQuorumMisses:  3,
		RecoveryBacklog:   10,
	}

	got := EvaluateFlowControl(policy, facts)
	if got.Action != FlowControlAccept {
		t.Fatalf("Action=%s want %s", got.Action, FlowControlAccept)
	}
	if got.Reason != FlowControlReasonHealthy {
		t.Fatalf("Reason=%s want %s", got.Reason, FlowControlReasonHealthy)
	}
}
