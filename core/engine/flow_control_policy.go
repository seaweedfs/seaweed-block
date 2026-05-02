package engine

// FlowControlAction is the coordinator-facing write-pressure decision.
// It is deliberately separate from recovery decisions:
//   - recovery policy decides how to feed a lagging replica;
//   - flow-control policy decides whether the primary write ingress should
//     keep accepting, shape, or fail an explicit durability contract.
type FlowControlAction string

const (
	FlowControlAccept              FlowControlAction = "accept"
	FlowControlShapePrimaryWrites  FlowControlAction = "shape_primary_writes"
	FlowControlFailDurabilityWrite FlowControlAction = "fail_durability_write"
	FlowControlDegradeSlowReplica  FlowControlAction = "degrade_slow_replica"
)

// FlowControlReason is stable test/log vocabulary for a flow-control decision.
type FlowControlReason string

const (
	FlowControlReasonHealthy              FlowControlReason = "healthy"
	FlowControlReasonPrimaryFlushLag      FlowControlReason = "primary_flush_lag"
	FlowControlReasonReplicaRetentionRisk FlowControlReason = "replica_retention_risk"
	FlowControlReasonSyncQuorumMiss       FlowControlReason = "sync_quorum_miss"
	FlowControlReasonRecoveryBacklog      FlowControlReason = "recovery_backlog"
)

// FlowControlVerdict is the result of evaluating primary write pressure.
type FlowControlVerdict struct {
	Action FlowControlAction
	Reason FlowControlReason
}

// FlowControlPolicy defines pressure thresholds. A zero threshold disables
// that check, which keeps tests explicit and avoids magic defaults.
type FlowControlPolicy struct {
	// MaxPrimaryFlushLag is the tolerated gap between the primary's accepted
	// WAL head and its local durable/synced frontier.
	MaxPrimaryFlushLag uint64

	// MaxReplicaDurableLag is the tolerated gap between primary head and the
	// slowest active replica durable ack before that replica is considered a
	// replica-side risk, not a normal feeder backlog.
	MaxReplicaDurableLag uint64

	// MaxRetentionPressure is the tolerated pressure on the primary WAL
	// retention window. When paired with replica lag this means the replica is
	// threatening WAL release; when paired only with primary flush lag it is a
	// primary-local write-pressure problem.
	MaxRetentionPressure uint64

	// MaxSyncQuorumMisses is the tolerated consecutive sync-quorum miss count
	// before the coordinator should classify the lagging member as a replica
	// health problem.
	MaxSyncQuorumMisses uint64

	// MaxRecoveryBacklog is the tolerated feeder/session backlog. It is a
	// replica decision input, not a reason to make every normal write wait.
	MaxRecoveryBacklog uint64
}

// FlowControlFacts are already-observed pressure gauges. They intentionally
// do not carry TargetLSN or raw completion-band fields. The write path should
// not infer recovery truth from this policy, and the feeder should not use it
// to release WAL pins.
type FlowControlFacts struct {
	PrimaryFlushLag    uint64
	ReplicaDurableLag  uint64
	RetentionPressure  uint64
	SyncQuorumMisses   uint64
	RecoveryBacklog    uint64
	ExplicitDurability bool
}

// EvaluateFlowControl classifies primary write pressure without feeding a
// replica, advancing a recycle pin, or starting recovery by itself. Wiring code
// may translate DegradeSlowReplica into a management action, but normal writes
// remain non-blocking until Shape/Fail is explicitly returned.
func EvaluateFlowControl(policy FlowControlPolicy, facts FlowControlFacts) FlowControlVerdict {
	if exceeds(policy.MaxPrimaryFlushLag, facts.PrimaryFlushLag) {
		if facts.ExplicitDurability {
			return FlowControlVerdict{
				Action: FlowControlFailDurabilityWrite,
				Reason: FlowControlReasonPrimaryFlushLag,
			}
		}
		return FlowControlVerdict{
			Action: FlowControlShapePrimaryWrites,
			Reason: FlowControlReasonPrimaryFlushLag,
		}
	}

	if exceeds(policy.MaxReplicaDurableLag, facts.ReplicaDurableLag) &&
		exceeds(policy.MaxRetentionPressure, facts.RetentionPressure) {
		return FlowControlVerdict{
			Action: FlowControlDegradeSlowReplica,
			Reason: FlowControlReasonReplicaRetentionRisk,
		}
	}

	if exceeds(policy.MaxSyncQuorumMisses, facts.SyncQuorumMisses) {
		return FlowControlVerdict{
			Action: FlowControlDegradeSlowReplica,
			Reason: FlowControlReasonSyncQuorumMiss,
		}
	}

	if exceeds(policy.MaxRecoveryBacklog, facts.RecoveryBacklog) &&
		exceeds(policy.MaxRetentionPressure, facts.RetentionPressure) {
		return FlowControlVerdict{
			Action: FlowControlDegradeSlowReplica,
			Reason: FlowControlReasonRecoveryBacklog,
		}
	}

	return FlowControlVerdict{
		Action: FlowControlAccept,
		Reason: FlowControlReasonHealthy,
	}
}

func exceeds(limit, value uint64) bool {
	return limit > 0 && value > limit
}
