package component_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/replication/component"
)

func TestComponent_FlowControlObservation_FromStorageBoundariesAndDurableAck(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			Start()

		c.PrimaryWriteN(12)
		primaryR, primaryS, primaryH := c.Primary().Store.Boundaries()
		if primaryH == 0 {
			t.Fatal("setup: primary H must advance after writes")
		}

		a := c.Adapter(0)
		a.SetFlowControlPolicy(engine.FlowControlPolicy{
			MaxPrimaryFlushLag:   2,
			MaxReplicaDurableLag: 100,
			MaxRetentionPressure: 100,
			MaxRecoveryBacklog:   100,
		})

		beforeCommands := len(a.CommandLog())
		obs := adapter.FlowControlObservationFromDurableAck(primaryR, primaryS, primaryH, adapter.DurableAckResult{
			ReplicaID:  "replica-0",
			SessionID:  7,
			DurableLSN: primaryH - 1,
		})
		got := a.OnFlowControlObservation(obs)

		if got.Action != engine.FlowControlShapePrimaryWrites {
			t.Fatalf("Action=%s want %s (primary local flush lag should dominate)",
				got.Action, engine.FlowControlShapePrimaryWrites)
		}
		if got.Reason != engine.FlowControlReasonPrimaryFlushLag {
			t.Fatalf("Reason=%s want %s", got.Reason, engine.FlowControlReasonPrimaryFlushLag)
		}
		if afterCommands := len(a.CommandLog()); afterCommands != beforeCommands {
			t.Fatalf("flow-control observation changed command log: before=%d after=%d log=%v",
				beforeCommands, afterCommands, a.CommandLog())
		}
	})
}

func TestComponent_FlowControlObservation_SyncedPrimaryReplicaRetentionRisk(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			Start()

		c.PrimaryWriteN(16)
		c.PrimarySync()
		primaryR, primaryS, primaryH := c.Primary().Store.Boundaries()
		if primaryR < primaryH {
			t.Fatalf("setup: primary sync did not catch up R=%d H=%d", primaryR, primaryH)
		}

		a := c.Adapter(0)
		a.SetFlowControlPolicy(engine.FlowControlPolicy{
			MaxPrimaryFlushLag:   2,
			MaxReplicaDurableLag: 4,
			MaxRetentionPressure: 4,
		})

		got := a.OnFlowControlObservation(adapter.FlowControlObservationFromDurableAck(primaryR, primaryS, primaryH, adapter.DurableAckResult{
			ReplicaID:  "replica-0",
			SessionID:  8,
			DurableLSN: primaryS,
		}))

		if got.Action != engine.FlowControlDegradeSlowReplica {
			t.Fatalf("Action=%s want %s", got.Action, engine.FlowControlDegradeSlowReplica)
		}
		if got.Reason != engine.FlowControlReasonReplicaRetentionRisk {
			t.Fatalf("Reason=%s want %s", got.Reason, engine.FlowControlReasonReplicaRetentionRisk)
		}
		if hasCommand(a.CommandLog(), "PublishDegraded") {
			t.Fatalf("flow-control verdict must not publish degraded directly; log=%v", a.CommandLog())
		}
	})
}

func hasCommand(commands []string, want string) bool {
	for _, got := range commands {
		if got == want {
			return true
		}
	}
	return false
}
