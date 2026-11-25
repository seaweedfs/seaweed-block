package component

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func attestSync(t *testing.T, c *Cluster, lba uint32, fill byte) error {
	t.Helper()
	c.PrimaryWriteViaBackend(lba, bytes.Repeat([]byte{fill}, 4096))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Primary().Backend.Sync(ctx)
}

// A replica whose store the master has not acknowledged (a rebuilt one)
// answers, but its ack never completes a sync_quorum write: with the
// acknowledged replica down, the flush fails; once the replica's store is
// acknowledged, the same flush succeeds.
func TestUnacknowledgedReplicaAckDoesNotCompleteAQuorumWrite(t *testing.T) {
	c := NewCluster(t, Walstore).WithReplicas(2).WithLiveShip().Start()
	rv := c.Primary().RepVol
	// fixture: a sync_all flush before attestation is required, so both
	// replicas' barriers report their stores
	rv.SetDurabilityMode(replication.DurabilitySyncAll)
	if err := attestSync(t, c, 1, 0x11); err != nil {
		t.Fatalf("fixture: first flush: %v", err)
	}
	stores := rv.AttestedStores()
	if len(stores) != 2 {
		t.Fatalf("fixture: attested %v, want both replicas", stores)
	}
	rv.SetDurabilityMode(replication.DurabilitySyncQuorum)
	rv.RequireAttestedStores()
	rv.ConfirmAttested(map[string]storage.StoreID{"replica-0": stores["replica-0"]}) // replica-1: not acknowledged
	c.Replica(0).Listener.StopHard()                                                 // the acknowledged replica is gone
	if err := attestSync(t, c, 2, 0x22); err == nil {
		t.Fatal("a quorum flush completed on the ack of a replica whose store the master never acknowledged")
	}
	rv.ConfirmAttested(map[string]storage.StoreID{"replica-1": stores["replica-1"]})
	if err := attestSync(t, c, 3, 0x33); err != nil {
		t.Fatalf("the flush after replica-1's store was acknowledged: %v", err)
	}
}

// The defect of the in-memory confirmation: a primary that has just
// started (or a new volume), with every replica Healthy, must not fail a
// flush only because no heartbeat has been acknowledged yet. Red until the
// cure for the first-heartbeat window is ruled (bounded wait, a persisted
// confirmation, or the master recording the store at ack-eligibility).
func TestFreshPrimaryWithHealthyReplicasFailsNoFlush(t *testing.T) {
	t.Skip("red: the first-heartbeat window fails flushes; cure pending a ruling")
	c := NewCluster(t, Walstore).WithReplicas(2).WithLiveShip().Start()
	rv := c.Primary().RepVol
	rv.SetDurabilityMode(replication.DurabilitySyncAll)
	rv.RequireAttestedStores() // as a volume host does when it installs the slot
	if err := attestSync(t, c, 1, 0x44); err != nil {
		t.Fatalf("a fresh primary with healthy replicas failed a flush before its first acknowledged heartbeat: %v", err)
	}
}
