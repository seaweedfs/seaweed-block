package component

import (
	"bytes"
	"context"
	"fmt"
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

// recordStores re-sends the primary's replica set at the same generation,
// with the stores the master recorded (a missing entry: none recorded).
func recordStores(t *testing.T, c *Cluster, stores map[string]storage.StoreID) {
	t.Helper()
	targets := make([]replication.ReplicaTarget, 0, c.ReplicaCount())
	for i := 0; i < c.ReplicaCount(); i++ {
		id := fmt.Sprintf("replica-%d", i)
		targets = append(targets, replication.ReplicaTarget{ReplicaID: id, DataAddr: c.Replica(i).Addr, ControlAddr: c.Replica(i).Addr,
			Epoch: 1, EndpointVersion: 1, StoreID: stores[id]})
	}
	if err := c.Primary().RepVol.UpdateReplicaSet(1, targets); err != nil {
		t.Fatal(err)
	}
}

// A replica whose store the master has not recorded (a rebuilt one)
// answers, but its ack never completes a sync_quorum write: with the
// recorded replica down, the flush fails; once the master's descriptor
// names the replica's store, the same flush succeeds.
func TestUnrecordedReplicaAckDoesNotCompleteAQuorumWrite(t *testing.T) {
	c := NewCluster(t, Walstore).WithReplicas(2).WithLiveShip().Start()
	rv := c.Primary().RepVol
	// fixture: a sync_all flush before stores are required, so both
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
	recordStores(t, c, map[string]storage.StoreID{"replica-0": stores["replica-0"]}) // replica-1: not recorded
	c.Replica(0).Listener.StopHard()                                                // the recorded replica is gone
	if err := attestSync(t, c, 2, 0x22); err == nil {
		t.Fatal("a quorum flush completed on the ack of a replica whose store the master never recorded")
	}
	recordStores(t, c, stores)
	if err := attestSync(t, c, 3, 0x33); err != nil {
		t.Fatalf("the flush after replica-1's store was recorded: %v", err)
	}
}
