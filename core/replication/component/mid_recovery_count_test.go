package component

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// withEpochTables gives each replica listener its epoch table, as the
// volume does before its role.
func withEpochTables(t *testing.T, c *Cluster) []*transport.EpochTable {
	t.Helper()
	tables := make([]*transport.EpochTable, c.ReplicaCount())
	for i := range tables {
		tbl, err := transport.LoadEpochTable(filepath.Join(t.TempDir(), "v1.epoch-table"), "v1")
		if err != nil {
			t.Fatal(err)
		}
		c.Replica(i).Listener.SetEpochTable(tbl)
		tables[i] = tbl
	}
	return tables
}

// startRecoveryOn opens a catch-up lane on replica i from its end and
// leaves it open: the replica is mid-catch-up, its epoch table invalid.
func startRecoveryOn(t *testing.T, c *Cluster, i int, tbl *transport.EpochTable) {
	t.Helper()
	end, err := c.Replica(i).Store.Sync()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", c.Replica(i).Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	lineage := transport.RecoveryLineage{SessionID: 1 << 40, Epoch: 1, EndpointVersion: 1, TargetLSN: end + 10}
	if err := transport.WriteMsg(conn, transport.MsgRecoveryLaneStart, transport.EncodeRecoveryLaneStart(lineage, end)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !tbl.Invalid() {
		if time.Now().After(deadline) {
			t.Fatal("fixture: the recovery lane never took the replica's store")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A replica mid-catch-up (its epoch table invalid until the recovery's
// final barrier) never completes a write: under sync_all (RF2) and under
// sync_quorum (RF3, the other replica down), the flush fails while it is
// the only replica that could count. Its reported end may be above a gap
// (a per-block catch-up ships no contiguous LSNs), so it must not count.
func TestReplicaMidCatchUpNeverCompletesAWrite(t *testing.T) {
	for _, store := range []struct {
		name    string
		factory SubstrateFactory
	}{{"walstore", Walstore}, {"smartwal", Smartwal}} {
		for _, mode := range []struct {
			name     string
			replicas int
			mode     replication.DurabilityMode
		}{{"sync_all RF2", 1, replication.DurabilitySyncAll}, {"sync_quorum RF3", 2, replication.DurabilitySyncQuorum}} {
			t.Run(store.name+"/"+mode.name, func(t *testing.T) {
				c := NewCluster(t, store.factory).WithReplicas(mode.replicas).WithApplyGate().WithLiveShip().Start()
				tables := withEpochTables(t, c)
				rv := c.Primary().RepVol
				rv.SetDurabilityMode(replication.DurabilitySyncAll)
				if err := attestSync(t, c, 1, 0x11); err != nil {
					t.Fatalf("fixture: a flush before the recovery: %v", err)
				}
				rv.SetDurabilityMode(mode.mode)
				if mode.replicas == 2 {
					c.Replica(1).Listener.StopHard() // the only other replica is gone
				}
				// Control: replica-0 alone completes the flush before its recovery.
				if err := attestSync(t, c, 2, 0x22); err != nil {
					t.Fatalf("control: a flush with replica-0 healthy: %v", err)
				}
				startRecoveryOn(t, c, 0, tables[0])
				if err := attestSync(t, c, 3, 0x33); err == nil {
					t.Fatal("a flush completed while the only replica that could count was mid-catch-up")
				}
			})
		}
	}
}
