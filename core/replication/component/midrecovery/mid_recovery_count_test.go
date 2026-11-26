// Package midrecovery_test is its own test binary: its clusters mint live
// sessions that must not change the session order other component tests see.
package midrecovery_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/replication/component"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// attestSync writes one block through the primary and flushes it.
func attestSync(t *testing.T, c *component.Cluster, lba uint32, fill byte) error {
	t.Helper()
	c.PrimaryWriteViaBackend(lba, bytes.Repeat([]byte{fill}, 4096))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Primary().Backend.Sync(ctx)
}

// withEpochTables gives each replica listener its epoch table, as the
// volume does before its role.
func withEpochTables(t *testing.T, c *component.Cluster) []*transport.EpochTable {
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

// beginRecoveryOn starts a recovery on replica i the way a dual-lane
// session does: it takes the epoch table's invalid mark under a fresh token
// and changes nothing else, so the primary's live session stays admissible
// and only the invalid table can refuse its frames.
func beginRecoveryOn(t *testing.T, c *component.Cluster, i int, tbl *transport.EpochTable) uint64 {
	t.Helper()
	tok, err := c.Replica(i).Listener.BeginEpochRecovery()
	if err != nil || !tbl.Invalid() {
		t.Fatalf("fixture: begin recovery: token %d err %v invalid %t", tok, err, tbl.Invalid())
	}
	return tok
}

// completeRecoveryOn ends it: the rows cut at the replica's end make the
// table valid again.
func completeRecoveryOn(t *testing.T, c *component.Cluster, i int, tbl *transport.EpochTable, tok uint64) {
	t.Helper()
	end, err := c.Replica(i).Store.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Replica(i).Listener.CompleteEpochRecovery(tok, transport.EncodeEpochRows(tbl.Rows()), end); err != nil || tbl.Invalid() {
		t.Fatalf("fixture: complete recovery: err %v invalid %t", err, tbl.Invalid())
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
		factory component.SubstrateFactory
	}{{"walstore", component.Walstore}, {"smartwal", component.Smartwal}} {
		for _, mode := range []struct {
			name     string
			replicas int
			mode     replication.DurabilityMode
		}{{"sync_all RF2", 1, replication.DurabilitySyncAll}, {"sync_quorum RF3", 2, replication.DurabilitySyncQuorum}} {
			t.Run(store.name+"/"+mode.name, func(t *testing.T) {
				c := component.NewCluster(t, store.factory).WithReplicas(mode.replicas).WithApplyGate().WithLiveShip().Start()
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
				// Control: the same session, the same steps, the recovery begun
				// and ended (the table valid again): replica-0 alone completes
				// the flush.
				completeRecoveryOn(t, c, 0, tables[0], beginRecoveryOn(t, c, 0, tables[0]))
				if err := attestSync(t, c, 2, 0x22); err != nil {
					t.Fatalf("control: a flush after a recovery ended: %v", err)
				}
				// The only difference: the recovery is still open (the table
				// invalid) when the flush runs.
				beginRecoveryOn(t, c, 0, tables[0])
				if err := attestSync(t, c, 3, 0x33); err == nil {
					t.Fatal("a flush completed while the only replica that could count was mid-recovery")
				}
			})
		}
	}
}
