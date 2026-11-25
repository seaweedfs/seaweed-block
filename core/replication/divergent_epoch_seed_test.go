package replication

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// seedWithTables installs one survivor whose store holds lsns 1..n and
// whose epoch table is replicaRows, behind a promoted primary whose table is
// primaryRows and whose frontier is also n, and returns the peer's state
// after the promotion seed barrier.
func seedWithTables(t *testing.T, n int, replicaRows, primaryRows []transport.EpochRow) ReplicaState {
	t.Helper()
	const blockSize = 4096
	primary := storage.NewBlockStore(64, blockSize)
	replica := storage.NewBlockStore(64, blockSize)
	for i := 0; i < n; i++ {
		p := make([]byte, blockSize)
		p[0] = 0xA0
		lsn, err := primary.Write(uint32(i), p)
		if err != nil {
			t.Fatal(err)
		}
		r := make([]byte, blockSize)
		r[0] = 0xB0 // the survivor's bytes differ from the new primary's above the shared prefix
		if err := replica.ApplyEntry(uint32(i), r, lsn); err != nil {
			t.Fatal(err)
		}
	}
	replicaTable, _ := transport.LoadEpochTable("", "vol-promoted")
	if err := replicaTable.Replace(replicaRows); err != nil {
		t.Fatal(err)
	}
	primaryTable, _ := transport.LoadEpochTable("", "vol-promoted")
	if err := primaryTable.Replace(primaryRows); err != nil {
		t.Fatal(err)
	}
	listener, err := transport.NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetEpochTable(replicaTable)
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })

	v := NewReplicationVolume("vol-promoted", primary)
	v.SetDurabilityMode(DurabilitySyncQuorum)
	v.SetEpochTable(primaryTable)
	t.Cleanup(func() { _ = v.Close() })
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r3", listener.Addr(), 2, 1)}); err != nil {
		t.Fatalf("UpdateReplicaSet: %v", err)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.peers["r3"].State()
}

// Equal length, different history: the survivor's lsns 3..4 are epoch 1, the
// new primary's are epoch 2. A length check sees them in sync; the epoch
// table does not, and the survivor is taken out of every count to be rebuilt.
func TestPromotionSeed_EqualLengthDivergentEpochIsRebuilt(t *testing.T) {
	got := seedWithTables(t, 4,
		[]transport.EpochRow{{Epoch: 1, FirstLSN: 1}},
		[]transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 3}})
	if got != ReplicaNeedsRebuild {
		t.Fatalf("peer state %s; want needs_rebuild for an equal-length survivor of another epoch", got)
	}
}

// The controls: the same history is in sync; a survivor without a table
// (data from before the table) is rebuilt; no table on the primary keeps the
// length checks.
func TestPromotionSeed_EpochTableControls(t *testing.T) {
	for _, c := range []struct {
		name         string
		replica, own []transport.EpochRow
		want         ReplicaState
	}{
		{"same history", []transport.EpochRow{{Epoch: 1, FirstLSN: 1}}, []transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 5}}, ReplicaHealthy},
		{"survivor without a table", nil, []transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 5}}, ReplicaNeedsRebuild},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := seedWithTables(t, 4, c.replica, c.own); got != c.want {
				t.Fatalf("peer state %s; want %s", got, c.want)
			}
		})
	}
}
