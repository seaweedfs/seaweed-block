package volume

import (
	"net"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// A peer the seed could not reach is degraded. When it answers again with
// the same length but another epoch's history, the engine's "healthy"
// verdict (a probe saw R >= H) must not heal it: only a reconciliation by
// (epoch, LSN) does, and here it says diverged.
func TestPublishHealthyNeverHealsAnEqualLengthDivergentPeer(t *testing.T) {
	const blockSize, n = 4096, 4
	primary := storage.NewBlockStore(64, blockSize)
	replica := storage.NewBlockStore(64, blockSize)
	for i := 0; i < n; i++ {
		lsn, err := primary.Write(uint32(i), make([]byte, blockSize))
		if err != nil {
			t.Fatal(err)
		}
		other := make([]byte, blockSize)
		other[0] = 0xB0
		if err := replica.ApplyEntry(uint32(i), other, lsn); err != nil {
			t.Fatal(err)
		}
	}
	own, _ := transport.LoadEpochTable("", "vol1")
	_ = own.Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 3}})
	theirs, _ := transport.LoadEpochTable("", "vol1")
	_ = theirs.Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}})

	// A free address with nothing behind it yet: the seed barrier fails.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	v := replication.NewReplicationVolume("vol1", primary)
	v.SetDurabilityMode(replication.DurabilitySyncQuorum)
	v.SetEpochTable(own)
	t.Cleanup(func() { _ = v.Close() })
	var peer *replication.ReplicaPeer
	if err := v.ConfigurePeerLifecycleHook(func(p *replication.ReplicaPeer) { peer = p }, func(string) {}); err != nil {
		t.Fatal(err)
	}
	if err := v.UpdateReplicaSet(1, []replication.ReplicaTarget{{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: 2, EndpointVersion: 1}}); err != nil {
		t.Fatal(err)
	}
	if peer == nil || peer.State() != replication.ReplicaDegraded {
		t.Fatalf("peer %v; want degraded after an unanswered seed barrier", peer)
	}

	listener, err := transport.NewReplicaListener(addr, replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetEpochTable(theirs)
	listener.Serve()
	// The primary's connection closes first, or Stop waits for it.
	t.Cleanup(func() { _ = v.Close(); listener.Stop() })

	NewPeerCommandExecutor(peer).PublishHealthy("r2")
	if s := peer.State(); s != replication.ReplicaNeedsRebuild {
		t.Fatalf("peer state %s after the engine's healthy verdict; want needs_rebuild (equal length, other epoch)", s)
	}
}
