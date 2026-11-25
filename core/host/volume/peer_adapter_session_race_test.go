package volume

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// A new peer is healthy and owned by the volume's live session: its seed
// barrier, then its live ship. The per-peer adapter (with the degraded-probe
// loop running) must not open sessions of its own on that replica, or the
// replica keeps the newest session and refuses the volume's barrier as
// stale. Each lineage bump adds a fresh peer while the loop ticks; the
// volume's own barrier on it must succeed every time.
func TestPeerAdapterNeverTakesAHealthyPeersSession(t *testing.T) {
	const blockSize, n = 4096, 4
	primary := storage.NewBlockStore(64, blockSize)
	replica := storage.NewBlockStore(64, blockSize)
	for i := 0; i < n; i++ {
		data := make([]byte, blockSize)
		data[0] = byte(i + 1)
		lsn, err := primary.Write(uint32(i), data)
		if err != nil {
			t.Fatal(err)
		}
		if err := replica.ApplyEntry(uint32(i), data, lsn); err != nil {
			t.Fatal(err)
		}
	}
	frontier, err := primary.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Sync(); err != nil {
		t.Fatal(err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	listener, err := transport.NewReplicaListener(addr, replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()

	v := replication.NewReplicationVolume("vol1", primary)
	v.SetDurabilityMode(replication.DurabilitySyncQuorum)
	reg := NewPeerAdapterRegistry("vol1", "r1")
	reg.SetFrontier(primary.Sync)
	var added *replication.ReplicaPeer // under v's lifecycle hook, read after UpdateReplicaSet
	if err := v.ConfigurePeerLifecycleHook(func(p *replication.ReplicaPeer) { added = p; reg.OnPeerAdded(p) }, reg.OnPeerRemoved); err != nil {
		t.Fatal(err)
	}
	cfg := replication.ProbeLoopConfig{Interval: time.Millisecond, MaxConcurrent: 1, CooldownBase: time.Millisecond, CooldownCap: 10 * time.Millisecond}
	if err := v.ConfigureProbeLoop(cfg, ProductionProbeFnWithPrime(reg.AdapterFor, reg.PrimePending), time.Now); err != nil {
		t.Fatal(err)
	}
	if err := v.StartProbeLoop(); err != nil {
		t.Fatal(err)
	}
	// The primary's connections close first, or Stop waits for them.
	t.Cleanup(func() { _ = v.Close(); reg.Close(); listener.Stop() })

	for epoch := uint64(1); epoch <= 8; epoch++ {
		if err := v.UpdateReplicaSet(epoch, []replication.ReplicaTarget{{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: epoch, EndpointVersion: 1}}); err != nil {
			t.Fatal(err)
		}
		peer := added
		if peer == nil || peer.Target().Epoch != epoch {
			t.Fatalf("epoch %d: no peer r2", epoch)
		}
		if s := peer.State(); s != replication.ReplicaHealthy {
			t.Fatalf("epoch %d: the seed left r2 %s; want healthy", epoch, s)
		}
		// Give an adapter that acted at the add time to run its probe
		// and fence, while the loop keeps ticking.
		time.Sleep(100 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ack, err := peer.Barrier(ctx, frontier)
		cancel()
		if err != nil {
			t.Fatalf("epoch %d: the volume's own barrier on its healthy peer failed: %v", epoch, err)
		}
		if ack.AchievedLSN < frontier {
			t.Fatalf("epoch %d: barrier achieved %d, want %d", epoch, ack.AchievedLSN, frontier)
		}
		if s := peer.State(); s != replication.ReplicaHealthy {
			t.Fatalf("epoch %d: r2 %s after its barrier; want healthy", epoch, s)
		}
	}
}
