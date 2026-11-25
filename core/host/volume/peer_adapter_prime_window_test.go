package volume

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// The window between the identity check and the adapter's first command:
// a dispatch for an old degraded peer passes PrimePendingFor, is held after
// the registry's lock is released, and the peer is replaced under the same
// ReplicaID meanwhile, its replacement seeded on the same replica. When the
// dispatch resumes, its commands run through the old peer's executor, which
// the volume stopped before installing the replacement: they are refused,
// so no connection reaches the replica through the old peer's address after
// the replacement, and the replica keeps the replacement's live session
// (its barrier succeeds).
//
// The replacement keeps the lineage (epoch 1, endpoint version 1) and only
// moves the data address: the old peer reaches the same replica through a
// relay. With an older lineage the replica itself would refuse the old
// adapter's fence, so this is the case in which only the stopped executor
// keeps the late command off the replica.
func TestPeerAdapterPrimeAfterUnlockNeverReachesTheReplacedReplica(t *testing.T) {
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
	relay, relayed := relayTo(t, addr) // a second address of the same replica

	v := replication.NewReplicationVolume("vol1", primary)
	v.SetDurabilityMode(replication.DurabilitySyncQuorum)
	reg := NewPeerAdapterRegistry("vol1", "r1")
	reg.SetFrontier(primary.Sync)
	entered, release := make(chan struct{}), make(chan struct{})
	reg.afterPrimeUnlock = func() { close(entered); <-release }
	var added *replication.ReplicaPeer
	if err := v.ConfigurePeerLifecycleHook(func(p *replication.ReplicaPeer) { added = p; reg.OnPeerAdded(p) }, reg.OnPeerRemoved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close(); reg.Close(); listener.Stop() })
	target := func(at string) []replication.ReplicaTarget {
		return []replication.ReplicaTarget{{ReplicaID: "r2", DataAddr: at, ControlAddr: at, Epoch: 1, EndpointVersion: 1}}
	}

	if err := v.UpdateReplicaSet(1, target(relay)); err != nil {
		t.Fatal(err)
	}
	old := added
	old.Invalidate("its live session failed")
	if !old.ProbeIfDegraded(time.Now()) {
		t.Fatal("the degraded old peer was not admitted")
	}
	fn := ProductionProbeFnWithPrime(reg.AdapterForPeer, reg.PrimePendingFor)
	done := make(chan struct{})
	go func() { _ = fn(context.Background(), old); close(done) }()
	<-entered // checked, unlocked, not yet acted

	if err := v.UpdateReplicaSet(2, target(addr)); err != nil {
		t.Fatal(err)
	}
	fresh := added
	if fresh == old || fresh.State() != replication.ReplicaHealthy {
		t.Fatalf("the replacement is %s; want a new healthy peer", fresh.State())
	}
	before := relayed()
	close(release)
	<-done
	time.Sleep(300 * time.Millisecond) // the old adapter's probe runs asynchronously
	if n := relayed() - before; n != 0 {
		t.Fatalf("%d connections reached the replica through the replaced peer's address after the dispatch resumed", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := fresh.Barrier(ctx, frontier); err != nil {
		t.Fatalf("the replacement's own barrier failed after the old dispatch resumed: %v", err)
	}
	if s := fresh.State(); s != replication.ReplicaHealthy {
		t.Fatalf("the replacement is %s after the old dispatch resumed; want healthy", s)
	}
	if a := reg.AdapterForPeer(fresh); a == nil || len(a.CommandLog()) != 0 {
		t.Fatal("the replacement's adapter acted")
	}
}

// relayTo serves a second local address that forwards every connection to
// addr, and counts them.
func relayTo(t *testing.T, addr string) (string, func() int32) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	var n atomic.Int32
	go func() {
		for {
			in, err := l.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			out, err := net.Dial("tcp", addr)
			if err != nil {
				_ = in.Close()
				continue
			}
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	return l.Addr().String(), n.Load
}
