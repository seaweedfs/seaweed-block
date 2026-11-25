package volume

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// A probe dispatch admitted for an old degraded peer, held while the peer
// is replaced under the same ReplicaID (lineage bump), then resumed: it acts
// for the peer it was admitted for, never for the new healthy one. The new
// peer's adapter issues nothing, stays pending, and no wire probe reaches
// the new peer's address.
func TestPeerAdapterAReplacedPeersDispatchNeverActsOnItsSuccessor(t *testing.T) {
	// The new peer's address counts every connection made to it.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var dials atomic.Int32
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			dials.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = l.Close() })

	peerAt := func(addr string, epoch uint64) *replication.ReplicaPeer {
		exec := transport.NewBlockExecutor(storage.NewBlockStore(8, 4096), addr)
		p, err := replication.NewReplicaPeer(replication.ReplicaTarget{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: epoch, EndpointVersion: 1}, exec)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	reg := NewPeerAdapterRegistry("vol1", "r1")
	old := peerAt("127.0.0.1:1", 1)
	reg.OnPeerAdded(old)
	old.Invalidate("its live session failed")
	if !old.ProbeIfDegraded(time.Now()) { // the loop admits the dispatch
		t.Fatal("the degraded old peer was not admitted")
	}

	// The dispatch is held here; the peer is replaced under the same id.
	reg.OnPeerRemoved("r2")
	fresh := peerAt(l.Addr().String(), 2)
	reg.OnPeerAdded(fresh)

	// The held dispatch resumes, for the peer it was admitted for.
	fn := ProductionProbeFnWithPrime(reg.AdapterForPeer, reg.PrimePendingFor)
	_ = fn(context.Background(), old)

	time.Sleep(300 * time.Millisecond) // an adapter probe runs asynchronously
	if a := reg.AdapterFor("r2"); a == nil || len(a.CommandLog()) != 0 {
		t.Fatalf("the new healthy peer's adapter acted for the old peer's dispatch: %v", a.CommandLog())
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("%d connections reached the new healthy peer from the old peer's dispatch", n)
	}
	if !reg.PrimePending("r2") {
		t.Fatal("the new peer is no longer pending: the old dispatch primed it")
	}
}
