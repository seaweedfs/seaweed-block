package replication

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// A peer attests the store its barrier reported, only while Healthy: a
// peer that leaves Healthy loses it, and one healed without a barrier is
// not attested until its next barrier answers.
func TestPeerAttestsTheStoreItsBarrierReported(t *testing.T) {
	replicaStore, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "r.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replicaStore.Close() })
	listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })
	exec := transport.NewBlockExecutor(storage.NewBlockStore(64, 4096), listener.Addr())
	peer, err := NewReplicaPeer(ReplicaTarget{ReplicaID: "r2", DataAddr: listener.Addr(), ControlAddr: listener.Addr(), Epoch: 1, EndpointVersion: 1}, exec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	want := storage.StoreIDOf(replicaStore)
	if want.IsZero() {
		t.Fatal("a WAL store carries no id")
	}

	if _, ok := peer.AttestedStore(); ok {
		t.Fatal("attested before any barrier")
	}
	if _, err := peer.Barrier(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got, ok := peer.AttestedStore(); !ok || got != want {
		t.Fatalf("after a barrier: %x %t, want %x", got, ok, want)
	}
	peer.Invalidate("test: shipping failed")
	if _, ok := peer.AttestedStore(); ok {
		t.Fatal("a Degraded peer is still attested")
	}
	peer.SetState(ReplicaHealthy)
	if peer.State() != ReplicaHealthy {
		t.Fatalf("state %s, want Healthy", peer.State())
	}
	if _, ok := peer.AttestedStore(); ok {
		t.Fatal("healed without a barrier, yet attested with the old store")
	}
	if _, err := peer.Barrier(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got, ok := peer.AttestedStore(); !ok || got != want {
		t.Fatalf("after the next barrier: %x %t, want %x", got, ok, want)
	}
}
