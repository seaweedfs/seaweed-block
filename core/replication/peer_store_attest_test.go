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

// ATTEST-BEFORE-COUNT: where stores are attested, a Healthy peer counts
// toward write ACKs only once the master has acknowledged its current
// store. The red schedule: the peer counted with store S1, its disk is
// replaced (S2), it heals; until an acked attestation names S2 it must not
// count, or it could hold the only copy of an acknowledged write that no
// promotion can verify.
func TestPeerCountsTowardAcksOnlyWithAnAckedStore(t *testing.T) {
	s1, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "s1.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	listener, err := transport.NewReplicaListener("127.0.0.1:0", s1)
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
	if !peer.countsTowardAcks() {
		t.Fatal("without attestation a Healthy peer must count, as before")
	}
	peer.setRequireAttest()
	if peer.countsTowardAcks() {
		t.Fatal("counted before any barrier taught its store")
	}
	if _, err := peer.Barrier(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if peer.countsTowardAcks() {
		t.Fatal("counted with a store the master has not acknowledged")
	}
	peer.confirmStore(storage.StoreID{9}) // an ack of another store
	if peer.countsTowardAcks() {
		t.Fatal("counted after an ack naming another store")
	}
	peer.confirmStore(storage.StoreIDOf(s1))
	if !peer.countsTowardAcks() {
		t.Fatal("does not count after its store's attestation was acked")
	}
	// The disk is replaced and the peer rebuilt: it heals, and its next
	// barrier reports the new store S2.
	peer.Invalidate("test: disk replaced")
	peer.SetState(ReplicaHealthy)
	s2 := storage.StoreID{2, 2}
	peer.mu.Lock()
	peer.storeID = s2
	peer.mu.Unlock()
	if peer.countsTowardAcks() {
		t.Fatal("the rebuilt peer counted on its old store's ack (S1) with a new store (S2)")
	}
	peer.confirmStore(s2)
	if !peer.countsTowardAcks() {
		t.Fatal("the rebuilt peer does not count after S2's attestation was acked")
	}
}

// ConfirmAttested routes an acknowledged attestation to the peers it names.
func TestVolumeConfirmAttestedReachesItsPeers(t *testing.T) {
	v := NewReplicationVolume("v", storage.NewBlockStore(64, 4096))
	t.Cleanup(func() { _ = v.Close() })
	v.RequireAttestedStores()
	if !v.requireAttest {
		t.Fatal("RequireAttestedStores did not take")
	}
	v.ConfirmAttested(map[string]storage.StoreID{"absent": {1}}) // no peer: nothing to do
}
