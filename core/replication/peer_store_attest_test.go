package replication

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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

func storeListener(t *testing.T, name string) (*storage.WALStore, *transport.ReplicaListener) {
	t.Helper()
	s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), name), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	l, err := transport.NewReplicaListener("127.0.0.1:0", s)
	if err != nil {
		t.Fatal(err)
	}
	l.Serve()
	t.Cleanup(func() { l.Stop() })
	return s, l
}

// COUNT RULE: where stores are required, a Healthy peer counts toward write
// ACKs only while its barrier reports the store the master's descriptor
// names. An empty descriptor store never matches.
func TestPeerCountsOnlyWithTheStoreTheMasterRecorded(t *testing.T) {
	s1, l := storeListener(t, "s1.wal")
	v := NewReplicationVolume("v", storage.NewBlockStore(64, 4096))
	t.Cleanup(func() { _ = v.Close() })
	target := ReplicaTarget{ReplicaID: "r2", DataAddr: l.Addr(), ControlAddr: l.Addr(), Epoch: 1, EndpointVersion: 1}
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{target}); err != nil {
		t.Fatal(err)
	}
	peer := v.peers["r2"]
	if !peer.countsTowardAcks() {
		t.Fatal("without required stores a Healthy peer must count, as before")
	}
	v.RequireAttestedStores()
	if _, err := peer.Barrier(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if peer.countsTowardAcks() {
		t.Fatal("counted with no store recorded by the master (empty must never match)")
	}
	target.StoreID = storage.StoreID{9}
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{target}); err != nil {
		t.Fatal(err)
	}
	if peer.countsTowardAcks() {
		t.Fatal("counted while the descriptor names another store")
	}
	target.StoreID = s1.StoreID()
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{target}); err != nil { // same generation: applied in place
		t.Fatal(err)
	}
	if v.peers["r2"] != peer {
		t.Fatal("a store-only change recreated the peer")
	}
	if !peer.countsTowardAcks() {
		t.Fatal("does not count with the store the master recorded")
	}
}

// GUARD: a wiped replica reconnects at the same generation with a new
// store. Its barrier answers (that is how the store is learned), but the
// peer's own word never changes the expected store: its acks do not count
// until the master's descriptor names the new store.
func TestWipedReplicaAtTheSameGenerationDoesNotCount(t *testing.T) {
	s1, _ := storeListener(t, "s1.wal")
	s2, wiped := storeListener(t, "s2.wal") // the same replica after its disk was wiped
	v := NewReplicationVolume("v", storage.NewBlockStore(64, 4096))
	t.Cleanup(func() { _ = v.Close() })
	v.RequireAttestedStores()
	target := ReplicaTarget{ReplicaID: "r2", DataAddr: wiped.Addr(), ControlAddr: wiped.Addr(), Epoch: 1, EndpointVersion: 1, StoreID: s1.StoreID()}
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{target}); err != nil {
		t.Fatal(err)
	}
	peer := v.peers["r2"]
	// The volume's store learner barriers this peer through its queue; let it
	// finish before the direct barriers below share the session connection.
	deadline := time.Now().Add(10 * time.Second)
	for peer.learningStore() {
		if time.Now().After(deadline) {
			t.Fatal("the store learner never finished")
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		if _, err := peer.Barrier(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
		if got, _ := peer.AttestedStore(); got != s2.StoreID() {
			t.Fatalf("barrier taught %x, want the wiped replica's new store %x", got, s2.StoreID())
		}
		if peer.countsTowardAcks() {
			t.Fatal("a wiped replica's ack counts at the same generation, on its own word")
		}
	}
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{target}); err != nil { // a replay naming the old store
		t.Fatal(err)
	}
	if peer.countsTowardAcks() {
		t.Fatal("a replay of the old descriptor made the wiped replica count")
	}
	target.StoreID = s2.StoreID()
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{target}); err != nil {
		t.Fatal(err)
	}
	if !peer.countsTowardAcks() {
		t.Fatal("does not count once the master recorded the new store")
	}
}
