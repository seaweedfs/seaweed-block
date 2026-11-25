package replication

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// Bring-up order: the primary installs its peers before a replica listens
// (the hosts start one after another). The barrier that learns the peer's
// store fails then; it must not degrade the peer (nothing re-probes a
// degraded peer while no write runs, so its store would never be attested
// and a new volume would never leave creating), and the store is learned
// once the replica answers.
func TestStoreLearnedOnceAReplicaListensAfterTheAssignment(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close() // nothing listens on addr yet

	v := NewReplicationVolume("v", storage.NewBlockStore(64, 4096))
	defer v.Close() // before the listener's cleanup: Stop waits for the peer's connection
	v.RequireAttestedStores()
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: 1, EndpointVersion: 1}}); err != nil {
		t.Fatal(err)
	}
	if s := v.peers["r2"].State(); s != ReplicaHealthy {
		t.Fatalf("a failed store-learning barrier degraded the peer (%s); nothing would ever attest it", s)
	}

	s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "r2.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	l, err := transport.NewReplicaListener(addr, s)
	if err != nil {
		t.Skipf("could not re-bind %s: %v", addr, err)
	}
	l.Serve()
	t.Cleanup(func() { l.Stop() })

	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := v.AttestedStores()["r2"]; got == s.StoreID() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the replica listens, but its store was never learned (attested %v)", v.AttestedStores())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The store learner and the first writes share the peer's connection. Run
// with -race: a write racing a learner must not touch the connection from
// two goroutines (the learner runs on the peer's queue worker, the single
// writer).
func TestStoreLearnerDoesNotRaceTheFirstWrites(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	local, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "p.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	v := NewReplicationVolume("v", local)
	defer v.Close()
	v.RequireAttestedStores()
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: 1, EndpointVersion: 1}}); err != nil {
		t.Fatal(err)
	}
	s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "r2.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	l, err := transport.NewReplicaListener(addr, s)
	if err != nil {
		t.Skipf("could not re-bind %s: %v", addr, err)
	}
	l.Serve()
	t.Cleanup(func() { l.Stop() })

	// Writes start the moment the replica listens, while the learner retries.
	data := make([]byte, 4096)
	for i := uint32(0); i < 40; i++ {
		lsn, err := local.Write(i%64, data)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = v.OnLocalWrite(ctx, LocalWrite{LBA: i % 64, Data: data, LSN: lsn})
		cancel()
	}
	deadline := time.Now().Add(10 * time.Second)
	for v.AttestedStores()["r2"] != s.StoreID() {
		if time.Now().After(deadline) {
			t.Fatalf("store never learned while writes ran (attested %v)", v.AttestedStores())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The learner's barrier connects but its read fails (the replica's process
// accepts and drops the connection, then restarts). The broken connection
// must not be reused: the next attempt re-dials and learns the store.
func TestStoreLearnerRedialsAfterABrokenConnection(t *testing.T) {
	drop, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := drop.Addr().String()
	go func() {
		for {
			c, err := drop.Accept()
			if err != nil {
				return
			}
			_ = c.Close() // connected, then the read fails
		}
	}()
	v := NewReplicationVolume("v", storage.NewBlockStore(64, 4096))
	defer v.Close()
	v.RequireAttestedStores()
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: 1, EndpointVersion: 1}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond) // a few connected, failed attempts
	_ = drop.Close()

	s, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "r2.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	l, err := transport.NewReplicaListener(addr, s)
	if err != nil {
		t.Skipf("could not re-bind %s: %v", addr, err)
	}
	l.Serve()
	t.Cleanup(func() { l.Stop() })
	deadline := time.Now().Add(10 * time.Second)
	for v.AttestedStores()["r2"] != s.StoreID() {
		if time.Now().After(deadline) {
			t.Fatalf("the replica restarted, but the learner kept a broken connection (attested %v)", v.AttestedStores())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
