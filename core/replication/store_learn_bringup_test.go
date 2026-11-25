package replication

import (
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
