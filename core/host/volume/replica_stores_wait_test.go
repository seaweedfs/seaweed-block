package volume

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// A new volume is not exposed before its replicas' stores are attested:
// while the master's fact awaits any initial replica's store, the primary's
// projection is not Healthy, writes through the real backend refuse, and
// status says "creating: waiting for replica attestations (n of N)". The
// same-generation re-send with nothing awaited opens it.
// requiringReplication is a replication slot that counts peers only with
// recorded stores (as replication.ReplicationVolume does on a volume host).
type requiringReplication struct{ *stubReplication }

func (requiringReplication) RequireAttestedStores() {}

func TestNewVolumePrimaryServesNothingBeforeReplicaAttestations(t *testing.T) {
	h, _, rep := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.replicationMu.Lock()
	h.setReplicationLocked(requiringReplication{rep})
	h.replicationMu.Unlock()
	proj := &mutableProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 1}}
	h.view = NewAdapterProjectionView(proj, "vol1", "rB", h)
	if err := h.SetSelfFence(&switchFencer{}); err != nil {
		t.Fatal(err)
	}
	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	backend := durable.NewStorageBackend(store, h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "rB", Epoch: 2, EndpointVersion: 1})
	backend.SetOperational(true, "replica stores wait")
	ctx := context.Background()

	fact := selfFact(2)
	fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = 2, 3
	h.applyFixtureFact(fact)
	if h.view.Projection().Healthy {
		t.Fatal("a new volume's primary is Healthy before its replicas' stores are recorded")
	}
	if _, err := backend.Write(ctx, 0, []byte("early")); err == nil {
		t.Fatal("a write was served before the replicas' stores were recorded")
	}
	s := NewStatusServer(h.view).statusProjection()
	if s.FrontendPrimaryReady || !strings.Contains(s.NotReadyReason, "creating: waiting for replica attestations (1 of 3)") {
		t.Fatalf("status: ready=%t reason=%q", s.FrontendPrimaryReady, s.NotReadyReason)
	}

	fact = selfFact(2) // the master's re-send once every store is recorded
	h.applyFixtureFact(fact)
	if !h.view.Projection().Healthy {
		t.Fatal("not Healthy once nothing is awaited")
	}
	if _, err := backend.Write(ctx, 0, []byte("open")); err != nil {
		t.Fatalf("write once nothing is awaited: %v", err)
	}
}

// A host whose replication slot does not require recorded stores counts no
// peer ack by store, so the wait does not apply to it: the fact's count
// does not close its frontend.
func TestReplicaStoresWaitOnlyWhereStoresAreRequired(t *testing.T) {
	h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	fact := selfFact(2)
	fact.ReplicaStoresAwaited, fact.ReplicaStoresInitial = 2, 2
	h.noteReplicaStores(fact)
	if reason := h.ReplicaStoresAwaited(); reason != "" {
		t.Fatalf("a host without a store-requiring slot waits: %q", reason)
	}
}
