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
func TestNewVolumePrimaryServesNothingBeforeReplicaAttestations(t *testing.T) {
	h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
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
