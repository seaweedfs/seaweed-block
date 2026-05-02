package volume

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// G8-A pins the host/frontend composition for old-primary fencing:
// when the volume-scoped assignment stream says another replica owns
// a newer authority line, the old primary's locally-healthy engine
// projection must fail closed at the durable backend gate.
func TestG8A_SupersedeFact_FencesOldPrimaryDurableBackend(t *testing.T) {
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r1"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)

	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	backend := durable.NewStorageBackend(store, h.view, id)
	backend.SetOperational(true, "g8-b old-primary baseline")

	ctx := context.Background()
	payload := []byte("before-supersede")
	if n, err := backend.Write(ctx, 0, payload); err != nil || n != len(payload) {
		t.Fatalf("baseline write: n=%d err=%v", n, err)
	}

	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r2",
		Epoch:           2,
		EndpointVersion: 1,
		DataAddr:        "r2-data",
		CtrlAddr:        "r2-ctrl",
	})

	if p := h.view.Projection(); p.Healthy {
		t.Fatalf("old primary projection must fail closed after supersede, got %+v", p)
	}
	if _, err := backend.Read(ctx, 0, make([]byte, len(payload))); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("superseded old-primary read: got %v want ErrStalePrimary", err)
	}
	if _, err := backend.Write(ctx, 0, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("superseded old-primary write: got %v want ErrStalePrimary", err)
	}
	if err := backend.Sync(ctx); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("superseded old-primary sync: got %v want ErrStalePrimary", err)
	}
}

func TestG8A_StaleSupersedeFact_DoesNotFenceCurrentPrimaryBackend(t *testing.T) {
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r1"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)

	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3}
	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	backend := durable.NewStorageBackend(store, h.view, id)
	backend.SetOperational(true, "g8-b current-primary baseline")

	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r2",
		Epoch:           4,
		EndpointVersion: 99,
		DataAddr:        "old-data",
		CtrlAddr:        "old-ctrl",
	})

	if p := h.view.Projection(); !p.Healthy {
		t.Fatalf("older cross-replica fact must not fence current primary, got %+v", p)
	}
	if n, err := backend.Write(context.Background(), 0, []byte("still-current")); err != nil || n != len("still-current") {
		t.Fatalf("current primary write after stale supersede: n=%d err=%v", n, err)
	}
}
