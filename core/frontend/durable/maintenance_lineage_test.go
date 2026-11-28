package durable

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type maintenanceStubLayer struct{ storage.ChunkLayer }

func (maintenanceStubLayer) Fenced() bool { return false }
func TestMaintenanceRejectsForeignLineageBeforeAdmission(t *testing.T) {
	for _, field := range []string{"epoch", "replica", "endpoint", "handle", "layer"} {
		t.Run(field, func(t *testing.T) {
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}
			lin := chunkLineage{"v1", "r1", 3, 1}
			layer := &maintenanceStubLayer{}
			h := &volHandle{chunk: layer}
			p := &DurableProvider{volumes: map[string]*volHandle{"v1": h}, view: precopyQueueView{id}}
			if !p.maintenanceCurrent("v1", h, layer, lin) {
				t.Fatal("healthy same attachment refused")
			}
			switch field {
			case "epoch":
				id.Epoch++
			case "replica":
				id.ReplicaID = "r2"
			case "endpoint":
				id.EndpointVersion++
			case "handle":
				p.volumes["v1"] = &volHandle{chunk: layer}
			case "layer":
				h.chunk = &maintenanceStubLayer{}
			}
			p.view = precopyQueueView{id}
			p.cfg.ChunkMaintenanceInterval = time.Nanosecond
			calls := 0
			p.cfg.ChunkMaintenance = func(context.Context, string) error { calls++; return nil }
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			p.chunkMaintenanceLoop(ctx, "v1", layer, func() bool { return p.maintenanceCurrent("v1", h, layer, lin) }, nil, nil)
			if calls != 0 {
				t.Fatalf("foreign attachment admitted %d passes", calls)
			}
		})
	}
}
func TestMaintenanceSameHandleRearmsAfterJoinedStop(t *testing.T) {
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}
	layer := &maintenanceStubLayer{}
	h := &volHandle{chunk: layer, chunkLineage: chunkLineage{"v1", "r1", 3, 1}}
	entered := make(chan struct{}, 2)
	p := &DurableProvider{volumes: map[string]*volHandle{"v1": h}, view: precopyQueueView{id}, cfg: ProviderConfig{ChunkMaintenanceInterval: time.Nanosecond, ChunkMaintenance: func(ctx context.Context, _ string) error { entered <- struct{}{}; <-ctx.Done(); return ctx.Err() }}}
	var stopperDone chan struct{}
	t.Cleanup(func() {
		p.mu.Lock()
		stop, ownerDone := h.chunkStop, h.chunkDone
		p.mu.Unlock()
		if stop != nil {
			stop()
		}
		for _, done := range []<-chan struct{}{stopperDone, ownerDone} {
			if done != nil {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("cleanup did not join outstanding maintenance stopper/owner")
				}
			}
		}
	})
	for i := 0; i < 2; i++ {
		p.mu.Lock()
		p.startChunkLoop("v1", h, layer)
		p.mu.Unlock()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("same handle did not arm")
		}
		done := make(chan struct{})
		stopperDone = done
		go func() { p.stopChunkLoop(h); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("stopped owner did not join")
		}
	}
}

// observingLayer records what the maintenance loop hands it.
type observingLayer struct {
	maintenanceStubLayer
	seen chan error
}

func (l *observingLayer) ObserveRemoteError(err error) error {
	select {
	case l.seen <- err:
	default:
	}
	return err
}

// The holder's cleaning may be the first call to see HEAD move away: the
// maintenance loop hands its refusal to the layer, which latches the fence.
func TestMaintenanceRefusalReachesTheLayer(t *testing.T) {
	layer := &observingLayer{seen: make(chan error, 1)}
	p := &DurableProvider{cfg: ProviderConfig{ChunkMaintenanceInterval: time.Millisecond, ChunkMaintenance: func(context.Context, string) error {
		return fmt.Errorf("%w: cleaning refused", storage.ErrChunkLayerFenced)
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go p.chunkMaintenanceLoop(ctx, "v1", layer, func() bool { return true }, nil, nil)
	select {
	case err := <-layer.seen:
		if !errors.Is(err, storage.ErrChunkLayerFenced) {
			t.Fatalf("the layer saw %v; want the fence", err)
		}
	case <-ctx.Done():
		t.Fatal("the cleaning refusal never reached the layer")
	}
}
