package durable

import (
	"context"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
	"time"
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
			p.chunkMaintenanceLoop(ctx, "v1", layer, func() bool { return p.maintenanceCurrent("v1", h, layer, lin) })
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
	t.Cleanup(func() { p.stopChunkLoop(h) })
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
		go func() { p.stopChunkLoop(h); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("stopped owner did not join")
		}
	}
}
