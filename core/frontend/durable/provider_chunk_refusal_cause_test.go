package durable_test

// A refused cut keeps storage.ErrChunkCaptureRefused and adds why (B9 W2
// §2.2): not a healthy primary here (go elsewhere) or a moved lineage
// (attach anew).

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestWALStoreChunk_CaptureRefusalCarriesCause(t *testing.T) {
	cases := map[string]struct {
		drift func(p *frontend.Projection)
		want  error
	}{
		"unhealthy": {func(p *frontend.Projection) { p.Healthy = false }, durable.ErrChunkNotPrimary},
		"epoch":     {func(p *frontend.Projection) { p.Epoch++ }, durable.ErrChunkEpochStale},
		"replica":   {func(p *frontend.Projection) { p.ReplicaID = "r2" }, durable.ErrChunkEpochStale},
		"endpoint":  {func(p *frontend.Projection) { p.EndpointVersion++ }, durable.ErrChunkEpochStale},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			view := v1View()
			p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), view, 0)
			s := openRecovered(t, p)
			if _, err := s.Write(1, block(1)); err != nil {
				t.Fatal(err)
			}
			proj := view.Projection()
			c.drift(&proj)
			view.set(proj)
			_, err := p.CaptureVolume(context.Background(), "v1")
			if !errors.Is(err, storage.ErrChunkCaptureRefused) || !errors.Is(err, c.want) {
				t.Fatalf("cut after %s: %v; want refused with %v", name, err, c.want)
			}
		})
	}
}
