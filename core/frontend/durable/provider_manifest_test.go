package durable_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type manifestAwareRemote struct {
	*storage.MemChunkRemote
	prepared atomic.Int32
}

func (r *manifestAwareRemote) PrepareManifest(ctx context.Context, base, next storage.ChunkHead, chunkBytes uint64) (storage.ChunkHead, error) {
	r.prepared.Add(1)
	return next, ctx.Err()
}
func TestWALStoreChunk_ProviderPreservesManifestCapability(t *testing.T) {
	r := &manifestAwareRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	p := chunkProvider(t, t.TempDir(), r, v1View(), 0)
	s := openRecovered(t, p)
	if _, err := s.Write(0, block(0x65)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	if r.prepared.Load() != 1 {
		t.Fatalf("provider erased remote manifest capability: preparations=%d", r.prepared.Load())
	}
}
