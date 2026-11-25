package durable_test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// layoutCase keeps the store and its volume identity across provider restarts.
type layoutCase struct {
	cat   *chunkstore.BCatalog
	ref   chunkstore.VolumeRef
	blobs *layoutProviderBlobs
}

func newLayoutCase(t *testing.T, blocks, chunkBlocks uint64) *layoutCase {
	t.Helper()
	blobs := &layoutProviderBlobs{blobs: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), blobs, "/layout-fixture")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(context.Background(), ref, layout.Geometry{SizeBytes: blocks * 4096, BlockSize: 4096, ChunkBytes: chunkBlocks * 4096}); err != nil {
		t.Fatal(err)
	}
	return &layoutCase{cat: cat, ref: ref, blobs: blobs}
}

func (c *layoutCase) provider(t *testing.T, root string, view *stubView, holder string, configure func(*durable.ProviderConfig)) *durable.DurableProvider {
	t.Helper()
	vol, err := c.cat.ReadVolume(context.Background(), c.ref)
	if err != nil {
		t.Fatal(err)
	}
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: uint32(vol.Geometry.SizeBytes / 4096), BlockSize: 4096, ChunkBlocks: uint32(vol.Geometry.ChunkBytes / 4096), ChunkHolder: holder,
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return c.cat.Store(c.ref) },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			return chunkstore.NewLayoutRemote(c.cat, c.ref, me)
		},
	}
	if configure != nil {
		configure(&cfg)
	}
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func (c *layoutCase) head(t *testing.T) layout.Head {
	t.Helper()
	h, found, err := c.cat.ReadHead(context.Background(), c.ref)
	if err != nil || !found {
		t.Fatalf("layout HEAD: found=%v err=%v", found, err)
	}
	return h
}

func (c *layoutCase) block(t *testing.T, lba uint32) []byte {
	t.Helper()
	h := c.head(t)
	if h.Current == nil {
		return make([]byte, 4096)
	}
	st, err := c.cat.Store(c.ref)
	if err != nil {
		t.Fatal(err)
	}
	r, err := layout.NewReader("/layout-fixture", c.ref.Dir(), st.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	img, err := r.Open(context.Background(), *h.Current)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := img.Chunk(context.Background(), uint64(lba)/(hGeometryChunkBlocks(c, t)))
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Zero {
		return make([]byte, 4096)
	}
	off := int(uint64(lba)%hGeometryChunkBlocks(c, t)) * 4096
	return chunk.Data[off : off+4096]
}

func hGeometryChunkBlocks(c *layoutCase, t *testing.T) uint64 {
	t.Helper()
	v, err := c.cat.ReadVolume(context.Background(), c.ref)
	if err != nil {
		t.Fatal(err)
	}
	return v.Geometry.ChunkBytes / 4096
}
