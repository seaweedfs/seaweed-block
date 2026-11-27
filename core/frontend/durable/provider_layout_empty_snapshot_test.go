package durable_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The first snapshot of a volume never written names its empty image with
// a commit of its own, which gives HEAD its first root. A later HEAD write
// by someone else (here the store-direct snapshot delete) must not make the
// holder's next cut lose its commit: the next snapshot, with data, is taken
// on its first attempt, and its data is in the image it names.
func TestLayoutProviderSnapshotAfterAnEmptySnapshotAndAnOutsideWrite(t *testing.T) {
	ctx := context.Background()
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), &layoutProviderBlobs{blobs: map[string][]byte{}}, "/layout-test")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	v := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err = cat.CreateVolume(ctx, v, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1",
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return cat.Store(v) },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			return chunkstore.NewLayoutRemote(cat, v, me)
		},
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ws := s.(*storage.WALStore)
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	head := func() layout.Head {
		h, _, err := cat.ReadHead(ctx, v)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	if h := head(); h.Current != nil {
		t.Fatalf("setup: a never-written volume's HEAD names an image: %+v", h)
	}
	if _, err := p.SnapshotVolume(ctx, "v1", "empty", layout.SnapshotMeta{CreatedNs: 1}); err != nil {
		t.Fatalf("snapshot of the empty volume: %v", err)
	}
	if h := head(); h.Current == nil {
		t.Fatalf("setup: the empty snapshot's commit gave HEAD no root: %+v", h)
	}
	if _, err := cat.DeleteSnapshot(ctx, v, "empty"); err != nil { // a HEAD write by someone else
		t.Fatalf("store-direct snapshot delete: %v", err)
	}
	data := bytes.Repeat([]byte("after-the-empty!"), 4096/16)
	if _, err = ws.Write(1, data); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Sync(); err != nil {
		t.Fatal(err)
	}
	hd, err := p.SnapshotVolume(ctx, "v1", "with-data", layout.SnapshotMeta{CreatedNs: 2})
	if err != nil {
		t.Fatalf("snapshot with data after the empty snapshot and an outside HEAD write: %v; want it taken on the first attempt", err)
	}
	sn, ok := hd.Snapshots["with-data"]
	if !ok || hd.Current == nil || sn.Root != *hd.Current || sn.Position == 0 {
		t.Fatalf("snapshot with-data: %+v (HEAD current %v); want it naming the image with the write", sn, hd.Current)
	}
	// The snapshot's own root, read from the store (root, segment, chunk;
	// not the holder's cache), holds the write at block 1 and zeros at block
	// 2, the unwritten block next to it in the same chunk.
	st, err := cat.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	r, err := layout.NewReader("/layout-test", v.Dir(), st.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	img, err := r.Open(ctx, sn.Root)
	if err != nil {
		t.Fatal(err)
	}
	c, err := img.Chunk(ctx, 0) // blocks 0-3: ChunkBlocks 4
	if err != nil || c.Zero || len(c.Data) != 4*4096 {
		t.Fatalf("chunk 0 of snapshot with-data: zero %t, %d bytes, %v", c.Zero, len(c.Data), err)
	}
	if !bytes.Equal(c.Data[4096:8192], data) {
		t.Fatalf("block 1 in snapshot with-data's stored image is not the write")
	}
	if !bytes.Equal(c.Data[8192:12288], make([]byte, 4096)) {
		t.Fatalf("block 2 in snapshot with-data's stored image is not zero")
	}
}
