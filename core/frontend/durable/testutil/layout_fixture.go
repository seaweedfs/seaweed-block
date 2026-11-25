// Package testutil supplies in-memory B-layout fixtures for provider tests.
package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type MemoryBlobs struct {
	mu   sync.Mutex
	next uint64
	data map[string][]byte
	// PutHook can hold or refuse a payload upload in a test. Set it before
	// starting concurrent provider work.
	PutHook func(context.Context) error
	// GetHook observes or changes a fetched payload; nil uses the stored bytes.
	GetHook func(context.Context, string, []byte) ([]byte, error)
}

func (b *MemoryBlobs) Put(ctx context.Context, data []byte) (string, error) {
	if b.PutHook != nil {
		if err := b.PutHook(ctx); err != nil {
			return "", err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	id := fmt.Sprintf("test-blob-%d", b.next)
	b.data[id] = append([]byte(nil), data...)
	return id, nil
}

func (b *MemoryBlobs) Get(ctx context.Context, id string) ([]byte, error) {
	b.mu.Lock()
	v, ok := b.data[id]
	copyV := append([]byte(nil), v...)
	b.mu.Unlock()
	if !ok {
		return nil, chunkstore.ErrBlobNotFound
	}
	if b.GetHook != nil {
		return b.GetHook(ctx, id, copyV)
	}
	return copyV, nil
}

// LayoutFixture creates one B VOLUME and HEAD, retaining them across provider
// restarts. Tests can inspect Catalog and Ref instead of a legacy ChunkHead.
type LayoutFixture struct {
	Catalog *chunkstore.BCatalog
	Ref     chunkstore.VolumeRef
	Blobs   *MemoryBlobs
}

func NewLayoutFixture(t testing.TB, name string, blocks, chunkBlocks uint64) *LayoutFixture {
	t.Helper()
	if blocks == 0 || chunkBlocks == 0 {
		t.Fatal("layout fixture requires nonzero geometry")
	}
	blobs := &MemoryBlobs{data: make(map[string][]byte)}
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), blobs, "/provider-test")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: name, ID: id}
	if _, err := cat.CreateVolume(context.Background(), ref, layout.Geometry{SizeBytes: blocks * 4096, BlockSize: 4096, ChunkBytes: chunkBlocks * 4096}); err != nil {
		t.Fatal(err)
	}
	return &LayoutFixture{Catalog: cat, Ref: ref, Blobs: blobs}
}

func (f *LayoutFixture) Configure(cfg *durable.ProviderConfig) {
	cfg.ChunkLayoutHeadFor = func(string) (layout.StartStore, error) { return f.Catalog.Store(f.Ref) }
	cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
		return chunkstore.NewLayoutRemote(f.Catalog, f.Ref, me)
	}
}
