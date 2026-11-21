package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type manifestFixtureFiler struct {
	chunkstore.Filer
	mu    sync.Mutex
	blobs map[string][]byte
}

func newManifestFixtureFiler(f chunkstore.Filer) *manifestFixtureFiler {
	return &manifestFixtureFiler{Filer: f, blobs: map[string][]byte{}}
}

func (f *manifestFixtureFiler) Put(_ context.Context, b []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := fmt.Sprintf("fixture-%d", len(f.blobs))
	f.blobs[id] = append([]byte(nil), b...)
	return id, nil
}

func (f *manifestFixtureFiler) Get(_ context.Context, id string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.blobs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %w", chunkstore.ErrBlobNotFound, chunkstore.ErrNotFound)
	}
	return append([]byte(nil), b...), nil
}

func prepareFixtureHead(t *testing.T, f *manifestFixtureFiler, h storage.ChunkHead) storage.ChunkHead {
	t.Helper()
	ctx := context.Background()
	r, err := chunkstore.NewChunkRemote(f, f, defaultChunkPathCLI+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	h, intents, err := r.PrepareManifest(ctx, storage.ChunkHead{}, h, 8*4096)
	if err != nil {
		t.Fatal(err)
	}
	// These fixtures hand the completed cut directly to the catalog; the
	// snapshot/fork operations establish their own reachability intents.
	for _, id := range intents {
		if err := r.DeleteIntent(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	return h
}
