package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
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
