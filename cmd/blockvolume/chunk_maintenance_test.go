package main

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"path"
	"testing"
)

func TestMaintenanceRetentionFailureNeverStartsGC(t *testing.T) {
	f := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewCatalog(f, defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	pass, closeFn, err := newChunkMaintenance(cat, nil, true, "unused-master:1234")
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	if err = pass(context.Background(), "missing-volume"); err == nil {
		t.Fatal("missing retention volume accepted")
	}
	if _, err = f.LookupEntry(context.Background(), path.Join(defaultChunkPath, "missing-volume"), "_GC_MARK"); !errors.Is(err, chunkstore.ErrNotFound) {
		t.Fatalf("failed retention entered GC: %v", err)
	}
}
func TestMaintenanceDefaultRetainNeedsNoEpochClient(t *testing.T) {
	f := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewCatalog(f, defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cat.EnsureVolume(context.Background(), "v", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	pass, closeFn, err := newChunkMaintenance(cat, nil, false, "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	if err = pass(context.Background(), "v"); err != nil {
		t.Fatal(err)
	}
}
