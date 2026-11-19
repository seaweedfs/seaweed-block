package durable_test

import (
	"context"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProviderCreateCapacityPrecedesFreshMarker(t *testing.T) {
	root := t.TempDir()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: ^uint32(0), BlockSize: 8192, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil }}, newStubView(healthyProj(id)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	// Invalid parent guarantees no giant allocation even when admission is removed.
	parent := filepath.Join(root, "v1.bin.chunkfresh")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	_, err = p.Open(context.Background(), "v1")
	if err == nil || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
		t.Fatalf("want capacity before marker, got %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "v1.bin.chunkfresh" {
		t.Fatalf("refused create wrote files: %v", entries)
	}
}
