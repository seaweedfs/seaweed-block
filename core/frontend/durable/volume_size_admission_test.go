package durable_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestProviderCreateCapacityPrecedesLayoutAccess(t *testing.T) {
	root := t.TempDir()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: ^uint32(0), BlockSize: 8192,
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) {
			t.Fatal("capacity check must precede layout access")
			return nil, nil
		},
		ChunkLayoutRemoteFor: func(string, layout.HolderID) (storage.ChunkRemote, error) {
			t.Fatal("capacity check must precede layout access")
			return nil, nil
		},
	}, newStubView(healthyProj(id)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	_, err = p.Open(context.Background(), "v1")
	if err == nil || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
		t.Fatalf("want capacity before layout access, got %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("refused create wrote files: %v", entries)
	}
}
