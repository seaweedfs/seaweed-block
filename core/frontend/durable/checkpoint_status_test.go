package durable_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestDurableProvider_CheckpointPersistenceStatus(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: 64, ChunkBlocks: 4, ChunkCaptureInterval: 0,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	be, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := be.Write(context.Background(), 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := be.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		statuses := p.DurableStatuses()
		raw, err := json.Marshal(statuses)
		if err != nil {
			t.Fatal(err)
		}
		var wire []struct {
			CheckpointPersistence *struct {
				Successes uint64
				Refusals  uint64
				Failures  uint64
			}
		}
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		if len(wire) == 1 && wire[0].CheckpointPersistence != nil && wire[0].CheckpointPersistence.Successes > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no checkpoint persist status in durable response: %s", raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
