package durable_test

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func hookedProvider(t *testing.T) (*durable.DurableProvider, func() []string) {
	t.Helper()
	remote := storage.NewMemChunkRemote()
	var mu sync.Mutex
	var calls []string
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:           durable.ImplWALStoreChunk,
		StorageRoot:    t.TempDir(),
		BlockSize:      4096,
		NumBlocks:      pcBlocks,
		ChunkBlocks:    pcChunkBlocks,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
		OnChunkDetach: func(id string) {
			mu.Lock()
			calls = append(calls, id)
			mu.Unlock()
		},
	}, v1View())
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	openRecovered(t, p)
	return p, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), calls...)
	}
}

// A detach (graceful stop) tells the host once, with the volume id; the
// lease-fence drain, a StopIO of its own, does not.
func TestWALStoreChunk_DetachCallsStopHook(t *testing.T) {
	ctx := context.Background()
	t.Run("detach", func(t *testing.T) {
		p, calls := hookedProvider(t)
		if _, _, err := p.DetachVolume(ctx, "v1"); err != nil {
			t.Fatal(err)
		}
		if got := calls(); len(got) != 1 || got[0] != "v1" {
			t.Fatalf("detach hook calls %q, want [v1]", got)
		}
	})
	t.Run("fence-stop", func(t *testing.T) {
		p, calls := hookedProvider(t)
		if err := p.Backend("v1").StopIO(ctx, "workspace lease fenced"); err != nil {
			t.Fatal(err)
		}
		if got := calls(); len(got) != 0 {
			t.Fatalf("lease-fence StopIO called the detach hook: %q", got)
		}
	})
}
