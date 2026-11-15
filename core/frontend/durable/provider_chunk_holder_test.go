package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Two servers serving the same replica id at the same epoch are different
// holders (<server-id>/<replica-id>): the second one is fenced and publishes
// nothing over the first one's HEAD.
func TestWALStoreChunk_SameReplicaOnAnotherServerIsFenced(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	newHost := func(holder string) *durable.DurableProvider {
		p, err := durable.NewDurableProvider(durable.ProviderConfig{
			Impl:           durable.ImplWALStoreChunk,
			StorageRoot:    t.TempDir(),
			BlockSize:      4096,
			NumBlocks:      pcBlocks,
			ChunkBlocks:    pcChunkBlocks,
			ChunkHolder:    holder,
			ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
		}, v1View())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	a := newHost("s1/r1")
	sa := openRecovered(t, a)
	if _, err := sa.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	head, _ := remote.GetHead(ctx)

	b := newHost("s2/r1")
	sb := openRecovered(t, b)
	if _, err := sb.Write(2, block(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CaptureVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerFenced) {
		t.Fatalf("a second server with the same replica id at the same epoch: %v; want fenced", err)
	}
	if after, _ := remote.GetHead(ctx); after.Gen != head.Gen || after.Holder != "s1/r1" {
		t.Fatalf("HEAD after the second server's cut: gen %d holder %q, want gen %d holder s1/r1", after.Gen, after.Holder, head.Gen)
	}
}
