package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Two servers serving the same replica id at the same epoch are different
// holders (<server-id>/<replica-id>). The second one's RecoverVolume fails
// fenced before anything can serve, and HEAD is unchanged (integration
// ruling F2: the earlier surface is canonical).
func TestWALStoreChunk_SameReplicaOnAnotherServerIsFenced(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	newHost := func(holder string) *durable.DurableProvider {
		cfg := durable.ProviderConfig{
			Impl:        durable.ImplWALStoreChunk,
			StorageRoot: t.TempDir(),
			BlockSize:   4096,
			NumBlocks:   pcBlocks,
			ChunkBlocks: pcChunkBlocks,
			ChunkHolder: holder,
		}
		fixture.Configure(&cfg)
		p, err := durable.NewDurableProvider(cfg, v1View())
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
	head, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
	if err != nil || !found {
		t.Fatalf("layout HEAD: found=%v err=%v", found, err)
	}

	b := newHost("s2/r1")
	if _, err := b.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerFenced) {
		t.Fatalf("a second server with the same replica id at the same epoch: RecoverVolume %v; want fenced", err)
	}
	if after, _, _ := fixture.Catalog.ReadHead(ctx, fixture.Ref); after.Gen != head.Gen || after.Holder != "s1/r1" {
		t.Fatalf("HEAD after the second server's recovery: gen %d holder %q, want gen %d holder s1/r1", after.Gen, after.Holder, head.Gen)
	}
	be, err := b.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := be.Write(ctx, 4096, block(2)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("a write on the fenced second server: %v; want ErrNotReady", err)
	}
	// The canonical refusal is at Claim, before any layer is installed.
	// Later cuts must still refuse; they need not repeat a layer's fence.
	if _, err := b.CaptureVolume(ctx, "v1"); err == nil {
		t.Fatal("a cut succeeded after refused recovery")
	}
	if after, _, _ := fixture.Catalog.ReadHead(ctx, fixture.Ref); after.Gen != head.Gen || after.Holder != head.Holder {
		t.Fatalf("refused cut changed HEAD: %+v", after)
	}
}
