package authority

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
)

type regrantStorageView struct{}

func (regrantStorageView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}
}

// These component controls obtain the real authority re-grant before opening
// the returning file-backed provider. They do not model a second live process.
func TestWorkspaceLeaseRegrantPreservesLocalLogThroughFirstCut(t *testing.T) {
	testWorkspaceRegrantStorage(t)
}

// The B-layout equivalents of the removed legacy-only cases are
// TestLayoutPlainDetachLostReplyRestartReplaysProof and
// TestLayoutProviderRefusesEmptyOwnEpochAndRemountedOldRoot.
func testWorkspaceRegrantStorage(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	disk, _ := newTempStore(t)
	clock := newTestClock()
	p := newLeasePublisher(t, disk, time.Second, clock)
	bindAndGrant(t, p, "v", "r")
	fixture := testutil.NewLayoutFixture(t, "v", 64, 4)
	root := t.TempDir()
	open := func() *durable.DurableProvider {
		t.Helper()
		cfg := durable.ProviderConfig{
			Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64,
			BlockSize: 4096, ChunkBlocks: 4,
		}
		fixture.Configure(&cfg)
		provider, err := durable.NewDurableProvider(cfg, regrantStorageView{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = provider.Close() })
		if _, err := provider.EnsureStorage("v"); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	first := open()
	if _, err := first.RecoverVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	backend, err := first.Open(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	a, b := bytes.Repeat([]byte{0x31}, 4096), bytes.Repeat([]byte{0x42}, 4096)
	write := func(offset int64, data []byte) {
		t.Helper()
		if n, err := backend.Write(ctx, offset, data); err != nil || n != len(data) {
			t.Fatal(n, err)
		}
		if err := backend.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	write(0, a)
	if _, err := first.CaptureVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	write(4096, b) // completed Sync, deliberately absent from the last HEAD
	before, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
	if err != nil || !found {
		t.Fatalf("HEAD before regrant: found=%v err=%v", found, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	clock.at(3) // beyond both original expiry/skew endpoints
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal("returning process could not obtain the authority re-grant", err)
	}
	second := open()
	_, err = second.RecoverVolume(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	returning, err := second.Open(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range [][]byte{a, b} {
		got := make([]byte, 4096)
		if _, err := returning.Read(ctx, int64(i*4096), got); err != nil || !bytes.Equal(got, want) {
			t.Fatal("return lost locally acknowledged bytes", i, err)
		}
	}
	claimed, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
	if err != nil || !found || claimed.Gen != before.Gen || claimed.Epoch != 1 || claimed.Holder != before.Holder || claimed.ClaimID != before.ClaimID || claimed.Position != before.Position {
		t.Fatal("same-root return changed the B HEAD before the first cut", claimed, err)
	}
	if _, err := second.CaptureVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	final, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
	if err != nil || !found || final.Position <= before.Position {
		t.Fatal("first returning cut did not cover the local log", final, err)
	}
	st, err := fixture.Catalog.Store(fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := layout.NewReader("/provider-test", fixture.Ref.Dir(), st.Fetch)
	if err != nil || final.Current == nil {
		t.Fatal("first cut has no B image", err)
	}
	image, err := reader.Open(ctx, *final.Current)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := image.Chunk(ctx, 0)
	if err != nil || chunk.Zero || len(chunk.Data) < 8192 || !bytes.Equal(chunk.Data[:4096], a) || !bytes.Equal(chunk.Data[4096:8192], b) {
		t.Fatal("first cut did not publish the full acknowledged image", err)
	}
}
