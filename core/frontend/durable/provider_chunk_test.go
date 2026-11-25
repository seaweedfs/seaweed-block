package durable_test

// walstore-chunk provider wiring, against the storage package's in-memory
// remote. The chunk layer's own contract (C1-C11) is tested in
// core/storage; these tests cover the provider: selection, attach after
// recovery, fresh host vs existing volume, the capture gate and lifetime.

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	pcBlocks      = 256
	pcChunkBlocks = 16
)

func chunkProvider(t *testing.T, root string, remote storage.ChunkRemote, view *stubView, interval time.Duration) *durable.DurableProvider {
	t.Helper()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	if gated, ok := remote.(*gatedRemote); ok {
		gated.fixture = fixture
		cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks, ChunkCaptureInterval: interval}
		fixture.Configure(&cfg)
		st, err := fixture.Catalog.Store(fixture.Ref)
		if err != nil {
			t.Fatal(err)
		}
		gated.StartStore = st
		cfg.ChunkLayoutHeadFor = func(string) (layout.StartStore, error) { return gated, nil }
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			inner, err := chunkstore.NewLayoutRemote(fixture.Catalog, fixture.Ref, me)
			if err != nil {
				return nil, err
			}
			gated.ChunkRemote = inner
			return gated, nil
		}
		p, err := durable.NewDurableProvider(cfg, view)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	_ = remote // plain legacy callers migrate to explicit shared fixtures.
	return layoutChunkProvider(t, root, fixture, view, interval)
}

func layoutChunkProvider(t *testing.T, root string, fixture *testutil.LayoutFixture, view *stubView, interval time.Duration) *durable.DurableProvider {
	t.Helper()
	cfg := durable.ProviderConfig{
		Impl:                 durable.ImplWALStoreChunk,
		StorageRoot:          root,
		BlockSize:            4096,
		NumBlocks:            pcBlocks,
		ChunkBlocks:          pcChunkBlocks,
		ChunkCaptureInterval: interval,
	}
	fixture.Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func openRecovered(t *testing.T, p *durable.DurableProvider) storage.LogicalStorage {
	t.Helper()
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatalf("EnsureStorage: %v", err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("RecoverVolume: %v", err)
	}
	return s
}

// freshV1Claim is only for fresh v1View fixtures, never reopened/local roots.
// INV-WS-CLAIM-BEFORE-SERVE-001: validate the open before using its baseline.
func freshV1Claim(t *testing.T, remote *storage.MemChunkRemote) storage.ChunkHead {
	t.Helper()
	head := headHolder(t, remote)
	assigned := v1View().Projection()
	if head.Gen != 1 || head.Epoch != assigned.Epoch || head.Holder != assigned.ReplicaID ||
		head.State != storage.ChunkHeadActive || head.RemoteLSN != 0 || head.DetachedBy != nil ||
		len(head.Chunks) != 0 || len(head.Segments) != 0 || head.ManifestChunkCount != 0 || head.ChunkBytes != 0 || remote.Puts() != 0 {
		t.Fatalf("fresh open did not establish exactly one empty assigned active claim: HEAD=%+v PUTs=%d", head, remote.Puts())
	}
	return head
}

func block(fill byte) []byte { return bytes.Repeat([]byte{fill}, 4096) }

func headBlock(t *testing.T, remote *storage.MemChunkRemote, lba uint32) []byte {
	t.Helper()
	head, err := remote.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := head.Chunks[uint64(lba/pcChunkBlocks)]
	if !ok {
		return make([]byte, 4096)
	}
	b, err := remote.Get(context.Background(), ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	off := int(lba%pcChunkBlocks) * 4096
	return b[off : off+4096]
}

func layoutHead(t *testing.T, fixture *testutil.LayoutFixture) layout.Head {
	t.Helper()
	h, found, err := fixture.Catalog.ReadHead(context.Background(), fixture.Ref)
	if err != nil || !found {
		t.Fatalf("layout HEAD: found=%v err=%v", found, err)
	}
	return h
}

func layoutHeadBlock(t *testing.T, fixture *testutil.LayoutFixture, lba uint32) []byte {
	t.Helper()
	h := layoutHead(t, fixture)
	if h.Current == nil {
		return make([]byte, 4096)
	}
	store, err := fixture.Catalog.Store(fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := layout.NewReader("/provider-test", fixture.Ref.Dir(), store.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	image, err := reader.Open(context.Background(), *h.Current)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := image.Chunk(context.Background(), uint64(lba/pcChunkBlocks))
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Zero {
		return make([]byte, 4096)
	}
	off := int(lba%pcChunkBlocks) * 4096
	return chunk.Data[off : off+4096]
}

func layoutHeadRefs(t *testing.T, fixture *testutil.LayoutFixture) map[uint64]layout.Ref {
	t.Helper()
	h := layoutHead(t, fixture)
	refs := make(map[uint64]layout.Ref)
	if h.Current == nil {
		return refs
	}
	store, err := fixture.Catalog.Store(fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := layout.NewReader("/provider-test", fixture.Ref.Dir(), store.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	image, err := reader.Open(context.Background(), *h.Current)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < pcBlocks/pcChunkBlocks; i++ {
		ref, zero, err := image.Resolve(context.Background(), i)
		if err != nil {
			t.Fatal(err)
		}
		if !zero {
			refs[i] = ref
		}
	}
	return refs
}

func v1View() *stubView {
	return newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}))
}

func TestWALStoreChunk_RequiresLayoutFactories(t *testing.T) {
	_, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks,
	}, v1View())
	if err == nil {
		t.Fatal("walstore-chunk without layout factories was accepted")
	}
}

// Capture publishes the volume; after a provider restart over the same
// local root, the volume is still authoritative and the next cut is
// incremental.
func TestWALStoreChunk_CaptureAndRestart(t *testing.T) {
	root := t.TempDir()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	view := v1View()
	p := layoutChunkProvider(t, root, fixture, view, 0)
	s := openRecovered(t, p)
	for lba := uint32(0); lba < 40; lba += 8 { // chunks 0, 0, 1, 1, 2
		if _, err := s.Write(lba, block(byte(lba+1))); err != nil {
			t.Fatal(err)
		}
	}
	h, err := p.CaptureVolume(context.Background(), "v1")
	if err != nil {
		t.Fatalf("CaptureVolume: %v", err)
	}
	head := layoutHead(t, fixture)
	if head.Position != h || head.Epoch != 3 || head.Holder != "r1" || head.Current == nil {
		t.Fatalf("HEAD after capture: position %d (want %d) epoch %d holder %q root %v", head.Position, h, head.Epoch, head.Holder, head.Current)
	}
	for lba := uint32(0); lba < 40; lba += 8 {
		if !bytes.Equal(layoutHeadBlock(t, fixture, lba), block(byte(lba+1))) {
			t.Fatalf("HEAD LBA %d differs", lba)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2 := layoutChunkProvider(t, root, fixture, view, 0)
	s2 := openRecovered(t, p2)
	if got, _ := s2.Read(16); !bytes.Equal(got, block(17)) {
		t.Fatal("restarted volume lost local data")
	}
	if _, err := s2.Write(100, block(0xAB)); err != nil { // chunk 6
		t.Fatal(err)
	}
	before := layoutHeadRefs(t, fixture)
	if _, err := p2.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatalf("CaptureVolume after restart: %v", err)
	}
	after := layoutHeadRefs(t, fixture)
	changed := 0
	for i := uint64(0); i < pcBlocks/pcChunkBlocks; i++ {
		if !reflect.DeepEqual(before[i], after[i]) {
			changed++
			if i != 6 {
				t.Fatalf("cut after restart changed chunk %d, want only 6", i)
			}
		}
	}
	if changed != 1 {
		t.Fatalf("cut after restart changed %d chunk refs, want 1", changed)
	}
	if !bytes.Equal(layoutHeadBlock(t, fixture, 100), block(0xAB)) {
		t.Fatal("HEAD lacks the post-restart write")
	}
}

// A provider that creates the volume (a new host) starts from the remote
// HEAD and reads lazily.
func TestWALStoreChunk_FreshHostReadsFromHead(t *testing.T) {
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	view := v1View()
	p := layoutChunkProvider(t, t.TempDir(), fixture, view, 0)
	s := openRecovered(t, p)
	if _, err := s.Write(33, block(0x33)); err != nil {
		t.Fatal(err)
	}
	if err := p.MakeVolumeRemoteDurable(context.Background(), "v1", 1); err != nil {
		t.Fatalf("MakeVolumeRemoteDurable: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	next := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2}))
	fresh := layoutChunkProvider(t, t.TempDir(), fixture, next, 0) // empty local root at a new grant
	s2 := openRecovered(t, fresh)
	if got, err := s2.Read(33); err != nil || !bytes.Equal(got, block(0x33)) {
		t.Fatalf("fresh host read: %v", err)
	}
}

func TestWALStoreChunk_CaptureNeedsHealthyProjection(t *testing.T) {
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	view := v1View()
	p := layoutChunkProvider(t, t.TempDir(), fixture, view, 0)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	proj := view.Projection()
	proj.Healthy = false
	view.set(proj)
	if _, err := p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, storage.ErrChunkCaptureRefused) {
		t.Fatalf("capture with an unhealthy projection: got %v, want refused", err)
	}
	if fixture.Blobs.Puts() != 0 {
		t.Fatal("a refused capture stored chunks")
	}
}

func TestWALStoreChunk_PeriodicCut(t *testing.T) {
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	p := layoutChunkProvider(t, t.TempDir(), fixture, v1View(), 20*time.Millisecond)
	s := openRecovered(t, p)
	lsn, err := s.Write(50, block(0x50))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		head := layoutHead(t, fixture)
		if head.Position >= lsn {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("periodic cut never published LSN %d (HEAD position %d)", lsn, head.Position)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !bytes.Equal(layoutHeadBlock(t, fixture, 50), block(0x50)) {
		t.Fatal("periodic cut published the wrong bytes")
	}
}

func TestWALStoreChunk_SelectorRules(t *testing.T) {
	view := v1View()
	// A non-chunk volume has no chunk operations.
	plain, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), NumBlocks: pcBlocks}, view)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	openRecovered(t, plain)
	if _, err := plain.CaptureVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrNotChunkVolume) {
		t.Fatalf("CaptureVolume on a plain walstore volume: got %v", err)
	}
	// A smartwal file is not a chunk volume.
	root := t.TempDir()
	sw, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplSmartWAL, StorageRoot: root, NumBlocks: pcBlocks}, view)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sw.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	_ = sw.Close()
	p := layoutChunkProvider(t, root, testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks), view, 0)
	if _, err := p.EnsureStorage("v1"); !errors.Is(err, durable.ErrImplKindMismatch) {
		t.Fatalf("walstore-chunk over a smartwal file: got %v, want ErrImplKindMismatch", err)
	}
}
