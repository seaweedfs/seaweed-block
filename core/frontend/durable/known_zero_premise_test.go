package durable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type kzLayoutBlobs struct {
	sync.Mutex
	b    map[string][]byte
	gets atomic.Int64
}

func (b *kzLayoutBlobs) Put(_ context.Context, data []byte) (string, error) {
	b.Lock()
	defer b.Unlock()
	id := fmt.Sprint(len(b.b) + 1)
	b.b[id] = append([]byte(nil), data...)
	return id, nil
}
func (b *kzLayoutBlobs) Get(_ context.Context, id string) ([]byte, error) {
	b.gets.Add(1)
	b.Lock()
	defer b.Unlock()
	v, ok := b.b[id]
	if !ok {
		return nil, chunkstore.ErrBlobNotFound
	}
	return append([]byte(nil), v...), nil
}

type kzLayout struct {
	cat   *chunkstore.BCatalog
	ref   chunkstore.VolumeRef
	blobs *kzLayoutBlobs
}

func newKZLayout(t *testing.T) *kzLayout {
	t.Helper()
	b := &kzLayoutBlobs{b: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), b, "/kz")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(context.Background(), ref, layout.Geometry{SizeBytes: 256 * 4096, BlockSize: 4096, ChunkBytes: 16 * 4096}); err != nil {
		t.Fatal(err)
	}
	return &kzLayout{cat: cat, ref: ref, blobs: b}
}

// kzProvider is a B-layout chunk provider for v1 (16 chunks).
func kzProvider(t *testing.T, root string, remote *kzLayout) *DurableProvider {
	t.Helper()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}
	p, err := NewDurableProvider(ProviderConfig{
		Impl: ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096, NumBlocks: 256, ChunkBlocks: 16,
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return remote.cat.Store(remote.ref) },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			return chunkstore.NewLayoutRemote(remote.cat, remote.ref, me)
		},
	}, precopyQueueView{id})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func kzCandidates(t *testing.T, p *DurableProvider) int {
	t.Helper()
	for _, st := range p.DurableStatuses() {
		if st.VolumeID == "v1" && st.Chunk != nil && st.Chunk.Attached {
			return st.Chunk.Stats.DirtyChunks
		}
	}
	t.Fatal("v1 has no attached chunk layer")
	return -1
}

func kzFreshAtRoot(t *testing.T, path string) bool {
	t.Helper()
	ws, err := storage.OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_, fresh, ok := ws.LayoutIdentity()
	if !ok {
		t.Fatal("layout identity missing from durable root")
	}
	return fresh
}

// The premise of RULE-FRESH-KNOWN-ZERO: until the attach commit retires the
// fresh marker, no write reaches the store. Held at that boundary (after the
// claim and the certification, before the commit), a frontend write is not
// admitted (it may wait; here it is bounded) and appends no WAL record. After
// the commit the store has no candidates: nothing was written.
func TestNoWriteReachesAFreshStoreBeforeItsAttachCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := kzProvider(t, t.TempDir(), newKZLayout(t))
	st, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ws := st.(*storage.WALStore)
	var writeErr error
	var nextAtBoundary uint64
	held := false
	p.testBeforeChunkFreshRetire = func() error {
		held = true
		wctx, wcancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer wcancel()
		_, writeErr = p.Backend("v1").Write(wctx, 0, bytes.Repeat([]byte{0x5a}, 4096))
		nextAtBoundary = ws.NextLSN()
		return nil
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	p.testBeforeChunkFreshRetire = nil
	if !held {
		t.Fatal("the attach did not reach its commit boundary")
	}
	if writeErr == nil || nextAtBoundary != 1 {
		t.Fatalf("at the commit boundary a frontend write returned %v and the WAL's next LSN was %d; want refused, 1", writeErr, nextAtBoundary)
	}
	if got := kzCandidates(t, p); got != 0 {
		t.Fatalf("after the commit: %d candidate chunks; want 0 (nothing was written)", got)
	}
	for _, st := range p.DurableStatuses() {
		if st.VolumeID == "v1" && st.Chunk != nil && st.Chunk.Stats.KnownZeroChunks != 16 {
			t.Fatalf("status KnownZeroChunks %d; want 16", st.Chunk.Stats.KnownZeroChunks)
		}
	}
}

// RULE-FRESH-KNOWN-ZERO needs THIS start's create: a first start that
// crashes before its attach commit leaves the fresh marker, but the next
// start did not create the store, so it certifies nothing (one scan: every
// chunk a candidate), and its next cut still publishes a write.
func TestFreshKnownZeroNotAfterARestartBeforeTheAttachCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, remote := t.TempDir(), newKZLayout(t)
	p := kzProvider(t, root, remote)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash before the attach commit")
	p.testBeforeChunkFreshRetire = func() error { return crash }
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, crash) {
		t.Fatalf("first start: %v; want the injected crash", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !kzFreshAtRoot(t, p.volumePath("v1")) {
		t.Fatal("durable layout identity was not fresh after the crash")
	}
	p2 := kzProvider(t, root, remote)
	if _, err := p2.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if got := kzCandidates(t, p2); got != 16 {
		t.Fatalf("second start: %d candidate chunks; want all 16 (it did not create the store)", got)
	}
	kzWriteAndPublish(t, ctx, p2, remote)
}

// The same after a restart between the create and any attach (test #13).
func TestFreshKnownZeroNotAfterARestartBeforeAnyAttach(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, remote := t.TempDir(), newKZLayout(t)
	p := kzProvider(t, root, remote)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := kzProvider(t, root, remote)
	if _, err := p2.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if got := kzCandidates(t, p2); got != 16 {
		t.Fatalf("restart before any attach: %d candidate chunks; want all 16", got)
	}
}

// The order: the attach commit retires the marker, THEN the marking is made
// durable, THEN the volume serves. A crash between the first two leaves no
// marker and every chunk UNKNOWN: the next start scans every chunk once, and
// is correct. (Marking before the retirement would leave certified chunks
// behind a marker a later start could trust.)
func TestFreshKnownZeroCrashBetweenMarkerRetireAndMarking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, remote := t.TempDir(), newKZLayout(t)
	p := kzProvider(t, root, remote)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash after the marker retirement, before the marking")
	p.testAfterChunkFreshRetire = func() error { return crash }
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, crash) {
		t.Fatalf("first start: %v; want the injected crash", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if kzFreshAtRoot(t, p.volumePath("v1")) {
		t.Fatal("durable layout identity remained fresh after its clear")
	}
	p2 := kzProvider(t, root, remote)
	if _, err := p2.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if got := kzCandidates(t, p2); got != 16 {
		t.Fatalf("restart after a crash between marker retirement and marking: %d candidate chunks; want all 16 (one scan)", got)
	}
	kzWriteAndPublish(t, ctx, p2, remote)
}

// kzWriteAndPublish writes chunk 5, cuts, and checks HEAD publishes it.
func kzWriteAndPublish(t *testing.T, ctx context.Context, p *DurableProvider, remote *kzLayout) {
	t.Helper()
	want := bytes.Repeat([]byte{0x6b}, 4096)
	if _, err := p.Backend("v1").Write(ctx, 5*16*4096, want); err != nil {
		t.Fatal(err)
	}
	if err := p.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	head, found, err := remote.cat.ReadHead(ctx, remote.ref)
	if err != nil || !found || head.Current == nil {
		t.Fatalf("the cut did not publish a B root: found=%v err=%v head=%+v", found, err, head)
	}
	st, err := remote.cat.Store(remote.ref)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := layout.NewReader("/kz", remote.ref.Dir(), st.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	image, err := reader.Open(ctx, *head.Current)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := image.Chunk(ctx, 5)
	if err != nil || chunk.Zero || !bytes.Equal(chunk.Data[:4096], want) {
		t.Fatalf("published chunk 5: %v", err)
	}
	if got := kzCandidates(t, p); got != 0 {
		t.Fatalf("after the cut: %d candidate chunks; want 0", got)
	}
}

// The premise again, for fills (test #12): until the attach commit retires
// the fresh marker, no fetch and no extent write reaches the store. A restore
// target whose HEAD names two chunks is held at the commit boundary; a
// frontend read of a HEAD-named block there is not served (it may wait;
// here it is bounded), the remote serves no chunk read, and after the
// commit both named chunks are still absent with no fetch started: no fill
// wrote the extent. After that the read returns HEAD's data.
func TestNoFetchOrFillReachesAFreshStoreBeforeItsAttachCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	remote := newKZLayout(t)
	src := kzProvider(t, t.TempDir(), remote)
	if _, err := src.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{0x3c}, 4096)
	for _, chunk := range []int64{3, 7} {
		if _, err := src.Backend("v1").Write(ctx, chunk*16*4096, want); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := src.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}

	p := kzProvider(t, t.TempDir(), remote)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	var readErr error
	var getsAtBoundary int64 = -1
	p.testBeforeChunkFreshRetire = func() error {
		before := remote.blobs.gets.Load()
		rctx, rcancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer rcancel()
		_, readErr = p.Backend("v1").Read(rctx, 3*16*4096, make([]byte, 4096))
		getsAtBoundary = remote.blobs.gets.Load() - before
		return nil
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	p.testBeforeChunkFreshRetire = nil
	if readErr == nil || getsAtBoundary != 0 {
		t.Fatalf("at the commit boundary a frontend read of a HEAD-named block returned %v and the remote served %d chunk reads; want refused, 0", readErr, getsAtBoundary)
	}
	for _, st := range p.DurableStatuses() {
		if st.VolumeID == "v1" && st.Chunk != nil {
			if h := st.Chunk.Stats.Hydration; st.Chunk.Stats.AbsentChunks != 2 || h.Fetches != 0 || h.Completed != 0 {
				t.Fatalf("after the commit: %d absent chunks, %d fetches, %d completed; want 2 absent and no fetch (no fill before the commit)", st.Chunk.Stats.AbsentChunks, h.Fetches, h.Completed)
			}
		}
	}
	got := make([]byte, 4096)
	if n, err := p.Backend("v1").Read(ctx, 3*16*4096, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
		t.Fatalf("read after the commit: %d %v, data equal %v; want HEAD's data", n, err, bytes.Equal(got, want))
	}
}
