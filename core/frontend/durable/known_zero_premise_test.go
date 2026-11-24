package durable

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// kzProvider is a walstore-chunk provider for v1 (256 blocks, 16 per
// chunk: 16 chunks) over root and remote.
func kzProvider(t *testing.T, root string, remote storage.ChunkRemote) *DurableProvider {
	t.Helper()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}
	p, err := NewDurableProvider(ProviderConfig{
		Impl: ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096, NumBlocks: 256, ChunkBlocks: 16,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
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

// The premise of RULE-FRESH-KNOWN-ZERO: until the attach commit retires the
// fresh marker, no write reaches the store. Held at that boundary (after the
// claim and the certification, before the commit), a frontend write is not
// admitted (it may wait; here it is bounded) and appends no WAL record. After
// the commit the store has no candidates: nothing was written.
func TestNoWriteReachesAFreshStoreBeforeItsAttachCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := kzProvider(t, t.TempDir(), storage.NewMemChunkRemote())
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
}

// A first start that crashes before its attach commit (the commit fails, the
// process ends) leaves the fresh marker: the next start decides again on its
// own claim. It still has no candidates, and a write after it is published
// by the next cut.
func TestFreshAttachCrashBeforeCommitDecidesAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, remote := t.TempDir(), storage.NewMemChunkRemote()
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
	if _, err := os.Stat(chunkFreshMarker(p.volumePath("v1"))); err != nil {
		t.Fatalf("fresh marker after the crash: %v; want it kept", err)
	}

	p2 := kzProvider(t, root, remote)
	if _, err := p2.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if got := kzCandidates(t, p2); got != 0 {
		t.Fatalf("second start: %d candidate chunks; want 0", got)
	}
	want := bytes.Repeat([]byte{0x6b}, 4096)
	if _, err := p2.Backend("v1").Write(ctx, 5*16*4096, want); err != nil {
		t.Fatal(err)
	}
	if err := p2.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	head, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := head.Chunks[5]
	if !ok {
		t.Fatal("the cut after the second start did not publish chunk 5")
	}
	chunk, err := remote.Get(ctx, ref.Key)
	if err != nil || !bytes.Equal(chunk[:4096], want) {
		t.Fatalf("published chunk 5: %v, data equal %v", err, err == nil && bytes.Equal(chunk[:4096], want))
	}
	if got := kzCandidates(t, p2); got != 0 {
		t.Fatalf("after the cut: %d candidate chunks; want 0", got)
	}
}
