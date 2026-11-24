package durable_test

// The first cut of a volume. A local store created empty against a HEAD
// that was never claimed and holds no segments knows every chunk it has not
// written is zero, so its first cut scans only the chunks written since; a
// store whose relation to HEAD is not known (a claimed HEAD, a restore or
// fork target, a store an earlier release created) keeps scanning every
// chunk it holds locally.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// fcutChunks is chunkProvider's chunk count: pcBlocks blocks of pcChunkBlocks.
const fcutChunks = pcBlocks / pcChunkBlocks

func fcutProvider(t *testing.T, root string, remote storage.ChunkRemote) *durable.DurableProvider {
	return chunkProvider(t, root, remote, v1View(), 0)
}

// fcutCheckpointedPast waits until the store's checkpoint has moved past
// from, so a write made after from has left the WAL's dirty map.
func fcutCheckpointedPast(t *testing.T, st storage.LogicalStorage, from uint64) {
	t.Helper()
	ws := st.(*storage.WALStore)
	waitFor(t, 10*time.Second, "the WAL checkpoint past the write", func() bool { return ws.CheckpointLSN() > from })
}

// fcutStats returns v1's chunk-layer counters.
func fcutStats(t *testing.T, p *durable.DurableProvider) storage.ChunkLayerStats {
	t.Helper()
	for _, st := range p.DurableStatuses() {
		if st.VolumeID == "v1" && st.Chunk != nil && st.Chunk.Attached {
			return st.Chunk.Stats
		}
	}
	t.Fatal("v1 has no attached chunk layer")
	return storage.ChunkLayerStats{}
}

func fcutOpen(t *testing.T, ctx context.Context, p *durable.DurableProvider) storage.LogicalStorage {
	t.Helper()
	st, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	return st
}

func fcutWrite(t *testing.T, ctx context.Context, p *durable.DurableProvider, lba int, fill byte) []byte {
	t.Helper()
	data := bytes.Repeat([]byte{fill}, 4096)
	b := p.Backend("v1")
	if n, err := b.Write(ctx, int64(lba)*4096, data); err != nil || n != len(data) {
		t.Fatalf("write lba %d: %d %v", lba, n, err)
	}
	if err := b.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	return data
}

// fcutHeadBlock reads block lba from the published HEAD (zero if its chunk
// is not named).
func fcutHeadBlock(t *testing.T, ctx context.Context, remote *storage.MemChunkRemote, lba int) []byte {
	t.Helper()
	head, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := head.Chunks[uint64(lba/pcChunkBlocks)]
	if !ok {
		return make([]byte, 4096)
	}
	c, err := remote.Get(ctx, ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	off := (lba % pcChunkBlocks) * 4096
	return c[off : off+4096]
}

// TestRedFirstCutOfAFreshVolumeScansOnlyWrittenChunks: a store created
// empty against a HEAD never claimed, with two chunks written (one of them
// already checkpointed out of the WAL), has two first-cut candidates, and
// its first cut publishes both. Today every chunk of the volume is a
// candidate (all 16 start Unknown).
func TestRedFirstCutOfAFreshVolumeScansOnlyWrittenChunks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	p := fcutProvider(t, t.TempDir(), remote)
	st := fcutOpen(t, ctx, p)
	cp0 := st.(*storage.WALStore).CheckpointLSN()
	a := fcutWrite(t, ctx, p, pcChunkBlocks*1+1, 0x31) // chunk 1, checkpointed below
	fcutCheckpointedPast(t, st, cp0)
	b := fcutWrite(t, ctx, p, pcChunkBlocks*9+3, 0x39) // chunk 9, still in the WAL
	if got := fcutStats(t, p).DirtyChunks; got != 2 {
		t.Errorf("before the first cut: %d candidate chunks; want 2 (the written ones), not all %d", got, fcutChunks)
	}
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if got := fcutStats(t, p).LastCut.CandidateChunks; got != 2 {
		t.Errorf("first cut: %d candidate chunks; want 2", got)
	}
	if !bytes.Equal(fcutHeadBlock(t, ctx, remote, pcChunkBlocks*1+1), a) || !bytes.Equal(fcutHeadBlock(t, ctx, remote, pcChunkBlocks*9+3), b) {
		t.Fatal("the first cut did not publish both written blocks")
	}
}

// TestFirstCutAgainstAClaimedHeadScansEveryChunk: a store created empty
// against a HEAD another store already claimed (no segments yet) keeps
// every chunk a candidate. Passes today; pins the fix's scope.
func TestFirstCutAgainstAClaimedHeadScansEveryChunk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	first := fcutProvider(t, t.TempDir(), remote)
	fcutOpen(t, ctx, first)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if head, err := remote.GetHead(ctx); err != nil || head.Gen == 0 || len(head.Segments) != 0 {
		t.Fatalf("setup: HEAD %+v %v; want a claimed HEAD with no segments", head, err)
	}
	p := fcutProvider(t, t.TempDir(), remote)
	fcutOpen(t, ctx, p)
	if got := fcutStats(t, p).DirtyChunks; got != fcutChunks {
		t.Fatalf("store created against a claimed HEAD: %d candidate chunks; want all %d", got, fcutChunks)
	}
}

// TestFirstCutOfARestoreTargetKeepsItsHydrationPath: a store created on a
// new root against a HEAD that names chunks hydrates them (they are
// absent, not candidates) and keeps every other chunk a candidate. Passes
// today; pins the fix's scope.
func TestFirstCutOfARestoreTargetKeepsItsHydrationPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	first := fcutProvider(t, t.TempDir(), remote)
	fcutOpen(t, ctx, first)
	fcutWrite(t, ctx, first, pcChunkBlocks*3, 0x33)
	fcutWrite(t, ctx, first, pcChunkBlocks*7, 0x37)
	if _, err := first.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	p := fcutProvider(t, t.TempDir(), remote)
	fcutOpen(t, ctx, p)
	s := fcutStats(t, p)
	if s.AbsentChunks != 2 || s.DirtyChunks != fcutChunks-2 {
		t.Fatalf("restore target: %d absent, %d candidate chunks; want 2 absent and %d candidates", s.AbsentChunks, s.DirtyChunks, fcutChunks-2)
	}
}
