package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
)

// Page writeback has no order across offsets of one file, so an extent
// written from a WAL record that is not yet durable could survive a power
// loss that loses the record (an unacked write). After recovery the chunk's
// map entry would not have moved, the chunk would not be a cut candidate,
// and local reads would return the lost write's bytes while the published
// HEAD holds the chunk without them. The flusher therefore writes extents
// only from synced records (INV-WAL-V3-WRITE-AHEAD-001). Local and published
// must agree for every block of a chunk that is not a candidate.
//
// The crash is injected, not raced: the real flusher runs and is stopped at
// its fsync; the store file after the crash is the file as it was before the
// unacked write, plus that block's extent page as the flusher left it, as if
// only that page had reached the disk. A flusher that writes extents only
// from synced records leaves the page unchanged and passes.
func TestRedCrashKeepingAnUnsyncedExtentWriteLeavesLocalAndHEADApart(t *testing.T) {
	// The name keeps its red-first form so the history and the row match.
	ctx := context.Background()
	s, path, crash := crashableStore(t)
	remote := NewMemChunkRemote()
	cfg := chunkLayerConfig{ChunkBlocks: ctChunkBlocks, Epoch: 1, Holder: "h"}
	layer := mustAttach(t, s, remote, cfg)

	acked := bytes.Repeat([]byte{0xA1}, ctBlockSize)
	if _, err := s.Write(1, acked); err != nil { // chunk 0
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	// Time passes: the flusher checkpoints, so the cut's clear of chunk 0
	// is in the persisted map (chunk 0 is not a candidate any more).
	mustSyncFlush(t, s)
	if n := layer.Stats().DirtyChunks; n != 0 {
		t.Fatalf("after the cut and a checkpoint: %d candidate chunks; want 0", n)
	}
	// Read through the store's own handle (another open of the file is
	// refused on some platforms while the store holds its lock).
	fi, err := s.fd.Stat()
	if err != nil {
		t.Fatal(err)
	}
	before := make([]byte, fi.Size())
	if _, err := s.fd.ReadAt(before, 0); err != nil {
		t.Fatal(err)
	}

	// An unacked write to another block of chunk 0. The flusher runs (below
	// the WAL pressure threshold, as in normal use); the power is cut at its
	// fsync: the extent pages it wrote may be on the disk, the WAL record
	// (never synced) is not, and no checkpoint follows.
	lost := bytes.Repeat([]byte{0xB2}, ctBlockSize)
	const lostLBA = 2
	if _, err := s.Write(lostLBA, lost); err != nil {
		t.Fatal(err)
	}
	off := int64(s.extentBase) + int64(lostLBA)*int64(ctBlockSize)
	page := make([]byte, ctBlockSize)
	s.testBeforeFileSync = func() {
		if _, err := s.fd.ReadAt(page, off); err != nil {
			t.Error(err)
		}
		s.noteFsyncFailure(errors.New("power cut"))
	}
	_ = s.flusher.flushOnce() // fails at its fsync: no checkpoint
	s.testBeforeFileSync = nil
	crash()

	// The disk after the crash: the earlier file, plus only that extent page
	// as the flusher left it.
	img := append([]byte(nil), before...)
	copy(img[off:off+int64(ctBlockSize)], page)
	if err := os.WriteFile(path, img, 0o644); err != nil {
		t.Fatal(err)
	}
	s2, _ := reopen(t, path)
	layer2 := mustAttach(t, s2, remote, cfg)

	local, err := s2.Read(lostLBA)
	if err != nil {
		t.Fatal(err)
	}
	head, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	published := make([]byte, ctBlockSize)
	if ref, ok := head.Chunks[0]; ok {
		chunk, err := remote.Get(ctx, ref.Key)
		if err != nil {
			t.Fatal(err)
		}
		inChunk := (lostLBA % ctChunkBlocks) * ctBlockSize
		copy(published, chunk[inChunk:inChunk+ctBlockSize])
	}
	candidate := layer2.Stats().DirtyChunks > 0
	t.Logf("after the crash: local block %d = %#x..., published = %#x..., chunk 0 a cut candidate: %v", lostLBA, local[0], published[0], candidate)
	if !bytes.Equal(local, published) && !candidate {
		t.Fatalf("block %d reads the lost write's bytes locally (%#x) while HEAD holds %#x, and chunk 0 is not a cut candidate: local and published disagree with nothing to reconcile them", lostLBA, local[0], published[0])
	}
}
