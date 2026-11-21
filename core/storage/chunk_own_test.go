package storage

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func seedForeign(key string) bool { return strings.HasPrefix(key, "seed/") }

func ownCtx(t *testing.T) context.Context { return boundedCtx(t, context.Background()) }

// M6.Restore.Owning>Owning: each batch is one cut that re-uploads at most n
// foreign chunks under own keys, the image unchanged, until none is left.
func TestOwnBatch_BoundedBatchesReplaceEveryForeignKey(t *testing.T) {
	fastWalker(t)
	_, layer, remote, _ := hydrationFixture(t, 2, 64)
	before := remote.stats()
	want := remote.imageOf(t, before.head)
	if p, err := layer.HydrateAll(ownCtx(t)); err != nil || p.Remaining != 0 {
		t.Fatalf("hydrate: %+v %v", p, err)
	}
	total := ctNumBlocks / ctChunkBlocks
	left := total
	for left > 0 {
		puts := remote.stats().puts
		cas := remote.stats().cas
		p, err := layer.OwnBatch(ownCtx(t), 3, seedForeign)
		if err != nil {
			t.Fatal(err)
		}
		step := min(3, left)
		if p.Owned != step || p.Remaining != left-step || p.NotLocal != 0 {
			t.Fatalf("batch: %+v, want %d owned and %d remaining", p, step, left-step)
		}
		if got := remote.stats().puts - puts; got != step {
			t.Fatalf("batch uploaded %d chunks, want exactly %d (bounded)", got, step)
		}
		if got := remote.stats().cas - cas; got != 1 {
			t.Fatalf("batch made %d HEAD CAS, want one", got)
		}
		left -= step
	}
	after := remote.stats().head
	for idx, ref := range after.Chunks {
		if seedForeign(ref.Key) {
			t.Fatalf("chunk %d still foreign after the last batch: %s", idx, ref.Key)
		}
	}
	if !bytes.Equal(remote.imageOf(t, after), want) {
		t.Fatal("owning changed the volume image")
	}
	cas := remote.stats().cas
	if p, err := layer.OwnBatch(ownCtx(t), 3, seedForeign); err != nil || p != (OwnProgress{}) || remote.stats().cas != cas {
		t.Fatalf("nothing left to own must run no cut: %+v %v", p, err)
	}
}

// A chunk not local yet is never fetched by a batch: it waits for the walker.
func TestOwnBatch_SkipsChunksNotLocal(t *testing.T) {
	_, layer, remote, _ := hydrationFixture(t, 2, 64)
	before := remote.stats()
	p, err := layer.OwnBatch(ownCtx(t), 3, seedForeign)
	total := ctNumBlocks / ctChunkBlocks
	if err != nil || p.Owned != 0 || p.Remaining != total || p.NotLocal != total {
		t.Fatalf("batch before any hydration: %+v %v", p, err)
	}
	after := remote.stats()
	if after.cas != before.cas || after.gets != before.gets {
		t.Fatalf("a batch with nothing local cut or fetched: cas %d->%d gets %d->%d", before.cas, after.cas, before.gets, after.gets)
	}
}

// A batch is an ordinary cut: a guest write since the last HEAD goes with it.
func TestOwnBatch_CarriesGuestWrites(t *testing.T) {
	fastWalker(t)
	s, layer, remote, _ := hydrationFixture(t, 2, 64)
	if _, err := layer.HydrateAll(ownCtx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctChunkBlocks*6+1, makeBlock(ctBlockSize, 0xAB)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.OwnBatch(ownCtx(t), 2, seedForeign); err != nil {
		t.Fatal(err)
	}
	img := remote.imageOf(t, remote.stats().head)
	off := (ctChunkBlocks*6 + 1) * ctBlockSize
	if !bytes.Equal(img[off:off+ctBlockSize], makeBlock(ctBlockSize, 0xAB)) {
		t.Fatal("the guest write did not reach HEAD with the batch")
	}
}
