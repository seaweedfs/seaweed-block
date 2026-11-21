package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
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

// A chunk whose every block the guest overwrote and flushed before the walker
// reached it stays absent but is fully local: a batch owns it with no fetch
// (codex01 #340 S1: otherwise nothing ever cuts it and the driver stalls).
func TestOwnBatch_OwnsAFullyOverwrittenAbsentChunkWithoutAFetch(t *testing.T) {
	fastWalker(t)
	s, layer, remote, _ := hydrationFixture(t, 2, 64)
	const idx = 3
	for i := uint32(0); i < ctChunkBlocks; i++ {
		if _, err := s.Write(idx*ctChunkBlocks+i, makeBlock(ctBlockSize, 0xC0+byte(i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	layer.stMu.Lock()
	absent := layer.absent[idx]
	layer.stMu.Unlock()
	if !absent {
		t.Fatal("fixture: the overwritten chunk must still be absent")
	}
	if _, err := layer.HydrateAll(ownCtx(t)); err != nil {
		t.Fatal(err)
	}
	seedKey := remote.stats().head.Chunks[idx].Key
	// The stall needs this chunk to be the only foreign one left: then no
	// other pick triggers the cut that would carry its guest write.
	onlyThis := func(key string) bool { return key == seedKey }
	p, err := layer.OwnBatch(ownCtx(t), 3, onlyThis)
	if err != nil || p.Owned != 1 || p.Remaining != 0 {
		t.Fatalf("batch with only the fully overwritten chunk foreign: %+v %v, want it owned", p, err)
	}
	if n := countGets(remote)[seedKey]; n != 0 {
		t.Fatalf("the fully overwritten chunk was fetched %d times", n)
	}
	img := remote.imageOf(t, remote.stats().head)
	for i := 0; i < ctChunkBlocks; i++ {
		off := (idx*ctChunkBlocks + i) * ctBlockSize
		if !bytes.Equal(img[off:off+ctBlockSize], makeBlock(ctBlockSize, 0xC0+byte(i))) {
			t.Fatalf("block %d of the overwritten chunk is not the guest's in HEAD", i)
		}
	}
}

// A failed ordinary cut remembers an equal-to-HEAD chunk without a key; the
// next batch must still upload it under an own key (codex01 #340 S2).
func TestOwnBatch_UploadsAChunkRememberedEqualByAFailedCut(t *testing.T) {
	fastWalker(t)
	s, layer, remote, _ := hydrationFixture(t, 2, 64)
	if _, err := layer.HydrateAll(ownCtx(t)); err != nil {
		t.Fatal(err)
	}
	same := makeBlock(ctBlockSize, 0x40) // block 0 rewritten with HEAD's own bytes
	if _, err := s.Write(0, same); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0xEE)); err != nil { // chunk 1 changed
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	remote.failPut = errors.New("test: put refused")
	remote.mu.Unlock()
	failCtx, cancelFail := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelFail()
	if _, err := layer.Cut(failCtx); err == nil {
		t.Fatal("fixture: the ordinary cut must fail")
	}
	remote.mu.Lock()
	remote.failPut = nil
	remote.mu.Unlock()
	layer.stMu.Lock()
	sc, kept := layer.reusable[0]
	layer.stMu.Unlock()
	if !kept || sc.key != "" {
		t.Fatalf("fixture: the failed cut must remember chunk 0 equal and keyless: kept=%v key=%q", kept, sc.key)
	}
	p, err := layer.OwnBatch(ownCtx(t), 1, seedForeign)
	if err != nil || p.Owned != 1 {
		t.Fatalf("batch after the failed cut: %+v %v, want chunk 0 owned", p, err)
	}
	if k := remote.stats().head.Chunks[0].Key; seedForeign(k) {
		t.Fatalf("chunk 0 still names the foreign key %s", k)
	}
	if !bytes.Equal(remote.imageOf(t, remote.stats().head)[:ctBlockSize], same) {
		t.Fatal("chunk 0's bytes changed")
	}
}
