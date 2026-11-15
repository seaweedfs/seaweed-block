package storage

import (
	"testing"
)

// A fresh local volume starts with an empty bitmap bound to HEAD, not in
// hash mode: its first cut uploads only the chunks actually written
// (integration ruling F1). Hash mode stays for an existing volume whose
// bitmap is lost.
func TestChunkLayer_FreshVolumeStartsWithEmptyBitmap(t *testing.T) {
	for _, tc := range []struct {
		name        string
		initialHead bool // the empty HEAD the VOLUME entry comes with
	}{
		{"no HEAD yet", false},
		{"initial empty HEAD", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeChunkRemote()
			if tc.initialHead {
				f.mu.Lock()
				f.installLocked(chunkHead{Chunks: map[uint64]chunkRef{}, State: chunkHeadActive})
				f.mu.Unlock()
			}
			s, _ := newChunkTestStore(t)
			cfg := ctConfig(1)
			cfg.FreshFromHead = true
			layer := mustAttach(t, s, f, cfg)
			w := newWriteLog()
			w.write(t, s, 3, 0x33) // one chunk written
			before := f.stats()
			r, err := layer.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if r.FullHash {
				t.Fatal("the first cut of a fresh volume compared every chunk by hash")
			}
			if n := f.stats().puts - before.puts; n != 1 {
				t.Fatalf("the first cut uploaded %d chunks, want only the one written", n)
			}
			assertEveryHeadIsPrefixImage(t, f, w)
		})
	}
}

// Control: an existing volume (not fresh) without a bitmap compares every
// chunk on its first cut.
func TestChunkLayer_LostBitmapStillHashes(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	copy(src, makeBlock(ctBlockSize, 0x44))
	f := newFakeChunkRemote()
	f.seedHead(t, src, 0, 0) // an older epoch: this holder may publish over it
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 3, 0x33)
	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !r.FullHash {
		t.Fatal("an existing volume without a bitmap did not compare every chunk")
	}
}
