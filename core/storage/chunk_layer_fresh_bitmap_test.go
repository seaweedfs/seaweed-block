package storage

import (
	"testing"
)

// A fresh local volume starts with an empty bitmap bound to HEAD, not in
// hash mode: its first cut uploads only the chunks actually written
// (integration ruling F1). An existing volume whose bitmap is lost
// reconciles its dirty set against HEAD instead (F1(b)).
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

// Control: an existing volume (not fresh) without a bitmap reconciles its
// local image against HEAD before it publishes, so a chunk that differs
// from HEAD without a recorded write is still uploaded by its first cut.
func TestChunkLayer_LostBitmapReconciles(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	copy(src, makeBlock(ctBlockSize, 0x44)) // HEAD's block 0 differs from the local image
	f := newFakeChunkRemote()
	f.seedHead(t, src, 0, 0) // an older epoch: this holder may publish over it
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, f, ctConfig(1)) // waits for reconciliation
	if st := layer.Stats(); st.Reconciling || st.ReconcileTotal == 0 || st.ReconcileDone != st.ReconcileTotal {
		t.Fatalf("an existing volume without a bitmap did not reconcile: reconciling=%v done=%d/%d", st.Reconciling, st.ReconcileDone, st.ReconcileTotal)
	}
	w := newWriteLog()
	w.write(t, s, ctNumBlocks-1, 0x33) // the last chunk, never HEAD's block 0 chunk
	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	assertImage(t, "first cut after reconciliation vs the local image", f.imageOf(t, r.Head), w.imageAt(r.H))
}
