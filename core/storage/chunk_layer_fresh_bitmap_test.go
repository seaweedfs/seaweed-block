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
