package storage

import (
	"context"
	"os"

	"testing"
)

// The mapped layer and the in-memory remote: after a crash the owned map
// makes the first cut incremental and exact, without a C11 file sidecar.
func TestChunkLayer_OwnedMapIncrementalAfterRestart(t *testing.T) {
	s, path, crash := crashableStore(t)
	remote := NewMemChunkRemote()
	cfg := chunkLayerConfig{ChunkBlocks: ctChunkBlocks, Epoch: 1, Holder: "h"}
	layer := mustAttach(t, s, remote, cfg)
	w := newWriteLog()
	w.write(t, s, 1, 0x01)  // chunk 0
	w.write(t, s, 21, 0x21) // chunk 5
	if _, err := layer.Capture(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 24, 0x24) // chunk 6, flushed: the owned map remembers it
	mustSyncFlush(t, s)
	w.write(t, s, 28, 0x28) // chunk 7, WAL only
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	assertNoC11Sidecars(t, path)
	crash()
	s2, _ := reopen(t, path)
	layer2 := mustAttach(t, s2, remote, cfg)
	before := remote.Puts()
	h, err := layer2.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n := remote.Puts() - before; n != 2 {
		t.Fatalf("first cut after restart stored %d chunks, want 2 (chunks 6 and 7)", n)
	}
	head, _ := remote.GetHead(context.Background())
	if head.RemoteLSN != h {
		t.Fatalf("HEAD remote_lsn %d, want %d", head.RemoteLSN, h)
	}
	img := make([]byte, ctNumBlocks*ctBlockSize)
	for idx, ref := range head.Chunks {
		b, err := remote.Get(context.Background(), ref.Key)
		if err != nil {
			t.Fatal(err)
		}
		copy(img[int(idx)*ctChunkBytes:], b)
	}
	assertImage(t, "HEAD image after restart", img, w.imageAt(h))
	assertNoC11Sidecars(t, path)
}

func assertNoC11Sidecars(t *testing.T, path string) {
	t.Helper()
	for _, suffix := range []string{".chunkbitmap", ".chunkbitmap.deltas"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("C11 sidecar %s: got %v, want absent", suffix, err)
		}
	}
}
