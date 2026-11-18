package storage

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLegacyC11FileCodec_RoundTripAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vol.chunkbitmap")
	b := newLegacyC11FileStore(path)
	if _, _, ok, err := b.Load(); ok || err != nil {
		t.Fatalf("empty store: ok %v err %v", ok, err)
	}
	if err := b.Save([]uint64{3, 7}); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteHeader(chunkBitmapHeader{Base: chunkBaseInstalling}); err != nil {
		t.Fatal(err)
	}
	hdr, dirty, ok, err := b.Load()
	if !ok || err != nil || hdr.Base != chunkBaseInstalling || !reflect.DeepEqual(dirty, []uint64{3, 7}) {
		t.Fatalf("after Save+WriteHeader: ok %v err %v hdr %+v dirty %v", ok, err, hdr, dirty)
	}
	want := chunkBitmapHeader{BaseLSN: 40, HeadGen: 3, Epoch: 2}
	delta := chunkDelta{FromLSN: 10, ToLSN: 40, Gen: 3, Dirty: []uint64{3}, NewKeys: map[uint64]string{3: "k3"}}
	if err := b.Reset(want, []uint64{7}, delta); err != nil {
		t.Fatal(err)
	}
	// A new instance reads exactly the durable state.
	b2 := newLegacyC11FileStore(path)
	hdr, dirty, ok, err = b2.Load()
	if !ok || err != nil || hdr != want || !reflect.DeepEqual(dirty, []uint64{7}) {
		t.Fatalf("reopened: ok %v err %v hdr %+v dirty %v", ok, err, hdr, dirty)
	}
	deltas, err := b2.Deltas()
	if err != nil || len(deltas) != 1 || !reflect.DeepEqual(deltas[0], delta) {
		t.Fatalf("delta log: %v %+v", err, deltas)
	}
}

func TestLegacyC11FileCodec_CorruptStateIsLost(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vol.chunkbitmap")
	b := newLegacyC11FileStore(path)
	if err := b.Save([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF // one flipped byte
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := newLegacyC11FileStore(path).Load(); ok || err != nil {
		t.Fatalf("corrupt state: ok %v err %v, want ok=false (hash-compare fallback) and no error", ok, err)
	}
}

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
