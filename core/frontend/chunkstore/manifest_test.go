package chunkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestManifest_CapturePublishesSegmentRoot(t *testing.T) {
	f, b, _, _, _ := gcFixture(t)
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "volume")
	s, err := storage.CreateWALStore(p, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Recover(); err != nil {
		t.Fatal(err)
	}
	l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: 8, Epoch: 1, Holder: "h1", FreshFromHead: true, Bitmap: storage.NewFileChunkBitmapStore(p + ".bitmap")})
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{0x61}, 4096)
	if _, err = s.Write(0, want); err != nil {
		t.Fatal(err)
	}
	if _, err = l.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, path.Join(catBase, "v1"), headName)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Format   int            `json:"format"`
		Chunks   map[string]any `json:"chunks"`
		Segments map[string]any `json:"segments"`
	}
	if err = json.Unmarshal(e.Content, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Format != 2 || len(doc.Chunks) != 0 || len(doc.Segments) != 1 {
		t.Fatalf("cut did not publish a small segmented root: %s", e.Content)
	}
	h, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := r.Get(ctx, h.Chunks[0].Key)
	if err != nil || !bytes.Equal(data[:4096], want) {
		t.Fatalf("resolved cut bytes: %v", err)
	}
}
