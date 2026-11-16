package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
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

func publishSegmented(t *testing.T, f *fakeFiler, b gcBlobs) (*ChunkRemote, storage.ChunkHead) {
	t.Helper()
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x41), 1: chunkBytes(0x42)})
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	next, ids, err := r.PrepareManifest(context.Background(), h, h, 32768)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := r.CASHead(context.Background(), storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next)
	if err != nil {
		t.Fatal(err)
	}
	next.Gen = gen
	for _, id := range ids {
		if err = r.DeleteIntent(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	return r, next
}

func TestManifest_ForkAfterParentSnapshotDelete(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, h := publishSegmented(t, f, b)
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "snap"}, h); err != nil {
		t.Fatal(err)
	}
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork"}); err != nil {
		t.Fatal(err)
	}
	fork, _ := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	fh, err := fork.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(fh.Segments) != 1 || len(fh.Chunks) != 2 {
		t.Fatalf("fork lost root/children: %+v", fh)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := g.Run(ctx); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(2 * time.Hour)
	}
	fh, err = fork.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for idx, ref := range fh.Chunks {
		got, err := fork.Get(ctx, ref.Key)
		if err != nil || !bytes.Equal(got, chunkBytes(byte(0x41+idx))) {
			t.Fatalf("fork lost shared chunk %d after parent delete: %v", idx, err)
		}
	}
}

func TestManifest_GCReadsPublishedSegments(t *testing.T) {
	f, b, _, g, _ := gcFixture(t)
	publishSegmented(t, f, b)
	out, err := g.Preview(context.Background())
	if err != nil || len(out.Roots) == 0 {
		t.Fatalf("GC cannot trace published segment: %+v %v", out, err)
	}
}

func TestManifest_ReusesUnchangedRangeAndCrashKeepsOldRoot(t *testing.T) {
	f, b, _, _, _ := gcFixture(t)
	ctx := context.Background()
	r, h := publishSegmented(t, f, b)
	// A second logical GiB uses a different immutable range; no local 1GiB
	// allocation is needed to exercise remote manifest publication.
	per := manifestSegmentBytes / h.ChunkBytes
	key := "second-range"
	data := chunkBytes(0x77)
	if err := r.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	h.Chunks[per] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	next, ids, err := r.PrepareManifest(ctx, h, h, h.ChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	if next.Segments[0] != h.Segments[0] {
		t.Fatal("unchanged range rewritten")
	}
	// Simulate death after immutable upload, with no CAS or cleanup.
	old, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(old.Chunks) != 2 || len(old.Segments) != 1 {
		t.Fatal("unpublished segment changed old root")
	}
	intents, err := r.ListIntents(ctx)
	if err != nil || len(intents) != len(ids) {
		t.Fatalf("uploaded segment unprotected: %v", err)
	}
	for _, ref := range old.Chunks {
		if _, err := r.Get(ctx, ref.Key); err != nil {
			t.Fatal(err)
		}
	}
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: old.Gen, Epoch: old.Epoch}, next)
	if err != nil || gen != old.Gen+1 {
		t.Fatalf("normal CAS semantics: %d %v", gen, err)
	}
}

func TestManifest_UnreadablePublishedSegmentRefusesWholeGC(t *testing.T) {
	for _, apply := range []bool{false, true} {
		t.Run(fmt.Sprint(apply), func(t *testing.T) {
			f, b, _, g, now := gcFixture(t)
			ctx := context.Background()
			r, h := publishSegmented(t, f, b)
			if err := r.Put(ctx, "unreachable", []byte("orphan")); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Run(ctx); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(2 * time.Hour)
			var ref storage.ChunkSegment
			for _, v := range h.Segments {
				ref = v
				break
			}
			dir, name, err := r.keyLocation(ref.Key)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := f.LookupEntry(ctx, dir, name)
			if err != nil {
				t.Fatal(err)
			}
			b.mu.Lock()
			b.data[string(entry.Extended[extFid])] = []byte("corrupted segment")
			before := len(b.data)
			b.mu.Unlock()
			if _, err := g.run(ctx, apply); err == nil {
				t.Fatal("unreadable published segment accepted")
			}
			b.mu.Lock()
			after := len(b.data)
			b.mu.Unlock()
			if before != after {
				t.Fatal("failed mark partially deleted data")
			}
		})
	}
}

func TestManifest_CopyIntentProtectsChildrenWithoutHead(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, h := publishSegmented(t, f, b)
	if _, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: h.Gen, Epoch: h.Epoch, Holder: "snapshot:v1/unpublished/attempt", Keys: headKeys(h), Segments: h.Segments, ChunkBytes: h.ChunkBytes}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if out, err := g.Run(ctx); err != nil || out.Reclaimed != 0 {
			t.Fatalf("copy intent lost transitive children: %+v %v", out, err)
		}
		*now = now.Add(2 * time.Hour)
	}
	for _, ref := range h.Chunks {
		if _, err := r.Get(ctx, ref.Key); err != nil {
			t.Fatal(err)
		}
	}
}
