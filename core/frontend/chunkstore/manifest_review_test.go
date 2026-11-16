package chunkstore

import (
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type manifestFilerBlobs struct {
	Filer
	Blobs
}

func TestManifest_MixedRepresentationRefusesReadAndWholeGC(t *testing.T) {
	for _, format := range []int{1, 2} {
		t.Run(string(rune('0'+format)), func(t *testing.T) {
			f, b, c, g, now := gcFixture(t)
			ctx := context.Background()
			r, _ := publishSegmented(t, f, b)
			if err := r.Put(ctx, "eligible-orphan", []byte("keep on mark error")); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Run(ctx); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(2 * time.Hour)
			e, err := f.LookupEntry(ctx, path.Join(catBase, "v1"), headName)
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]json.RawMessage
			if err = json.Unmarshal(e.Content, &doc); err != nil {
				t.Fatal(err)
			}
			if format == 1 {
				doc["format"] = json.RawMessage("1")
			} else {
				doc["chunks"] = json.RawMessage("{}")
			}
			e.Content, err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.UpdateEntry(ctx, path.Join(catBase, "v1"), e, nil); err != nil {
				t.Fatal(err)
			}
			if h, err := r.GetHead(ctx); err == nil {
				t.Errorf("mixed root returned readable image (possibly zeros): %+v", h)
			}
			if h, _, err := c.ReadHead(ctx, "v1"); err == nil {
				t.Errorf("catalog accepted mixed root: %+v", h)
			}
			_, _, before := b.counts()
			for _, apply := range []bool{false, true} {
				if _, err := g.run(ctx, apply); err == nil {
					t.Errorf("mixed root accepted by GC apply=%v", apply)
				}
			}
			_, _, after := b.counts()
			if before != after {
				t.Fatal("failed mark deleted objects")
			}
		})
	}
}

func TestManifest_UsageCountsOwnedClippedChunksAndForkHolders(t *testing.T) {
	ctx := context.Background()
	f := newFakeFiler()
	b := gcBlobs{newFakeBlobs()}
	c, err := NewCatalog(manifestFilerBlobs{f, b}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	for _, vol := range []string{"v1", "v2"} {
		if err = f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = c.EnsureVolume(ctx, "v1", Geometry{NumBlocks: 10, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	r, h := publishSegmented(t, f, b)
	if _, err = c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s", RequestID: "snapshot"}, h); err != nil {
		t.Fatal(err)
	}
	u, err := c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if u.LiveChunks != 2 || u.LiveBytes != 40960 || len(u.Snapshots) != 1 || u.Snapshots[0].Chunks != 2 || u.Snapshots[0].ExclusiveBytes != 0 {
		t.Fatalf("segmented clipped usage: %+v", u)
	}
	segmentBytes := uint64(0)
	for _, ref := range h.Segments {
		body, e := r.Get(ctx, ref.Key)
		if e != nil {
			t.Fatal(e)
		}
		segmentBytes += uint64(len(body))
	}
	if u.ManifestBindings != 1 || u.ManifestBytes != segmentBytes || u.StoredBytes != 40960+segmentBytes || u.StoredUpperBound {
		t.Fatalf("manifest bytes must not be charged as a full data chunk: %+v", u)
	}
	if _, err = c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s", Target: "v2", RequestID: "fork"}); err != nil {
		t.Fatal(err)
	}
	if _, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN}); err != nil {
		t.Fatal(err)
	}
	u, err = c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if u.LiveChunks != 0 || u.Snapshots[0].Chunks != 2 || u.Snapshots[0].ExclusiveBytes != 0 {
		t.Fatalf("foreign fork owner omitted: %+v", u)
	}
	fork, _ := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	fh, err := fork.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fork.CASHead(ctx, storage.ChunkHeadVersion{Gen: fh.Gen, Epoch: fh.Epoch}, storage.ChunkHead{RemoteLSN: fh.RemoteLSN}); err != nil {
		t.Fatal(err)
	}
	u, err = c.Usage(ctx, "v1")
	if err != nil || u.Snapshots[0].ExclusiveBytes != 40960 {
		t.Fatalf("snapshot exclusivity must use clipped owner bytes: %+v %v", u, err)
	}
}
