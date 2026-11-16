package chunkstore

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"testing"
	"time"
	"unicode"

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

func TestManifest_RejectsFieldAliases(t *testing.T) {
	for _, field := range []string{`"segments":null`, `"Segments":null`, `"CHUNK_BYTES":0`, `"chunk_count":0`} {
		body := []byte(`{"format":1,"gen":1,"epoch":1,"holder":"h","chunks":{},` + field + `}`)
		if _, err := DecodeHeadDocument("mixed", body); err == nil {
			t.Errorf("format1 accepted residual field %s", field)
		}
	}
	for _, field := range []string{`"chunks":{}`, `"Chunks":null`} {
		body := []byte(`{"format":2,"gen":1,"epoch":1,"holder":"h","chunk_bytes":32768,"chunk_count":0,"segments":{},` + field + `}`)
		if _, err := DecodeHeadDocument("mixed", body); err == nil {
			t.Errorf("format2 accepted residual field %s", field)
		}
	}
}

func TestManifest_FormatSpecificFoldedFields(t *testing.T) {
	for _, tc := range []struct {
		format int
		name   string
	}{{1, "segments"}, {1, "chunk_bytes"}, {1, "chunk_count"}, {2, "chunks"}} {
		variants := map[string]bool{tc.name: true, strings.ToUpper(tc.name): true}
		original := []rune(tc.name)
		for i, r := range original {
			for folded := unicode.SimpleFold(r); folded != r; folded = unicode.SimpleFold(folded) {
				key := append([]rune(nil), original...)
				key[i] = folded
				variants[string(key)] = true
			}
		}
		for key := range variants {
			encoded, _ := json.Marshal(key)
			suffix := string(encoded) + `:null`
			root := `{"format":1,"gen":1,"epoch":1,"holder":"h",` + suffix + `}`
			if tc.format == 2 {
				root = `{"format":2,"gen":1,"epoch":1,"holder":"h","chunk_bytes":32768,"segments":{},` + suffix + `}`
			}
			if _, err := DecodeHeadDocument("folded", []byte(root)); err == nil {
				t.Errorf("format%d accepted folded residual %s", tc.format, key)
			}
		}
	}
	for _, root := range []string{
		`{"format":1,"gen":1,"epoch":1,"holder":"h","\u017fegments":null}`,
		`{"format":2,"gen":1,"epoch":1,"holder":"h","chunk_bytes":32768,"segments":{},"chun\u212as":null}`,
	} {
		if _, err := DecodeHeadDocument("escaped", []byte(root)); err == nil {
			t.Errorf("accepted escaped mixed root: %s", root)
		}
	}
}

func TestManifest_LongSDiscriminatorCorruptionRefusesReadAndGC(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := publishSegmented(t, f, b)
	if err := r.Put(ctx, "orphan", []byte("do not delete after bad mark")); err != nil {
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
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(e.Content, &fields); err != nil {
		t.Fatal(err)
	}
	fields["format"] = json.RawMessage("1")
	fields["\u017fegments"] = fields["segments"]
	delete(fields, "segments")
	delete(fields, "chunk_bytes")
	delete(fields, "chunk_count")
	e.Content, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.UpdateEntry(ctx, path.Join(catBase, "v1"), e, nil); err != nil {
		t.Fatal(err)
	}
	if h, err := r.GetHead(ctx); err == nil {
		t.Errorf("Unicode mixed root became readable empty image: %+v", h)
	}
	if h, _, err := c.ReadHead(ctx, "v1"); err == nil {
		t.Errorf("catalog accepted Unicode mixed root: %+v", h)
	}
	_, _, before := b.counts()
	for _, apply := range []bool{false, true} {
		if _, err := g.run(ctx, apply); err == nil {
			t.Errorf("Unicode mixed root accepted by GC apply=%v", apply)
		}
	}
	_, _, after := b.counts()
	if before != after {
		t.Fatal("Unicode mixed root allowed partial delete")
	}
}
