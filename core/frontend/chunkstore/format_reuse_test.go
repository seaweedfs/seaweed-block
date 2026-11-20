package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func legacySegmentedFixture(t *testing.T) (*ChunkRemote, *fakeBlobs, storage.ChunkHead) {
	t.Helper()
	f, b, _, _, _ := gcFixture(t)
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	doc := segmentedHeadDoc{headIdentityDoc: headIdentityDoc{Format: 2, Gen: 1, Epoch: 1, Holder: "old", RemoteLSN: 42}, Segments: map[uint64]legacySegmentRefDoc{}, ChunkBytes: 4096, ChunkCount: 2}
	for group := uint64(0); group < 2; group++ {
		key := fmt.Sprintf("legacy-data-%d", group)
		data := []byte{byte(0x41 + group)}
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		idx := group * (manifestSegmentBytes / 4096)
		segment := legacySegmentDoc{Format: 1, Range: group, ChunkBytes: 4096, Owner: "v1", Chunks: map[uint64]legacyChunkRefDoc{idx: {Key: qualifyManifestKey("v1", key), SHA256: sha256.Sum256(data)}}}
		body, err := json.Marshal(segment)
		if err != nil {
			t.Fatal(err)
		}
		segmentKey := fmt.Sprintf("legacy-segment-%d", group)
		if err := r.Put(ctx, segmentKey, body); err != nil {
			t.Fatal(err)
		}
		doc.Segments[group] = legacySegmentRefDoc{Key: qualifyManifestKey("v1", segmentKey), SHA256: sha256.Sum256(body), Owner: "v1", Count: 1}
	}
	e := legacyHeadFixture(t, storage.ChunkHead{Gen: 1, Epoch: 1, Holder: "old", RemoteLSN: 42})
	e.Content, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.UpdateEntry(ctx, path.Join(catBase, "v1"), e, nil); err != nil {
		t.Fatal(err)
	}
	h, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return r, b.fakeBlobs, h
}

func TestManifestFormatUpgradeReusesColdSegmentsWithoutUploads(t *testing.T) {
	r, b, base := legacySegmentedFixture(t)
	ctx := context.Background()
	before, _, _ := b.counts()
	unchanged, ids, err := r.PrepareManifest(ctx, base, base, base.ChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	after, _, _ := b.counts()
	if before != after || len(ids) != 0 || unchanged.Segments[0] != base.Segments[0] || unchanged.Segments[1] != base.Segments[1] {
		t.Fatal("zero changed groups uploaded a segment or intent")
	}
	next := base
	next.RemoteLSN = 43
	next.Chunks = map[uint64]storage.ChunkRef{}
	for idx, ref := range base.Chunks {
		next.Chunks[idx] = ref
	}
	data := []byte{0x53}
	if err := r.Put(ctx, "changed-data", data); err != nil {
		t.Fatal(err)
	}
	idx := manifestSegmentBytes / base.ChunkBytes
	next.Chunks[idx] = storage.ChunkRef{Key: qualifyManifestKey("v1", "changed-data"), SHA256: sha256.Sum256(data), Encoding: "full", BirthLSN: 43}
	before, _, _ = b.counts()
	updated, ids, err := r.PrepareManifest(ctx, base, next, base.ChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	after, _, _ = b.counts()
	if after-before != 1 || len(ids) != 1 || updated.Segments[0] != base.Segments[0] || updated.Segments[1].SegFormat != 2 || updated.Segments[1].MaxBirthLSN != 43 {
		t.Fatal("one changed group rewrote cold segments or lost the new format")
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: base.Gen, Epoch: base.Epoch}, updated); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := r.DeleteIntent(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	h, err := r.GetHead(ctx)
	if err != nil || h.Segments[0].SegFormat != 1 || h.Chunks[0].BirthLSN != 42 || h.Chunks[idx].BirthLSN != 43 {
		t.Fatalf("mixed legacy/new image did not resolve: %+v %v", h, err)
	}
	got, err := r.Get(ctx, h.Chunks[idx].Key)
	if err != nil || len(got) != 1 || got[0] != 0x53 {
		t.Fatal("mixed root changed bytes")
	}
}

func TestManifestFormatUpgradeAllowsInheritedZeroBirth(t *testing.T) {
	f, b, _, _, _ := gcFixture(t)
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	base, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte{0x57}
	if err := r.Put(ctx, "zero-birth", data); err != nil {
		t.Fatal(err)
	}
	next := storage.ChunkHead{Epoch: 1, Holder: "base", Chunks: map[uint64]storage.ChunkRef{0: {Key: "zero-birth", SHA256: sha256.Sum256(data), Encoding: "full", BirthLSN: 0}}}
	next, ids, err := r.PrepareManifest(ctx, base, next, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: base.Gen, Epoch: base.Epoch}, next); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := r.DeleteIntent(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	h, err := r.GetHead(ctx)
	if err != nil || h.Chunks[0].BirthLSN != 0 || h.Segments[0].MaxBirthLSN != 0 {
		t.Fatalf("fresh base zero birth refused: %+v %v", h, err)
	}
}

func TestManifestFormatUpgradeSnapshotAndForkPreserveMetadata(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "format3", RequestID: "format3"}, h); err != nil {
		t.Fatal(err)
	}
	snapshotPath := snapPath(t, f, "v1", "format3")
	e, err := f.LookupEntry(ctx, path.Dir(snapshotPath), path.Base(snapshotPath))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Head struct {
			Format int `json:"format"`
			Levels int `json:"levels"`
		} `json:"head"`
	}
	if err := json.Unmarshal(e.Content, &snapshot); err != nil || snapshot.Head.Format != 3 || snapshot.Head.Levels != 1 {
		t.Fatalf("data snapshot embedded flat HEAD: %s %v", e.Content, err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "format3", Target: "v2", RequestID: "format3-fork"}); err != nil {
		t.Fatal(err)
	}
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	if err != nil {
		t.Fatal(err)
	}
	fork, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fork.RemoteLSN != h.RemoteLSN || len(fork.Chunks) != len(h.Chunks) {
		t.Fatal("fork lost inherited frontier or chunk count")
	}
	for idx, ref := range h.Chunks {
		if fork.Chunks[idx] != ref {
			t.Fatal("fork changed owner, encoding, birth or stored-byte hash")
		}
	}
	for group, ref := range h.Segments {
		if fork.Segments[group] != ref {
			t.Fatal("fork changed segment version or maximum")
		}
	}
}
