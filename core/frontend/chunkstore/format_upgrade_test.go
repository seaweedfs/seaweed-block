package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestManifestFormatUpgradeWritesVersionedStoredByteReferences(t *testing.T) {
	f, b, _, _, _ := gcFixture(t)
	r, h := publishSegmented(t, f, b)
	ctx := context.Background()
	entry, err := f.LookupEntry(ctx, path.Join(catBase, "v1"), headName)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Format   int `json:"format"`
		Levels   int `json:"levels"`
		Segments map[string]struct {
			Format int    `json:"seg_format"`
			Max    uint64 `json:"max_birth_lsn"`
		} `json:"segments"`
	}
	if err := json.Unmarshal(entry.Content, &root); err != nil {
		t.Fatal(err)
	}
	if root.Format != 3 || root.Levels != 1 || root.Segments["0"].Format != 2 || root.Segments["0"].Max != h.RemoteLSN {
		t.Fatalf("missing format3/levels/segment2 metadata: %s", entry.Content)
	}
	body, err := r.Get(ctx, h.Segments[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	var segment struct {
		Format int    `json:"format"`
		Max    uint64 `json:"max_birth_lsn"`
		Chunks map[string]struct {
			Encoding string `json:"enc"`
			Birth    uint64 `json:"birth_lsn"`
			Hash     string `json:"sha256"`
		} `json:"chunks"`
	}
	if err := json.Unmarshal(body, &segment); err != nil {
		t.Fatal(err)
	}
	if segment.Format != 2 || segment.Max != h.RemoteLSN || len(segment.Chunks) != 2 {
		t.Fatalf("missing segment format/max: %s", body)
	}
	for _, ref := range segment.Chunks {
		if ref.Encoding != "full" || ref.Birth != h.RemoteLSN || len(ref.Hash) != 64 {
			t.Fatalf("invalid inherited chunk reference: %+v", ref)
		}
	}
	resolved, err := r.GetHead(ctx)
	if err != nil || len(resolved.Chunks) != 2 {
		t.Fatalf("format3 roundtrip: %+v %v", resolved, err)
	}
}

func TestManifestFormatUpgradeRefusesUnknownSegmentField(t *testing.T) {
	f, b, _, _, _ := gcFixture(t)
	r, h := publishSegmented(t, f, b)
	ctx := context.Background()
	if _, err := r.GetHead(ctx); err != nil {
		t.Fatalf("valid legacy segment refused: %v", err)
	}
	ref := h.Segments[0]
	body, err := r.Get(ctx, ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the object hash valid: the refusal must come from the segment schema.
	body = append(bytes.TrimSuffix(body, []byte("}")), []byte(",\"future_encoding\":\"unsupported\"}")...)
	if err := r.Put(ctx, "format-upgrade-unknown-field", body); err != nil {
		t.Fatal(err)
	}
	ref.Key = qualifyManifestKey(ref.Owner, "format-upgrade-unknown-field")
	ref.SHA256 = sha256.Sum256(body)
	h.Segments[0] = ref
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetHead(ctx); err == nil {
		t.Fatal("published segment with unknown field accepted")
	}
}
