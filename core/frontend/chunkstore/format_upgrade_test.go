package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

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
