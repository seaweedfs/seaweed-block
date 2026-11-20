package chunkstore

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Fuzz targets for the persisted chunkstore documents: the manifest JSON
// reader (duplicate keys, nesting, trailing bytes) and the HEAD document.
// Under `go test` (no -fuzz) they run their seeds.

func fuzzHeadSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	sum := sha256.Sum256([]byte("chunk"))
	heads := []storage.ChunkHead{
		{Gen: 1, Epoch: 2, Holder: "r1", RemoteLSN: 7, Chunks: map[uint64]storage.ChunkRef{0: {Key: "k0", SHA256: sum}, 5: {Key: "@v2/k5", SHA256: sum}}},
		{Gen: 3, Epoch: 4, Holder: "r2", RemoteLSN: 9, State: storage.ChunkHeadDetached,
			DetachedBy: &storage.ChunkDetachedBy{Holder: "r2", Epoch: 4, Gen: 3}, Chunks: map[uint64]storage.ChunkRef{}},
		{Gen: 2, Epoch: 1, Holder: "r1", RemoteLSN: 3, ChunkBytes: 4 << 20, ManifestChunkCount: 2,
			Segments: map[uint64]storage.ChunkSegment{0: {Key: "@v1/seg/00", SHA256: sum, Owner: "v1", Count: 2}}},
	}
	var out [][]byte
	for _, h := range heads {
		e, err := headEntry(h)
		if err != nil {
			tb.Fatalf("seed %+v: %v", h, err)
		}
		out = append(out, e.Content)
	}
	return append(out,
		[]byte(`{"format":1,"gen":1,"epoch":1,"holder":"a","remote_lsn":0,"gen":2}`), // duplicate key
		[]byte(`{"format":1,"gen":1}{"x":1}`),                                        // trailing value
		[]byte(`{"format":2,"gen":1,"segments":{"0":{"Key":"","Owner":"v","Count":1}},"chunk_bytes":4096,"chunk_count":1}`),
		[]byte(`[[[[[[[[[[1]]]]]]]]]]`), // nesting
		[]byte(``),
	)
}

// FuzzDecodeManifestJSON: never panics; when it accepts a document, the
// value equals what encoding/json alone decodes from the same bytes.
func FuzzDecodeManifestJSON(f *testing.F) {
	for _, s := range fuzzHeadSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		var got any
		if err := decodeManifestJSON(body, &got); err != nil {
			return
		}
		var want any
		if err := json.Unmarshal(body, &want); err != nil {
			t.Fatalf("accepted a document encoding/json rejects: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("decoded %v, encoding/json decodes %v", got, want)
		}
	})
}

// canonicalHead normalises the empty-collection forms the codec does not
// distinguish (an absent and an empty chunk map).
func canonicalHead(h storage.ChunkHead) storage.ChunkHead {
	if len(h.Chunks) == 0 {
		h.Chunks = nil
	}
	return h
}

// FuzzDecodeHeadDocument: never panics; a rejected document yields the zero
// head (no partial value); an accepted one re-encodes (headEntry) and
// decodes back to the same head, including through decodeHead with the
// entry's extended attributes.
func FuzzDecodeHeadDocument(f *testing.F) {
	for _, s := range fuzzHeadSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		h, err := DecodeHeadDocument("fuzz", body)
		if err != nil {
			if gap := knownHeadDecodeGap(h, err); gap != "" {
				return // pinned below
			}
			if !reflect.DeepEqual(h, storage.ChunkHead{}) {
				t.Fatalf("error %v with a partial head %+v", err, h)
			}
			return
		}
		e, err := headEntry(h)
		if err != nil {
			if gap := knownHeadEncodeGap(h); gap != "" {
				return
			}
			t.Fatalf("an accepted head does not re-encode: %v (%+v)", err, h)
		}
		again, err := decodeHead("fuzz", e)
		if err != nil {
			t.Fatalf("re-encoded head does not decode: %v", err)
		}
		if !reflect.DeepEqual(canonicalHead(again), canonicalHead(h)) {
			t.Fatalf("head round trip: %+v, want %+v", again, h)
		}
	})
}

// knownHeadDecodeGap names the rejections that return a partial head:
// decodeHeadDoc returns the half-built segmented head when validateRoot
// fails (chunk_remote.go, "return h, err"). Found by FuzzDecodeHeadDocument;
// reported, not changed here: no caller reads the head on that error.
func knownHeadDecodeGap(h storage.ChunkHead, err error) string {
	if err != nil && h.Segments != nil {
		return "segmented head returned with a validateRoot error"
	}
	return ""
}

// knownHeadEncodeGap names accepted heads headEntry refuses (none known).
func knownHeadEncodeGap(storage.ChunkHead) string { return "" }

// Pins the known partial-value gap that knownHeadDecodeGap exempts: a
// segmented root whose segment reference is invalid is rejected, but the
// half-built head comes back with the error. If this starts failing, the
// gap is closed: drop knownHeadDecodeGap.
func TestDecodeHeadDocumentReturnsAPartialHeadOnAnInvalidRoot(t *testing.T) {
	body := []byte(`{"format":2,"gen":1,"segments":{"0":{"Key":"","Owner":"v","Count":1}},"chunk_bytes":4096,"chunk_count":1}`)
	h, err := DecodeHeadDocument("fuzz", body)
	if err == nil {
		t.Fatal("an invalid segment reference was accepted")
	}
	if h.Gen != 1 || h.Segments == nil {
		t.Fatalf("expected the known partial head with the error, got %+v", h)
	}
}
