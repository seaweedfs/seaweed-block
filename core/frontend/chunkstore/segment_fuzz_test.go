package chunkstore

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func FuzzDecodeVersionedSegment(f *testing.F) {
	sum := sha256.Sum256([]byte("stored bytes"))
	refs := map[uint64]storage.ChunkRef{0: {Key: "@v1/k", SHA256: sum, Encoding: "full", BirthLSN: 7}}
	body, _, err := encodeSegmentV2(0, 4096, "v1", refs)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(body, uint8(2))
	legacy, err := json.Marshal(legacySegmentDoc{Format: 1, Range: 0, ChunkBytes: 4096, Owner: "v1", Chunks: map[uint64]legacyChunkRefDoc{0: {Key: "@v1/k", SHA256: sum}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(legacy, uint8(1))
	f.Add([]byte(`{"format":2,"format":1}`), uint8(2))
	f.Add([]byte(`{"format":3}`), uint8(3))
	f.Fuzz(func(t *testing.T, body []byte, version uint8) {
		ref := storage.ChunkSegment{SegFormat: int(version), MaxBirthLSN: 7}
		got, err := decodeSegment(body, ref, 7)
		if err != nil {
			return
		}
		if got.Format != 1 && got.Format != 2 {
			t.Fatalf("accepted format %d", got.Format)
		}
		for _, cr := range got.Chunks {
			if cr.Encoding != "full" || cr.BirthLSN > 7 {
				t.Fatalf("accepted unsupported chunk metadata: %+v", cr)
			}
		}
		if got.Format == 2 {
			encoded, maxBirth, err := encodeSegmentV2(got.Range, got.ChunkBytes, got.Owner, got.Chunks)
			if err != nil {
				t.Fatal(err)
			}
			ref.MaxBirthLSN = maxBirth
			again, err := decodeSegment(encoded, ref, 7)
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatalf("round trip: %+v / %+v: %v", got, again, err)
			}
		}
	})
}
