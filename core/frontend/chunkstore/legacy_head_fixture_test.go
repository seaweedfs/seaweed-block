package chunkstore

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Only old-reader fixtures bypass the production format-1 data-write refusal.
func legacyHeadFixture(t testing.TB, h storage.ChunkHead) *filerpb.Entry {
	t.Helper()
	doc := headDoc{Format: 1, Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN, State: h.State, Chunks: map[uint64]refDoc{}}
	for idx, ref := range h.Chunks {
		doc.Chunks[idx] = refDoc{Key: ref.Key, SHA256: hex.EncodeToString(ref.SHA256[:])}
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return &filerpb.Entry{Name: headName, Content: body, Extended: map[string][]byte{extKind: []byte(kindChunkHead), extGen: []byte(strconv.FormatUint(h.Gen, 10)), extEpoch: []byte(strconv.FormatUint(h.Epoch, 10)), extHolder: []byte(h.Holder)}}
}
