package chunkstore

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Only identity/proof fields are shared between persisted representations.
// Format-specific structs deliberately have no field for the other format:
// DisallowUnknownFields rejects even encoding/json's Unicode-folded aliases.
type headIdentityDoc struct {
	Format     int            `json:"format"`
	Gen        uint64         `json:"gen"`
	Epoch      uint64         `json:"epoch"`
	Holder     string         `json:"holder"`
	RemoteLSN  uint64         `json:"remote_lsn"`
	State      string         `json:"state,omitempty"`
	DetachedBy *detachedByDoc `json:"detachedBy,omitempty"`
}
type flatHeadDoc struct {
	headIdentityDoc
	Chunks map[uint64]refDoc `json:"chunks"`
}
type segmentedHeadDoc struct {
	headIdentityDoc
	Segments   map[uint64]storage.ChunkSegment `json:"segments"`
	ChunkBytes uint64                          `json:"chunk_bytes"`
	ChunkCount int                             `json:"chunk_count"`
}

func (d headIdentityDoc) document() headDoc {
	return headDoc{Format: d.Format, Gen: d.Gen, Epoch: d.Epoch, Holder: d.Holder, RemoteLSN: d.RemoteLSN, State: d.State, DetachedBy: d.DetachedBy}
}

func decodeFormatHead(content []byte) (headDoc, error) {
	var discriminator struct {
		Format int `json:"format"`
	}
	// Preserve duplicate/nesting/byte/trailing-byte checks before selecting the
	// schema. Unknown fields are allowed ONLY for this discriminator read.
	if err := decodeManifestJSON(content, &discriminator); err != nil {
		return headDoc{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	switch discriminator.Format {
	case 1:
		var flat flatHeadDoc
		if err := decoder.Decode(&flat); err != nil {
			return headDoc{}, err
		}
		doc := flat.headIdentityDoc.document()
		doc.Chunks = flat.Chunks
		return doc, nil
	case 2:
		var segmented segmentedHeadDoc
		if err := decoder.Decode(&segmented); err != nil {
			return headDoc{}, err
		}
		doc := segmented.headIdentityDoc.document()
		doc.Segments, doc.ChunkBytes, doc.ChunkCount = segmented.Segments, segmented.ChunkBytes, segmented.ChunkCount
		return doc, nil
	default:
		return headDoc{}, fmt.Errorf("chunkstore: unsupported HEAD format %d", discriminator.Format)
	}
}
