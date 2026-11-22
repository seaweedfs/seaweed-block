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
	Format        int            `json:"format"`
	Gen           uint64         `json:"gen"`
	Epoch         uint64         `json:"epoch"`
	Holder        string         `json:"holder"`
	HolderRoot    string         `json:"holder_root,omitempty"`
	HolderRootGen uint64         `json:"holder_root_gen,omitempty"`
	RemoteLSN     uint64         `json:"remote_lsn"`
	State         string         `json:"state,omitempty"`
	DetachedBy    *detachedByDoc `json:"detachedBy,omitempty"`
}
type flatHeadDoc struct {
	headIdentityDoc
	Chunks map[uint64]refDoc `json:"chunks"`
}
type segmentedHeadDoc struct {
	headIdentityDoc
	Segments   map[uint64]legacySegmentRefDoc `json:"segments"`
	ChunkBytes uint64                         `json:"chunk_bytes"`
	ChunkCount int                            `json:"chunk_count"`
}

type legacySegmentRefDoc struct {
	Key    string   `json:"key"`
	SHA256 [32]byte `json:"sha256"`
	Owner  string   `json:"owner"`
	Count  int      `json:"count"`
}

type segmentRefV3Doc struct {
	legacySegmentRefDoc
	MaxBirthLSN *uint64 `json:"max_birth_lsn"`
	SegFormat   int     `json:"seg_format"`
}

type levelHeadDoc struct {
	headIdentityDoc
	Levels     int                        `json:"levels"`
	Segments   map[uint64]segmentRefV3Doc `json:"segments"`
	ChunkBytes uint64                     `json:"chunk_bytes"`
	ChunkCount int                        `json:"chunk_count"`
}

func (d headIdentityDoc) document() headDoc {
	return headDoc{Format: d.Format, Gen: d.Gen, Epoch: d.Epoch, Holder: d.Holder, HolderRoot: d.HolderRoot, HolderRootGen: d.HolderRootGen, RemoteLSN: d.RemoteLSN, State: d.State, DetachedBy: d.DetachedBy}
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
		doc.Segments = make(map[uint64]storage.ChunkSegment, len(segmented.Segments))
		for group, ref := range segmented.Segments {
			doc.Segments[group] = storage.ChunkSegment{Key: ref.Key, SHA256: ref.SHA256, Owner: ref.Owner, Count: ref.Count, SegFormat: 1, MaxBirthLSN: doc.RemoteLSN}
		}
		doc.ChunkBytes, doc.ChunkCount = segmented.ChunkBytes, segmented.ChunkCount
		return doc, nil
	case 3:
		var root levelHeadDoc
		if err := decoder.Decode(&root); err != nil {
			return headDoc{}, err
		}
		if root.Levels != 1 {
			return headDoc{}, fmt.Errorf("chunkstore: unsupported levels %d", root.Levels)
		}
		doc := root.headIdentityDoc.document()
		doc.Levels, doc.ChunkBytes, doc.ChunkCount = root.Levels, root.ChunkBytes, root.ChunkCount
		doc.Segments = make(map[uint64]storage.ChunkSegment, len(root.Segments))
		for group, ref := range root.Segments {
			if ref.MaxBirthLSN == nil || (ref.SegFormat != 1 && ref.SegFormat != 2) || *ref.MaxBirthLSN > root.RemoteLSN {
				return headDoc{}, fmt.Errorf("chunkstore: invalid segment version/birth maximum")
			}
			doc.Segments[group] = storage.ChunkSegment{Key: ref.Key, SHA256: ref.SHA256, Owner: ref.Owner, Count: ref.Count, SegFormat: ref.SegFormat, MaxBirthLSN: *ref.MaxBirthLSN}
		}
		return doc, nil
	default:
		return headDoc{}, fmt.Errorf("chunkstore: unsupported HEAD format %d", discriminator.Format)
	}
}
