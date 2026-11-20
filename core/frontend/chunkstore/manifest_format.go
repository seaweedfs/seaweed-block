package chunkstore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type segmentChunkV2Doc struct {
	Key string `json:"key"`
	// SHA256 covers stored object bytes, before any encoding is decoded.
	SHA256   string  `json:"sha256"`
	Encoding string  `json:"enc"`
	BirthLSN *uint64 `json:"birth_lsn"`
}

type segmentV2Doc struct {
	Format      int                          `json:"format"`
	Range       uint64                       `json:"range"`
	ChunkBytes  uint64                       `json:"chunk_bytes"`
	Owner       string                       `json:"owner"`
	MaxBirthLSN *uint64                      `json:"max_birth_lsn"`
	Chunks      map[uint64]segmentChunkV2Doc `json:"chunks"`
}

type legacyChunkRefDoc struct {
	Key    string
	SHA256 [32]byte
}

type legacySegmentDoc struct {
	Format     int                          `json:"format"`
	Range      uint64                       `json:"range"`
	ChunkBytes uint64                       `json:"chunk_bytes"`
	Owner      string                       `json:"owner"`
	Chunks     map[uint64]legacyChunkRefDoc `json:"chunks"`
}

func encodeSegmentV2(group, chunkBytes uint64, owner string, chunks map[uint64]storage.ChunkRef) ([]byte, uint64, error) {
	doc := segmentV2Doc{Format: 2, Range: group, ChunkBytes: chunkBytes, Owner: owner, Chunks: map[uint64]segmentChunkV2Doc{}}
	var maxBirth uint64
	for idx, ref := range chunks {
		birth := ref.BirthLSN
		doc.Chunks[idx] = segmentChunkV2Doc{Key: ref.Key, SHA256: hex.EncodeToString(ref.SHA256[:]), Encoding: ref.Encoding, BirthLSN: &birth}
		maxBirth = max(maxBirth, birth)
	}
	doc.MaxBirthLSN = &maxBirth
	body, err := json.Marshal(doc)
	return body, maxBirth, err
}

func decodeSegment(body []byte, ref storage.ChunkSegment, remoteLSN uint64) (segmentDoc, error) {
	var discriminator struct {
		Format int `json:"format"`
	}
	if err := decodeManifestJSON(body, &discriminator); err != nil {
		return segmentDoc{}, err
	}
	if discriminator.Format != ref.SegFormat {
		return segmentDoc{}, errors.New("chunkstore: segment format/reference mismatch")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	result := segmentDoc{Chunks: map[uint64]storage.ChunkRef{}}
	switch discriminator.Format {
	case 1:
		var doc legacySegmentDoc
		if err := d.Decode(&doc); err != nil {
			return result, err
		}
		result.Format, result.Range, result.ChunkBytes, result.Owner = doc.Format, doc.Range, doc.ChunkBytes, doc.Owner
		for idx, cr := range doc.Chunks {
			result.Chunks[idx] = storage.ChunkRef{Key: cr.Key, SHA256: cr.SHA256, Encoding: "full", BirthLSN: ref.MaxBirthLSN}
		}
	case 2:
		var doc segmentV2Doc
		if err := d.Decode(&doc); err != nil {
			return result, err
		}
		if doc.MaxBirthLSN == nil || *doc.MaxBirthLSN != ref.MaxBirthLSN || *doc.MaxBirthLSN > remoteLSN {
			return result, errors.New("chunkstore: segment birth maximum mismatch")
		}
		result.Format, result.Range, result.ChunkBytes, result.Owner = doc.Format, doc.Range, doc.ChunkBytes, doc.Owner
		var maxBirth uint64
		for idx, cr := range doc.Chunks {
			sum, err := hex.DecodeString(cr.SHA256)
			if err != nil || len(sum) != 32 || cr.Encoding != "full" || cr.BirthLSN == nil || *cr.BirthLSN > remoteLSN {
				return result, errors.New("chunkstore: invalid segment chunk encoding/hash/birth")
			}
			v := storage.ChunkRef{Key: cr.Key, Encoding: cr.Encoding, BirthLSN: *cr.BirthLSN}
			copy(v.SHA256[:], sum)
			result.Chunks[idx] = v
			maxBirth = max(maxBirth, v.BirthLSN)
		}
		if maxBirth != *doc.MaxBirthLSN {
			return result, errors.New("chunkstore: incorrect segment birth maximum")
		}
	default:
		return result, errors.New("chunkstore: unsupported segment format")
	}
	return result, nil
}
