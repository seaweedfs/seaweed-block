package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func (c *Catalog) prepareLegacyHead(ctx context.Context, root string, base, next storage.ChunkHead, geo Geometry) (storage.ChunkHead, []string, error) {
	if next.Segments != nil || len(next.Chunks) == 0 {
		return next, nil, nil
	}
	if c.blobs == nil {
		return next, nil, errors.New("chunkstore: legacy data upgrade needs blob storage")
	}
	r, err := NewChunkRemote(c.f, c.blobs, root)
	if err != nil {
		return next, nil, err
	}
	return r.PrepareManifest(ctx, base, next, uint64(geo.BlockSize)*uint64(geo.ChunkBlocks))
}
func (c *Catalog) dropManifestIntents(ctx context.Context, root string, ids []string) {
	for _, id := range ids {
		c.dropIntent(ctx, root, id)
	}
}

// Fork identity is a persisted protocol. Old snapshots keep the pre-format-3
// canonical encoding even when their next publication upgrades the manifest.
func snapshotHeadDigest(h storage.ChunkHead, embedded json.RawMessage) (string, error) {
	var version struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(embedded, &version); err != nil {
		return "", err
	}
	if version.Format == 3 {
		return headDigest(h)
	}
	type oldHead struct {
		Segments   map[uint64]legacySegmentRefDoc `json:"segments,omitempty"`
		ChunkBytes uint64                         `json:"chunk_bytes,omitempty"`
		ChunkCount int                            `json:"chunk_count,omitempty"`
		Format     int                            `json:"format"`
		Gen        uint64                         `json:"gen"`
		Epoch      uint64                         `json:"epoch"`
		Holder     string                         `json:"holder"`
		RemoteLSN  uint64                         `json:"remote_lsn"`
		Chunks     map[uint64]refDoc              `json:"chunks,omitempty"`
		State      string                         `json:"state,omitempty"`
		DetachedBy *detachedByDoc                 `json:"detachedBy,omitempty"`
	}
	d := oldHead{Format: version.Format, Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN, State: h.State}
	if h.DetachedBy != nil {
		d.DetachedBy = &detachedByDoc{Holder: h.DetachedBy.Holder, Epoch: h.DetachedBy.Epoch, Gen: h.DetachedBy.Gen}
	}
	if version.Format == 2 {
		d.Segments = map[uint64]legacySegmentRefDoc{}
		d.ChunkBytes = h.ChunkBytes
		d.ChunkCount = h.ManifestChunkCount
		for idx, ref := range h.Segments {
			d.Segments[idx] = legacySegmentRefDoc{Key: ref.Key, SHA256: ref.SHA256, Owner: ref.Owner, Count: ref.Count}
		}
	} else {
		d.Chunks = map[uint64]refDoc{}
		for idx, ref := range h.Chunks {
			d.Chunks[idx] = refDoc{Key: ref.Key, SHA256: hex.EncodeToString(ref.SHA256[:])}
		}
	}
	body, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
