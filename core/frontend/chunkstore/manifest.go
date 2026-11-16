package chunkstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// One segment spans a fixed logical GiB. This is a format constant, not a knob.
const manifestSegmentBytes uint64 = 1 << 30
const maxManifestBytes = 64 << 20

type segmentDoc struct {
	Format     int                         `json:"format"`
	Range      uint64                      `json:"range"`
	ChunkBytes uint64                      `json:"chunk_bytes"`
	Owner      string                      `json:"owner"`
	Chunks     map[uint64]storage.ChunkRef `json:"chunks"`
}

func qualifyManifestKey(owner, key string) string {
	if strings.HasPrefix(key, "@") {
		return key
	}
	return "@" + owner + "/" + key
}

func manifestCount(h storage.ChunkHead) int {
	if h.Segments != nil {
		return h.ManifestChunkCount
	}
	return len(h.Chunks)
}

func validateRoot(h storage.ChunkHead) error {
	if h.ChunkBytes < 4096 || h.ChunkBytes > manifestSegmentBytes || manifestSegmentBytes%h.ChunkBytes != 0 || h.ManifestChunkCount < 0 {
		return errors.New("chunkstore: invalid manifest geometry/count")
	}
	count := 0
	for group, ref := range h.Segments {
		if CheckVolumeID(ref.Owner) != nil || storage.ValidChunkKey(ref.Key) != nil || ref.Count <= 0 || uint64(ref.Count) > manifestSegmentBytes/h.ChunkBytes || group > ^uint64(0)/(manifestSegmentBytes/h.ChunkBytes) {
			return errors.New("chunkstore: invalid segment reference")
		}
		count += ref.Count
	}
	if count != h.ManifestChunkCount {
		return errors.New("chunkstore: manifest count mismatch")
	}
	return nil
}

// PrepareManifest never changes generation/epoch or publishes the root. All
// segment keys are protected before their Put; the cut owns returned intents.
func (r *ChunkRemote) PrepareManifest(ctx context.Context, base, next storage.ChunkHead, chunkBytes uint64) (storage.ChunkHead, []string, error) {
	next.ChunkBytes = chunkBytes
	next.ManifestChunkCount = len(next.Chunks)
	next.Segments = map[uint64]storage.ChunkSegment{}
	if chunkBytes < 4096 || chunkBytes > manifestSegmentBytes || manifestSegmentBytes%chunkBytes != 0 {
		return next, nil, errors.New("chunkstore: invalid segment geometry")
	}
	per := manifestSegmentBytes / chunkBytes
	owner := path.Base(r.root)
	groups := map[uint64]map[uint64]storage.ChunkRef{}
	canonical := make(map[uint64]storage.ChunkRef, len(next.Chunks))
	for idx, ref := range next.Chunks {
		ref.Key = qualifyManifestKey(owner, ref.Key)
		canonical[idx] = ref
		group := idx / per
		if groups[group] == nil {
			groups[group] = map[uint64]storage.ChunkRef{}
		}
		groups[group][idx] = ref
	}
	next.Chunks = canonical
	type upload struct {
		key  string
		data []byte
	}
	var uploads []upload
	for group, chunks := range groups {
		old := map[uint64]storage.ChunkRef{}
		for idx, ref := range base.Chunks {
			if idx/per == group {
				ref.Key = qualifyManifestKey(owner, ref.Key)
				old[idx] = ref
			}
		}
		if previous, ok := base.Segments[group]; ok && base.ChunkBytes == chunkBytes && reflect.DeepEqual(chunks, old) {
			next.Segments[group] = previous
			continue
		}
		body, err := json.Marshal(segmentDoc{1, group, chunkBytes, owner, chunks})
		if err != nil {
			return next, nil, err
		}
		if len(body) > maxManifestBytes {
			return next, nil, errors.New("chunkstore: manifest too large")
		}
		nonce := make([]byte, 16)
		if _, err := rand.Read(nonce); err != nil {
			return next, nil, err
		}
		key := fmt.Sprintf("manifests/%x", nonce)
		next.Segments[group] = storage.ChunkSegment{Key: qualifyManifestKey(owner, key), SHA256: sha256.Sum256(body), Owner: owner, Count: len(chunks)}
		uploads = append(uploads, upload{key, body})
	}
	if err := validateRoot(next); err != nil {
		return next, nil, err
	}
	if len(uploads) == 0 {
		return next, nil, nil
	}
	keys := make([]string, len(uploads))
	for i, u := range uploads {
		keys[i] = u.key
	}
	id, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: base.Gen, Epoch: next.Epoch, Holder: next.Holder, Keys: keys})
	if err != nil {
		return next, nil, err
	}
	ids := []string{id}
	for _, u := range uploads {
		if err := r.Put(ctx, u.key, u.data); err != nil {
			return next, ids, err
		}
	}
	return next, ids, nil
}

func (r *ChunkRemote) resolveManifest(ctx context.Context, h storage.ChunkHead) (storage.ChunkHead, error) {
	if h.Segments == nil {
		return h, nil
	}
	if err := validateRoot(h); err != nil {
		return h, err
	}
	h.Chunks = map[uint64]storage.ChunkRef{}
	for group, ref := range h.Segments {
		body, err := r.Get(ctx, ref.Key)
		if err != nil {
			return h, fmt.Errorf("chunkstore: read published segment: %w", err)
		}
		if len(body) > maxManifestBytes || sha256.Sum256(body) != ref.SHA256 {
			return h, errors.New("chunkstore: segment hash/size mismatch")
		}
		var doc segmentDoc
		if err := json.Unmarshal(body, &doc); err != nil {
			return h, err
		}
		if doc.Format != 1 || doc.Range != group || doc.ChunkBytes != h.ChunkBytes || doc.Owner != ref.Owner || len(doc.Chunks) != ref.Count {
			return h, errors.New("chunkstore: invalid segment contents")
		}
		for idx, cr := range doc.Chunks {
			if idx/(manifestSegmentBytes/h.ChunkBytes) != group || storage.ValidChunkKey(cr.Key) != nil {
				return h, errors.New("chunkstore: segment index/key invalid")
			}
			cr.Key = qualifyManifestKey(ref.Owner, cr.Key)
			h.Chunks[idx] = cr
		}
	}
	return h, nil
}
