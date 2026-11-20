package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
)

// DefaultCutPayloadMemoryBudget bounds engine-owned payload buffers, including
// saved-before-write bytes. Metadata and filesystem page cache are separate.
const DefaultCutPayloadMemoryBudget int64 = 64 << 20

// One uploader owns its local image, an optional pinned-HEAD fetch and its
// completed image, or a binding verification read. Reserve four chunk buffers
// per uploader before admitting savers. No payload queue or whole-round copy.
// (4*uploadWorkers + 2*savers)*chunkBytes + cachedSaves <= memoryLimit.
// Oversized chunks scale this bound, never candidate count or volume size.
func (l *chunkLayerImpl) cutPayloadWorkers() (uploadWorkers int, reserved, limit int64) {
	size := int64(l.blockSize) * int64(l.chunkBlocks)
	uploadWorkers = min(l.cfg.UploadConcurrency, max(1, int(DefaultCutPayloadMemoryBudget/(8*size))))
	reserved = int64(4*uploadWorkers) * size
	limit = max(DefaultCutPayloadMemoryBudget, reserved)
	return
}

// Recent LSNs approximate overwrite heat. Ties are deterministic; BASE/UNKNOWN
// chunks still participate even when their recency is not known.
func hotChunkKeys(indices map[uint64]bool, recent map[uint64]uint64) []uint64 {
	keys := sortedKeys(indices)
	sort.SliceStable(keys, func(i, j int) bool { return recent[keys[i]] > recent[keys[j]] })
	return keys
}

// Each wave owns at most one image per uploader. Classify that bounded set,
// durably name only its changed keys, then upload and discard its buffers.
// Later waves remain protected by COW; no whole-round payload is retained.
func (l *chunkLayerImpl) streamCutPayloads(ctx context.Context, batch []uint64, head chunkHead, read func(context.Context, uint64) (capturedChunk, error)) ([]capturedChunk, []string, error) {
	workers, _, _ := l.cutPayloadWorkers()
	out := make([]capturedChunk, 0, len(batch))
	var intents []string
	for start := 0; start < len(batch); start += workers {
		order := batch[start:min(len(batch), start+workers)]
		wave := make([]capturedChunk, len(order))
		err := l.uploadBatch(ctx, len(order), func(ctx context.Context, i int) error {
			c, err := read(ctx, order[i])
			if err != nil {
				return err
			}
			ref, had := head.Chunks[c.idx]
			if !c.zero && !(had && ref.SHA256 == c.sum) {
				c.key = l.newKey(c.idx)
				c.size = len(c.data)
			}
			wave[i] = c
			return nil
		})
		if err != nil {
			return nil, intents, err
		}
		var keys []string
		var changed []int
		for i, c := range wave {
			if c.key != "" {
				keys = append(keys, c.key)
				changed = append(changed, i)
			}
		}
		if len(keys) > 0 {
			id, err := l.remote.PutIntent(ctx, chunkIntent{Gen: head.Gen, Epoch: l.cfg.Epoch, Holder: l.cfg.Holder, Keys: keys})
			if err != nil {
				return nil, intents, fmt.Errorf("storage: capture intent: %w: %w", errChunkRemoteUnavailable, err)
			}
			intents = append(intents, id)
			if err = l.uploadBatch(ctx, len(changed), func(ctx context.Context, i int) error {
				c := wave[changed[i]]
				return l.putCaptured(ctx, c.key, c.data)
			}); err != nil {
				return nil, intents, fmt.Errorf("storage: capture upload: %w: %w", errChunkRemoteUnavailable, err)
			}
		}
		for i := range wave {
			wave[i].data = nil
			out = append(out, wave[i])
		}
	}
	return out, intents, nil
}

func (l *chunkLayerImpl) stagePayloadRound(ctx context.Context, batch []uint64, versions map[uint64]uint64, head chunkHead, st *chunkCutStats) (map[uint64]stagedChunk, []string, error) {
	out, intents, err := l.streamCutPayloads(ctx, batch, head, func(ctx context.Context, idx uint64) (capturedChunk, error) {
		data, err := l.readChunk(idx)
		if err != nil {
			return capturedChunk{}, err
		}
		if hook := l.testAfterStageReadHook; hook != nil {
			hook(idx)
		}
		return capturedChunk{idx: idx, data: data, sum: sha256.Sum256(data), zero: isAllZero(data)}, nil
	})
	if err != nil {
		return nil, intents, err
	}
	chunks := make(map[uint64]stagedChunk, len(out))
	for _, c := range out {
		chunks[c.idx] = stagedChunk{capturedChunk: c, ver: versions[c.idx], headGen: head.Gen}
		if c.key != "" {
			st.UploadedChunks++
			st.UploadedBytes += int64(c.size)
		}
	}
	return chunks, intents, nil
}
