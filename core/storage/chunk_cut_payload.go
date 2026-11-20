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

func (l *chunkLayerImpl) stagePayloadRound(ctx context.Context, batch []uint64, versions map[uint64]uint64, head chunkHead, st *chunkCutStats) (map[uint64]stagedChunk, string, error) {
	keys := make([]string, len(batch))
	for i, idx := range batch {
		keys[i] = l.newKey(idx)
	}
	// Reserve every possible binding before the first worker uploads. Unused
	// keys are harmless intent roots and disappear when this cut settles.
	intent, err := l.remote.PutIntent(ctx, chunkIntent{Gen: head.Gen, Epoch: l.cfg.Epoch, Holder: l.cfg.Holder, Keys: keys})
	if err != nil {
		return nil, "", fmt.Errorf("storage: capture stage intent: %w: %w", errChunkRemoteUnavailable, err)
	}
	out := make([]stagedChunk, len(batch))
	err = l.uploadBatch(ctx, len(batch), func(ctx context.Context, i int) error {
		idx := batch[i]
		data, err := l.readChunk(idx)
		if err != nil {
			return err
		}
		if hook := l.testAfterStageReadHook; hook != nil {
			hook(idx)
		}
		c := capturedChunk{idx: idx, sum: sha256.Sum256(data), zero: isAllZero(data)}
		ref, had := head.Chunks[idx]
		if !c.zero && !(had && ref.SHA256 == c.sum) {
			if err := l.putCaptured(ctx, keys[i], data); err != nil {
				return err
			}
			c.key, c.size = keys[i], len(data)
		}
		out[i] = stagedChunk{capturedChunk: c, ver: versions[idx], headGen: head.Gen}
		return nil
	})
	if err != nil {
		return nil, intent, fmt.Errorf("storage: capture stage upload: %w: %w", errChunkRemoteUnavailable, err)
	}
	chunks := make(map[uint64]stagedChunk, len(batch))
	for _, c := range out {
		chunks[c.idx] = c
		if c.key != "" {
			st.UploadedChunks++
			st.UploadedBytes += int64(c.size)
		}
	}
	return chunks, intent, nil
}
