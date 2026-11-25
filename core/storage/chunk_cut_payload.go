package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// DefaultCutPayloadMemoryBudget bounds engine-owned payload buffers, including
// saved-before-write bytes. Metadata and filesystem page cache are separate.
const DefaultCutPayloadMemoryBudget int64 = 64 << 20

// One uploader owns its local image, an optional pinned-HEAD fetch and its
// completed image, or a binding verification read. Reserve four chunk buffers
// per uploader before admitting savers. No payload queue or whole-round copy.
// (4*uploadWorkers + 2*savers)*chunkBytes + cachedSaves <= memoryLimit.
// The configured bound is independent of candidate count and volume size.
func (l *chunkLayerImpl) cutPayloadWorkers() (uploadWorkers int, reserved, limit int64) {
	size := int64(l.blockSize) * int64(l.chunkBlocks)
	limit = l.cfg.PayloadMemoryBudget
	if limit == 0 {
		limit = max(DefaultCutPayloadMemoryBudget, 6*size)
	}
	uploadWorkers = min(l.cfg.UploadConcurrency, max(1, int(limit/(8*size))))
	reserved = int64(4*uploadWorkers) * size
	return
}

// Recent LSNs approximate overwrite heat. Ties are deterministic; BASE/UNKNOWN
// chunks still participate even when their recency is not known.
func hotChunkKeys(indices map[uint64]bool, recent map[uint64]uint64) []uint64 {
	keys := sortedKeys(indices)
	sort.SliceStable(keys, func(i, j int) bool { return recent[keys[i]] > recent[keys[j]] })
	return keys
}

// Preallocate metadata keys once per round. Each worker owns one immutable
// image through PUT/retry, then reads its next candidate independently of slow
// workers. One durable intent covers all candidate keys before the first PUT;
// unused keys are harmless and retain no payload memory.
//
// staging says the batch is a staging round (before the barrier): each chunk
// it finishes is counted in Stats.StagedChunks as it finishes.
func (l *chunkLayerImpl) streamCutPayloads(ctx context.Context, batch []uint64, head chunkHead, staging bool, read func(context.Context, uint64) (capturedChunk, error)) ([]capturedChunk, []string, error) {
	out := make([]capturedChunk, len(batch))
	keys := make([]string, len(batch))
	for i, idx := range batch {
		keys[i] = l.newKey(idx)
	}
	var intents []string
	var once sync.Once
	var intent string
	var intentErr error
	put := l.beginPutRound(len(batch))
	err := l.uploadBatch(ctx, len(batch), func(ctx context.Context, i int) error {
		c, err := read(ctx, batch[i])
		if err != nil {
			return err
		}
		ref, had := head.Chunks[c.idx]
		if !c.zero && !(had && ref.SHA256 == c.sum && !l.owning(c.idx)) {
			c.key, c.size = l.keyFor(c.idx, c.sum, keys[i]), len(c.data)
			once.Do(func() {
				intent, intentErr = l.remote.PutIntent(ctx, chunkIntent{Gen: head.Gen, Epoch: l.cfg.Epoch, Holder: l.cfg.Holder, Keys: keys})
				if intentErr == nil {
					intents = append(intents, intent)
				}
			})
			if intentErr != nil {
				return fmt.Errorf("storage: capture intent: %w: %w", errChunkRemoteUnavailable, intentErr)
			}
			c.intent = intent
			if err := l.putCapturedWith(ctx, c.key, c.data, func(ctx context.Context) error { return put(ctx, i, c.key, c.data) }); err != nil {
				return fmt.Errorf("storage: capture upload: %w", errors.Join(errChunkRemoteUnavailable, errCutUpload, err))
			}
		}
		l.rememberPayload(c, head.Gen, staging)
		c.data = nil
		out[i] = c
		return nil
	})
	if err != nil {
		return nil, intents, err
	}
	return out, intents, nil
}

func (l *chunkLayerImpl) stagePayloadRound(ctx context.Context, batch []uint64, versions map[uint64]uint64, head chunkHead, st *chunkCutStats) (map[uint64]stagedChunk, []string, error) {
	out, intents, err := l.streamCutPayloads(ctx, batch, head, true, func(ctx context.Context, idx uint64) (capturedChunk, error) {
		data, err := l.readChunk(idx)
		if err != nil {
			return capturedChunk{}, err
		}
		if hook := l.testAfterStageReadHook; hook != nil {
			hook(idx)
		}
		return capturedChunk{idx: idx, version: versions[idx], data: data, sum: sha256.Sum256(data), zero: isAllZero(data)}, nil
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

// Only immutable metadata survives an attempt. The intent remains a GC root
// until this version is superseded or a confirmed HEAD makes it reachable.
// This cache belongs to one layer/owner lifetime. Generation equality is not
// a reuse proof across reopen or takeover; those require content-hash matching.
// A staged chunk is counted in Stats.StagedChunks here, when it is
// remembered, so an interrupted round counts what it finished.
func (l *chunkLayerImpl) rememberPayload(c capturedChunk, gen uint64, staged bool) {

	c.data = nil
	l.stMu.Lock()
	defer l.stMu.Unlock()
	if staged {
		l.stats.StagedChunks++
	}
	if l.reusable == nil {
		l.reusable = make(map[uint64]stagedChunk)
	}
	l.reusable[c.idx] = stagedChunk{capturedChunk: c, ver: c.version, headGen: gen}
}
