package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// DefaultCutPayloadMemoryBudget bounds engine-owned payload buffers, including
// saved-before-write bytes. Metadata and filesystem page cache are separate.
const DefaultCutPayloadMemoryBudget int64 = 64 << 20

// A copier can own the local image, a missing-HEAD fetch and its completed
// image, plus a local read buffer. An uploader can own its image and a retry
// verification read. Reserve these BEFORE admitting any saver:
// (4*copyWorkers + 2*uploadWorkers)*chunkBytes + savedBytes <= memoryLimit.
// The default 4 MiB geometry uses 1 copier, 2 uploaders and <=32 MiB saves.
// Oversized chunks require at least one worker; their explicit bound scales
// with chunk size, never with candidate count or volume size. No payload queue.
func (l *chunkLayerImpl) cutPayloadWorkers() (copyWorkers, uploadWorkers int, reserved, limit int64) {
	size := int64(l.blockSize) * int64(l.chunkBlocks)
	copyWorkers = min(l.cfg.UploadConcurrency, max(1, int(DefaultCutPayloadMemoryBudget/(16*size))))
	uploadWorkers = min(l.cfg.UploadConcurrency, max(1, int(DefaultCutPayloadMemoryBudget/(8*size))))
	reserved = int64(4*copyWorkers+2*uploadWorkers) * size
	limit = max(DefaultCutPayloadMemoryBudget, reserved)
	return
}

// cutPayloadStore owns immutable local copies through upload settlement. Its
// caller joins all copy/upload workers before Close. Files are private scratch
// data: a crash discards them, never publishes them or reads them as recovery.
// No fsync is needed: HEAD remains unchanged until remote objects are durable.
type cutPayloadStore struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	bytes       int64
	limit       int64
	reserve     func(int64) error // shares the COW side-file cap when attached
	beforeWrite func()
}

func (l *chunkLayerImpl) newCutPayloadStore() *cutPayloadStore {
	return &cutPayloadStore{path: l.s.path, limit: l.cfg.COWSpillLimit, beforeWrite: l.testPayloadWrite}
}

func (p *cutPayloadStore) keep(c capturedChunk, head map[uint64]chunkRef) (capturedChunk, error) {
	ref, had := head[c.idx]
	if len(c.missing) == 0 && (c.zero || (had && c.sum == ref.SHA256)) {
		c.data = nil
		return c, nil
	}
	size := int64(len(c.data))
	p.mu.Lock()
	if p.reserve != nil {
		if err := p.reserve(size); err != nil {
			p.mu.Unlock()
			return capturedChunk{}, err
		}
	} else if size > p.limit-p.bytes {
		p.mu.Unlock()
		return capturedChunk{}, fmt.Errorf("%w: local cut payload cap %d bytes", errCutCOWAborted, p.limit)
	}
	if p.file == nil {
		f, err := os.CreateTemp(filepath.Dir(p.path), filepath.Base(p.path)+".cut-cow-payload-")
		if err != nil {
			p.mu.Unlock()
			return capturedChunk{}, err
		}
		p.file = f
	}
	f, offset := p.file, p.bytes
	p.bytes += size
	p.mu.Unlock()
	if p.beforeWrite != nil {
		p.beforeWrite()
	}
	n, err := f.WriteAt(c.data, offset)
	if err == nil && int64(n) != size {
		err = io.ErrShortWrite
	}
	if err != nil {
		return capturedChunk{}, err
	}
	c.offset, c.size, c.data = offset, int(size), nil
	return c, nil
}

// A pinned-HEAD fetch is deliberately after local owners have drained. A slow
// remote cannot prolong the window or make guest overwrites keep saving.
func (p *cutPayloadStore) complete(ctx context.Context, l *chunkLayerImpl, head map[uint64]chunkRef, c capturedChunk) (capturedChunk, error) {
	data, err := p.read(c)
	if err == nil {
		data, err = l.fillMissing(ctx, head, c.idx, data, c.missing)
	}
	if err != nil {
		return capturedChunk{}, errors.Join(errCutCOWAborted, err)
	}
	n, err := p.file.WriteAt(data, c.offset)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return capturedChunk{}, err
	}
	c.sum, c.zero, c.missing = sha256.Sum256(data), isAllZero(data), nil
	return c, nil
}

func (p *cutPayloadStore) read(c capturedChunk) ([]byte, error) {
	if c.data != nil {
		return c.data, nil
	} // explicit legacy test fixtures
	if p.file == nil || c.size <= 0 {
		return nil, fmt.Errorf("storage: missing cut payload for chunk %d", c.idx)
	}
	b := make([]byte, c.size)
	_, err := p.file.ReadAt(b, c.offset)
	return b, err
}

func (p *cutPayloadStore) Close() error {
	if p.file == nil {
		return nil
	}
	return errors.Join(p.file.Close(), os.Remove(p.file.Name()))
}

func (l *chunkLayerImpl) stagePayloadRound(ctx context.Context, batch []uint64, versions map[uint64]uint64, head chunkHead, st *chunkCutStats) (chunks map[uint64]stagedChunk, intent string, err error) {
	payloads := l.newCutPayloadStore()
	defer func() { err = errors.Join(err, payloads.Close()) }()
	chunks = make(map[uint64]stagedChunk, len(batch))
	var uploads []stagedChunk
	for _, idx := range batch {
		data, e := l.readChunk(idx)
		if e != nil {
			return nil, "", fmt.Errorf("storage: capture stage chunk %d: %w", idx, e)
		}
		if hook := l.testAfterStageReadHook; hook != nil {
			hook(idx)
		}
		c := capturedChunk{idx: idx, data: data, sum: sha256.Sum256(data), zero: isAllZero(data)}
		ref, had := head.Chunks[idx]
		changed := !c.zero && !(had && ref.SHA256 == c.sum)
		c, e = payloads.keep(c, head.Chunks)
		if e != nil {
			return nil, "", e
		}
		sc := stagedChunk{capturedChunk: c, ver: versions[idx], headGen: head.Gen}
		if changed {
			sc.key = l.newKey(idx)
			uploads = append(uploads, sc)
		} else {
			chunks[idx] = sc
		}
	}
	if len(uploads) == 0 {
		return chunks, "", nil
	}
	keys := make([]string, len(uploads))
	for i, u := range uploads {
		keys[i] = u.key
	}
	intent, err = l.remote.PutIntent(ctx, chunkIntent{Gen: head.Gen, Epoch: l.cfg.Epoch, Holder: l.cfg.Holder, Keys: keys})
	if err != nil {
		return nil, "", fmt.Errorf("storage: capture stage intent: %w: %w", errChunkRemoteUnavailable, err)
	}
	err = l.uploadBatch(ctx, len(uploads), func(ctx context.Context, i int) error {
		u := uploads[i]
		data, e := payloads.read(u.capturedChunk)
		if e != nil {
			return e
		}
		return l.putCaptured(ctx, u.key, data)
	})
	if err != nil {
		return nil, intent, fmt.Errorf("storage: capture stage upload: %w: %w", errChunkRemoteUnavailable, err)
	}
	for _, u := range uploads {
		st.UploadedChunks++
		st.UploadedBytes += int64(u.size)
		chunks[u.idx] = u
	}
	return chunks, intent, nil
}
