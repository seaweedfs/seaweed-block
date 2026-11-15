package storage

// FileChunkBitmapStore is the production chunkBitmapStore: one state file
// (header + dirty-since-HEAD chunks, with a SHA-256 over its body) replaced
// atomically on every change, plus an append-only delta log with one JSON
// record per closed HEAD period. Every method is durable on return.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileChunkBitmapStore struct {
	path string

	mu     sync.Mutex
	loaded bool
	stored bool
	hdr    chunkBitmapHeader
	dirty  []uint64
}

// NewFileChunkBitmapStore returns a store at path (the delta log is
// path + ".deltas").
func NewFileChunkBitmapStore(path string) *FileChunkBitmapStore {
	return &FileChunkBitmapStore{path: path}
}

type chunkBitmapFile struct {
	Version int               `json:"version"`
	Header  chunkBitmapHeader `json:"header"`
	Dirty   []uint64          `json:"dirty"`
}

type chunkBitmapEnvelope struct {
	Body   json.RawMessage `json:"body"`
	SHA256 string          `json:"sha256"`
}

func (b *FileChunkBitmapStore) loadLocked() error {
	if b.loaded {
		return nil
	}
	b.loaded = true
	raw, err := os.ReadFile(b.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: chunk bitmap read: %w", err)
	}
	var env chunkBitmapEnvelope
	if json.Unmarshal(raw, &env) != nil {
		return nil // corrupt: treated as lost (hash-compare fallback)
	}
	sum := sha256.Sum256(env.Body)
	if hex.EncodeToString(sum[:]) != env.SHA256 {
		return nil
	}
	var f chunkBitmapFile
	if json.Unmarshal(env.Body, &f) != nil || f.Version != 1 {
		return nil
	}
	b.stored, b.hdr, b.dirty = true, f.Header, f.Dirty
	return nil
}

func (b *FileChunkBitmapStore) writeLocked(hdr chunkBitmapHeader, dirty []uint64) error {
	body, err := json.Marshal(chunkBitmapFile{Version: 1, Header: hdr, Dirty: dirty})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	raw, err := json.Marshal(chunkBitmapEnvelope{Body: body, SHA256: hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	if err := writeFileDurable(b.path, raw); err != nil {
		return fmt.Errorf("storage: chunk bitmap write: %w", err)
	}
	// Durable on return includes the rename itself.
	if err := syncDir(filepath.Dir(b.path)); err != nil {
		return fmt.Errorf("storage: chunk bitmap: fsync directory: %w", err)
	}
	b.stored, b.hdr, b.dirty = true, hdr, append([]uint64(nil), dirty...)
	return nil
}

func (b *FileChunkBitmapStore) Load() (chunkBitmapHeader, []uint64, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.loadLocked(); err != nil {
		return chunkBitmapHeader{}, nil, false, err
	}
	if !b.stored {
		return chunkBitmapHeader{}, nil, false, nil
	}
	return b.hdr, append([]uint64(nil), b.dirty...), true, nil
}

func (b *FileChunkBitmapStore) Save(dirty []uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.loadLocked(); err != nil {
		return err
	}
	return b.writeLocked(b.hdr, dirty)
}

func (b *FileChunkBitmapStore) WriteHeader(hdr chunkBitmapHeader) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.loadLocked(); err != nil {
		return err
	}
	return b.writeLocked(hdr, b.dirty)
}

func (b *FileChunkBitmapStore) Reset(hdr chunkBitmapHeader, stillDirty []uint64, delta chunkDelta) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.loadLocked(); err != nil {
		return err
	}
	rec, err := json.Marshal(delta)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(b.path+".deltas", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("storage: chunk delta log: %w", err)
	}
	if _, err := f.Write(append(rec, '\n')); err != nil {
		f.Close()
		return fmt.Errorf("storage: chunk delta log: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: chunk delta log: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	return b.writeLocked(hdr, stillDirty)
}

// Deltas returns the archived delta records, oldest first.
func (b *FileChunkBitmapStore) Deltas() ([]chunkDelta, error) {
	raw, err := os.ReadFile(b.path + ".deltas")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []chunkDelta
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var d chunkDelta
		if err := json.Unmarshal(line, &d); err != nil {
			return nil, fmt.Errorf("storage: chunk delta log: %w", err)
		}
		out = append(out, d)
	}
	return out, nil
}

var _ chunkBitmapStore = (*FileChunkBitmapStore)(nil)
