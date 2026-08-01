package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// StorageFactory creates a new unpublished storage object at path. It must
// fail if path already exists.
type StorageFactory func(path string, numBlocks uint32, blockSize int) (storage.LogicalStorage, error)

type RestoreResult struct {
	SnapshotID     string
	SourceFrontier uint64
	TargetFrontier uint64
	NumBlocks      uint32
	BlockSize      int
	RestoredBlocks uint64
	RestoredBytes  uint64
}

// RestoreToNew writes a verified snapshot through a fresh target's normal
// Write path, syncs and closes it, then atomically publishes targetPath. A
// failure before directory durability removes the unpublished target.
func (m *Manager) RestoreToNew(ctx context.Context, snapshotID, targetPath string, factory StorageFactory) (RestoreResult, error) {
	if targetPath == "" {
		return RestoreResult{}, fmt.Errorf("snapshot: restore target path is required")
	}
	if factory == nil {
		return RestoreResult{}, fmt.Errorf("snapshot: restore storage factory is required")
	}
	m.mu.Lock()
	rec, ok := m.byID[snapshotID]
	m.mu.Unlock()
	if !ok {
		return RestoreResult{}, ErrNotFound
	}
	if _, err := os.Stat(targetPath); err == nil {
		return RestoreResult{}, fmt.Errorf("restore target not empty: %w", os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return RestoreResult{}, fmt.Errorf("snapshot: stat restore target: %w", err)
	}
	parent := filepath.Dir(targetPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: mkdir restore target parent: %w", err)
	}
	tmp, err := os.CreateTemp(parent, ".tmp-restore-"+snapshotID+"-*")
	if err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: create restore temp: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return RestoreResult{}, fmt.Errorf("snapshot: close restore temp: %w", err)
	}
	if err := os.Remove(tmpPath); err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: prepare restore temp: %w", err)
	}
	renamed := false
	published := false
	defer func() {
		if !published {
			_ = os.Remove(tmpPath)
			if renamed {
				_ = os.Remove(targetPath)
			}
		}
	}()

	target, err := factory(tmpPath, rec.NumBlocks, rec.BlockSize)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: create restore target: %w", err)
	}
	targetClosed := false
	defer func() {
		if !targetClosed {
			_ = target.Close()
		}
	}()
	result := RestoreResult{
		SnapshotID:     snapshotID,
		SourceFrontier: rec.Frontier,
		NumBlocks:      rec.NumBlocks,
		BlockSize:      rec.BlockSize,
	}
	cut, err := m.ReadBlocks(ctx, snapshotID, func(lba uint32, data []byte) error {
		if _, err := target.Write(lba, data); err != nil {
			return fmt.Errorf("snapshot: restore write LBA %d: %w", lba, err)
		}
		result.RestoredBlocks++
		result.RestoredBytes += uint64(len(data))
		return nil
	})
	if err != nil {
		return RestoreResult{}, err
	}
	if cut.NumBlocks != rec.NumBlocks || cut.BlockSize != rec.BlockSize || cut.Frontier != rec.Frontier || result.RestoredBlocks != rec.RecordCount || result.RestoredBytes != rec.DataBytes {
		return RestoreResult{}, fmt.Errorf("%w: restore counters do not match catalog", ErrArchiveCorrupt)
	}
	frontier, err := target.Sync()
	if err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: sync restore target: %w", err)
	}
	result.TargetFrontier = frontier
	if err := target.Close(); err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: close restore target: %w", err)
	}
	targetClosed = true
	if _, err := os.Stat(targetPath); err == nil {
		return RestoreResult{}, fmt.Errorf("restore target not empty: %w", os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return RestoreResult{}, fmt.Errorf("snapshot: restat restore target: %w", err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: publish restore target: %w", err)
	}
	renamed = true
	if err := syncDir(parent); err != nil {
		return RestoreResult{}, err
	}
	published = true
	return result, nil
}
