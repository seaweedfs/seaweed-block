package durable

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// chunkRestorer is the chunk layer's independent-restore surface.
type chunkRestorer interface {
	HydrateAllAt(ctx context.Context, bytesPerSecond int64) (storage.HydrateProgress, error)
	OwnBatch(ctx context.Context, n int, foreign func(key string) bool) (storage.OwnProgress, error)
}

func (p *DurableProvider) restorer(volumeID string) (chunkRestorer, error) {
	_, layer, err := p.chunkHandle(volumeID)
	if err != nil {
		return nil, err
	}
	r, ok := layer.(chunkRestorer)
	if !ok {
		return nil, fmt.Errorf("durable: volume %s: chunk layer has no restore surface: %w", volumeID, ErrNotChunkVolume)
	}
	return r, nil
}

// HydrateVolume makes every HEAD-named absent chunk of volumeID local
// (M6.Restore.Restoring>Hydrating), paced to bytesPerSecond, under the
// host limiter's BackgroundFill class.
func (p *DurableProvider) HydrateVolume(ctx context.Context, volumeID string, bytesPerSecond int64) (storage.HydrateProgress, error) {
	r, err := p.restorer(volumeID)
	if err != nil {
		return storage.HydrateProgress{}, err
	}
	return r.HydrateAllAt(hostlimit.WithClass(ctx, hostlimit.BackgroundFill), bytesPerSecond)
}

// OwnVolumeBatch runs one bounded cut of volumeID that re-uploads up to n
// local chunks with foreign HEAD keys under own keys
// (M6.Restore.Owning>Owning).
func (p *DurableProvider) OwnVolumeBatch(ctx context.Context, volumeID string, n int, foreign func(string) bool) (storage.OwnProgress, error) {
	r, err := p.restorer(volumeID)
	if err != nil {
		return storage.OwnProgress{}, err
	}
	ctx, cancel := p.chunkCutContext(ctx)
	defer cancel()
	return r.OwnBatch(ctx, n, foreign)
}
