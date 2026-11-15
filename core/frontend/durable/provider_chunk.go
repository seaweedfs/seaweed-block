package durable

// walstore-chunk: attaching the storage chunk layer to a provider volume.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ErrNotChunkVolume is returned by the chunk operations for a volume that
// has no attached chunk layer (another impl, or not recovered yet).
var ErrNotChunkVolume = errors.New("durable: volume has no chunk layer")

// attachChunk attaches the chunk layer after a successful Recover. A volume
// this provider created starts from the remote HEAD (fresh host); an
// existing local volume stays authoritative. The manifest epoch is the
// master-assigned epoch from the projection, held by the layer separately
// from the frontend Identity.
func (p *DurableProvider) attachChunk(volumeID string, h *volHandle) error {
	p.mu.Lock()
	attached := h.chunk != nil
	p.mu.Unlock()
	if attached {
		return nil
	}
	ws, ok := h.storage.(*storage.WALStore)
	if !ok {
		return fmt.Errorf("durable: %s storage is %T, not a walstore", ImplWALStoreChunk, h.storage)
	}
	remote, err := p.cfg.ChunkRemoteFor(volumeID)
	if err != nil {
		return fmt.Errorf("durable: chunk remote for %s: %w", volumeID, err)
	}
	proj := p.view.Projection()
	holder := p.cfg.ChunkHolder
	if holder == "" {
		holder = proj.ReplicaID
	}
	layer, err := storage.AttachChunkLayer(ws, remote, storage.ChunkLayerConfig{
		ChunkBlocks:     p.cfg.ChunkBlocks,
		Epoch:           proj.Epoch,
		Holder:          holder,
		FreshFromHead:   h.created,
		ReadMissTimeout: p.cfg.ChunkReadMissTimeout,
		Bitmap:          storage.NewFileChunkBitmapStore(h.path + ".chunkbitmap"),
		CaptureAllowed: func() error {
			cur := p.view.Projection()
			if cur.VolumeID != volumeID || !cur.Healthy {
				return fmt.Errorf("volume %s is not a healthy primary here", volumeID)
			}
			return nil
		},
	})
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h.chunk = layer
	if p.cfg.ChunkCaptureInterval > 0 && !p.closed {
		ctx, cancel := context.WithCancel(context.Background())
		h.chunkStop, h.chunkDone = cancel, make(chan struct{})
		go p.chunkLoop(ctx, volumeID, layer, h.chunkDone)
	}
	return nil
}

// chunkLoop takes a periodic cut. A refused cut (not a healthy primary,
// BASE install) is normal and skipped quietly.
func (p *DurableProvider) chunkLoop(ctx context.Context, volumeID string, layer storage.ChunkLayer, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(p.cfg.ChunkCaptureInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := layer.Capture(ctx); err != nil && !errors.Is(err, storage.ErrChunkCaptureRefused) && ctx.Err() == nil {
			log.Printf("durable: chunk cut for volume=%s: %v", volumeID, err)
		}
	}
}

func (p *DurableProvider) stopChunkLoop(h *volHandle) {
	p.mu.Lock()
	stop, done := h.chunkStop, h.chunkDone
	h.chunkStop = nil
	p.mu.Unlock()
	if stop != nil {
		stop()
		<-done
	}
}

func (p *DurableProvider) chunkLayer(volumeID string) (storage.ChunkLayer, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	p.mu.Unlock()
	if !ok {
		return nil, ErrVolumeNotOpen
	}
	p.mu.Lock()
	layer := h.chunk
	p.mu.Unlock()
	if layer == nil {
		return nil, ErrNotChunkVolume
	}
	return layer, nil
}

// CaptureVolume takes one quiesced cut of volumeID and publishes it to the
// remote. It returns the cut's LSN.
func (p *DurableProvider) CaptureVolume(ctx context.Context, volumeID string) (uint64, error) {
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return 0, err
	}
	return layer.Capture(ctx)
}

// MakeVolumeRemoteDurable returns nil only once volumeID's writes up to lsn
// are published in the remote HEAD (snapshot, detach, migrate).
func (p *DurableProvider) MakeVolumeRemoteDurable(ctx context.Context, volumeID string, lsn uint64) error {
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return err
	}
	return layer.MakeRemoteDurable(ctx, lsn)
}
