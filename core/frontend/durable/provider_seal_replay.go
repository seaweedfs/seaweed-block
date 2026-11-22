package durable

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func (p *DurableProvider) chunkHolder(proj frontend.Projection) string {
	if p.cfg.ChunkHolder != "" {
		return p.cfg.ChunkHolder
	}
	return proj.ReplicaID
}

// replayPlainSeal implements M5 PlainSeal -> RemoteProof. It also works when
// RecoverVolume refused a sealed incarnation and installed no local layer.
func (p *DurableProvider) replayPlainSeal(ctx context.Context, volumeID string, result storage.ChunkCutResult) (storage.ChunkCutResult, uint64, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	if p.closed || !ok {
		p.mu.Unlock()
		return storage.ChunkCutResult{}, 0, ErrVolumeNotOpen
	}
	if h.impl != ImplWALStoreChunk {
		p.mu.Unlock()
		return storage.ChunkCutResult{}, 0, ErrNotChunkVolume
	}
	p.inflight.Add(1) // Close cannot pass its join before this admitted read.
	p.mu.Unlock()
	defer p.inflight.Done()
	h.attachMu.Lock()
	defer h.attachMu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
	proj := p.view.Projection()
	if !chunkAssignment(proj, volumeID) {
		return storage.ChunkCutResult{}, 0, ErrChunkNotPrimary
	}
	lin := chunkLineage{volumeID: proj.VolumeID, replicaID: proj.ReplicaID, epoch: proj.Epoch, endpoint: proj.EndpointVersion}
	if h.chunkLineage != (chunkLineage{}) && h.chunkLineage != lin {
		return storage.ChunkCutResult{}, 0, ErrChunkEpochStale
	}
	if err := h.backend.stopPlainDetachIfIdle(); err != nil {
		// A seal may already have committed before this read-back. Do not
		// expose the before-mutation plain_detach_busy promise here.
		return storage.ChunkCutResult{}, 0, fmt.Errorf("%w: replay admission: %v", ErrDetachOutcomeUnknown, err)
	}
	h.chunkDetach = true
	p.stopChunkLoop(h)
	inner, err := p.cfg.ChunkRemoteFor(volumeID)
	if err != nil {
		return storage.ChunkCutResult{}, 0, fmt.Errorf("%w: %w", ErrDetachOutcomeUnknown, err)
	}
	remote := &closeBoundRemote{ChunkRemote: inner, done: p.closeCtx}
	head, err := storage.ReadDetachedChunkHead(ctx, remote, proj.Epoch, p.chunkHolder(proj))
	if err != nil {
		return storage.ChunkCutResult{}, 0, fmt.Errorf("%w: %w", ErrDetachOutcomeUnknown, err)
	}
	current := p.view.Projection()
	if current.VolumeID != lin.volumeID || current.ReplicaID != lin.replicaID || current.Epoch != lin.epoch || current.EndpointVersion != lin.endpoint {
		return storage.ChunkCutResult{}, 0, ErrChunkEpochStale
	}
	if err := ctx.Err(); err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
	if p.closeCtx.Err() != nil {
		return storage.ChunkCutResult{}, 0, errProviderClosed
	}
	result.H, result.Head = head.RemoteLSN, head
	return result, head.RemoteLSN, nil
}
