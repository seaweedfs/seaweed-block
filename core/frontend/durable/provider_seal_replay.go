package durable

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"

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
func (p *DurableProvider) replayPlainSeal(ctx context.Context, volumeID string, result storage.ChunkCutResult, want *SealExpectation) (storage.ChunkCutResult, uint64, error) {
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
	if err := p.checkSealExpectation(volumeID, h, want); err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
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
	head, err := p.readLayoutSeal(ctx, volumeID, h, proj)
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

// readLayoutSeal reads the same layout HEAD as Start; it never constructs a
// second runtime or falls back to the removed key/intent protocol. After a
// same-epoch detached restart there is no attached runtime, so the durable
// local claim id supplies the identity checked against the record.
func (p *DurableProvider) readLayoutSeal(ctx context.Context, volumeID string, h *volHandle, proj frontend.Projection) (storage.ChunkHead, error) {
	ws, ok := h.storage.(*storage.WALStore)
	if !ok {
		return storage.ChunkHead{}, ErrNotChunkVolume
	}
	claim, _, present := ws.LayoutIdentity()
	if !present || claim == "" {
		return storage.ChunkHead{}, errors.New("durable: detached proof requires the local layout identity")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.closeCtx, cancel)
	defer func() { stop(); cancel() }()
	var head layout.Head
	var err error
	if remote, ok := h.layoutRemote.(interface {
		ReadHead(context.Context) (layout.Head, error)
	}); ok {
		head, err = remote.ReadHead(ctx)
	} else {
		var st layout.StartStore
		st, err = p.cfg.ChunkLayoutHeadFor(volumeID)
		if err == nil && st == nil {
			err = errors.New("durable: nil layout store for detached proof")
		}
		if err == nil {
			var found bool
			head, found, err = st.ReadHead(ctx)
			if err == nil && !found {
				err = layout.ErrHeadAbsent
			}
		}
	}
	if err != nil {
		return storage.ChunkHead{}, err
	}
	me := layout.HolderID{Epoch: proj.Epoch, Holder: p.chunkHolder(proj), ClaimID: claim}
	if head.Epoch != me.Epoch || head.Holder != me.Holder || head.ClaimID != me.ClaimID {
		return storage.ChunkHead{}, ErrChunkEpochStale
	}
	if !layout.DetachedBy(head, me) {
		return storage.ChunkHead{}, errors.New("durable: layout HEAD is not detached")
	}
	return layoutDetachResult(head).Head, nil
}
