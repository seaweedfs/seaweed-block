package durable

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func initializeLayoutRoot(ws *storage.WALStore) error {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	return ws.SetLayoutIdentity(hex.EncodeToString(id[:]), true)
}

// grantedLayoutLine carries the actual assignment accepted by the volume
// host. The master issues a higher epoch only after its clean proof or forced
// lease barrier; the grant is the proof, not a local clock or a HEAD read.
// layout.Start checks that a changed claim is above the HEAD it updates.
type grantedLayoutLine layout.HolderID

func (g grantedLayoutLine) OldLeaseEnded(ctx context.Context, epoch uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.Epoch == 0 || g.Holder == "" || g.Epoch != epoch {
		return errors.New("durable: claim does not match the granted line")
	}
	return nil
}

func startLayoutRoot(ctx context.Context, ws *storage.WALStore, st layout.StartStore, line layout.HolderID) (layout.Head, error) {
	id, fresh, ok := ws.LayoutIdentity()
	if !ok {
		return layout.Head{}, errors.New("durable: legacy root has no layout identity; recreate it")
	}
	position, _, _ := ws.Boundaries()
	h, err := layout.Start(ctx, st, grantedLayoutLine(line), line, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: uint64(ws.NumBlocks()) * uint64(ws.BlockSize()), BlockSize: uint32(ws.BlockSize()), ChunkBytes: uint64(ws.ChunkBlocks()) * uint64(ws.BlockSize())}, ClaimID: id, Fresh: fresh, Position: position})
	if err != nil {
		return layout.Head{}, fmt.Errorf("durable: layout Start: %w", err)
	}
	return h, nil
}

// LayoutHolder returns only the identity of the successfully attached Start.
// Pending, failed and replaced attachments provide no cleaning authority.
func (p *DurableProvider) LayoutHolder(ctx context.Context, volume string) (layout.HolderID, error) {
	if err := ctx.Err(); err != nil {
		return layout.HolderID{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volume]
	if p.closed || h == nil || h.chunk == nil || h.layoutHolder.ClaimID == "" {
		return layout.HolderID{}, ErrNotChunkVolume
	}
	return h.layoutHolder, nil
}

// LayoutRemote returns the single runtime cached by a successful attachment.
// Callers must not construct another remote for the same attachment.
func (p *DurableProvider) LayoutRemote(ctx context.Context, volume string) (storage.ChunkRemote, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volume]
	if p.closed || h == nil || h.chunk == nil || h.layoutHolder.ClaimID == "" || h.layoutRemote == nil {
		return nil, ErrNotChunkVolume
	}
	return h.layoutRemote, nil
}

type layoutBoundRemote struct {
	*closeBoundRemote
	keyer storage.ChunkKeyer
}

func (r *layoutBoundRemote) ChunkKey(index uint64, sum [32]byte) string {
	return r.keyer.ChunkKey(index, sum)
}

func (r *layoutBoundRemote) AbortCut() {
	if a, ok := r.ChunkRemote.(storage.CutAborter); ok {
		a.AbortCut()
	}
}

type layoutStopRemote interface {
	AbortOpenSession(context.Context) (string, error)
	ReadHead(context.Context) (layout.Head, error)
}

func settleLayoutStop(ctx context.Context, remote layoutStopRemote, me layout.HolderID) (storage.DetachSettlement, storage.ChunkCutResult, error) {
	outcome, err := remote.AbortOpenSession(ctx)
	if errors.Is(err, storage.ErrChunkLayerFenced) {
		return storage.DetachFenced, storage.ChunkCutResult{}, nil
	}
	if err != nil {
		return 0, storage.ChunkCutResult{}, err
	}
	switch outcome {
	case "aborted", "idle":
		return storage.DetachNotPublished, storage.ChunkCutResult{}, nil
	case "detached":
		h, err := remote.ReadHead(ctx)
		if err != nil {
			return 0, storage.ChunkCutResult{}, err
		}
		if h.Epoch != me.Epoch || h.Holder != me.Holder || h.ClaimID != me.ClaimID {
			return storage.DetachFenced, storage.ChunkCutResult{}, nil
		}
		if h.State == layout.StateDetached {
			return storage.DetachApplied, storage.ChunkCutResult{H: h.Position, Head: storage.ChunkHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.Position, State: storage.ChunkHeadDetached, DetachedBy: &storage.ChunkDetachedBy{Holder: h.Holder, Epoch: h.Epoch, Gen: h.Gen}}}, nil
		}
	}
	return 0, storage.ChunkCutResult{}, ErrDetachOutcomeUnknown
}
