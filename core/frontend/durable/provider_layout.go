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

func startLayoutRoot(ctx context.Context, ws *storage.WALStore, st layout.HeadStore, line layout.HolderID) (layout.Head, error) {
	id, fresh, ok := ws.LayoutIdentity()
	if !ok {
		return layout.Head{}, errors.New("durable: legacy root has no layout identity; recreate it")
	}
	position, _, _ := ws.Boundaries()
	h, err := layout.Start(ctx, st, grantedLayoutLine(line), line, layout.LocalRoot{ClaimID: id, Fresh: fresh, Position: position})
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

func settleLayoutStop(ctx context.Context, st layout.HeadStore, me layout.HolderID, session string) (storage.DetachSettlement, storage.ChunkCutResult, error) {
	var abortErr error
	for read := 0; read < 2; read++ {
		h, found, err := st.ReadHead(ctx)
		if err != nil {
			return 0, storage.ChunkCutResult{}, err
		}
		if !found {
			return 0, storage.ChunkCutResult{}, layout.ErrHeadAbsent
		}
		if h.Epoch != me.Epoch || h.Holder != me.Holder || h.ClaimID != me.ClaimID {
			return storage.DetachFenced, storage.ChunkCutResult{}, nil
		}
		if h.State == layout.StateDetached {
			return storage.DetachApplied, storage.ChunkCutResult{H: h.Position, Head: storage.ChunkHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.Position, State: storage.ChunkHeadDetached, DetachedBy: &storage.ChunkDetachedBy{Holder: h.Holder, Epoch: h.Epoch, Gen: h.Gen}}}, nil
		}
		if h.State == layout.StateLive && h.Activity.Kind == layout.ActIdle {
			return storage.DetachNotPublished, storage.ChunkCutResult{}, nil
		}
		if read == 0 && h.State == layout.StateLive && session != "" && h.Activity == (layout.Activity{Kind: layout.ActSnap, Session: session}) {
			// Abort and Commit compete on the same generation. After an unknown abort
			// reply, the read below still must prove idle (or detached); words alone
			// never allow guest writes to resume ahead of a delayed Commit.
			_, abortErr = layout.RunAbort(ctx, st, me, session)
			continue
		}
		return 0, storage.ChunkCutResult{}, fmt.Errorf("durable: stop cannot prove its session ended (%s/%s): %w", h.State, h.Activity.Kind, errors.Join(ErrDetachOutcomeUnknown, abortErr))
	}
	return 0, storage.ChunkCutResult{}, ErrDetachOutcomeUnknown
}
