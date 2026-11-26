package durable

import (
	"context"
	"errors"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// SealExpectation binds an operator seal to the receiver's assignment.
// ClaimID is checked too when the caller knows it.
type SealExpectation struct {
	ReplicaID string
	Epoch     uint64
	ClaimID   string
}

// ErrSealHolderChanged refuses a conditional seal before stopping a different holder.
var ErrSealHolderChanged = errors.New("durable: holder changed")

// DetachVolumePlainFor seals only the expected assignment, or replays its proof.
func (p *DurableProvider) DetachVolumePlainFor(ctx context.Context, volumeID string, want SealExpectation) (storage.ChunkCutResult, uint64, error) {
	return p.detachVolumePlain(ctx, volumeID, &want)
}

// Called under the same attachment lock as the stop/replay admission. Never
// stop a replacement because a request matched an earlier HTTP observation.
func (p *DurableProvider) checkSealExpectation(volumeID string, h *volHandle, want *SealExpectation) error {
	if want == nil {
		return nil
	}
	own := p.view.Projection()
	if want.ReplicaID == "" || want.Epoch == 0 || own.VolumeID != volumeID || own.ReplicaID != want.ReplicaID || own.Epoch != want.Epoch {
		return ErrSealHolderChanged
	}
	if want.ClaimID != "" {
		ws, ok := h.storage.(*storage.WALStore)
		if !ok {
			return ErrSealHolderChanged
		}
		claim, _, present := ws.LayoutIdentity()
		if !present || claim != want.ClaimID {
			return ErrSealHolderChanged
		}
	}
	return nil
}
