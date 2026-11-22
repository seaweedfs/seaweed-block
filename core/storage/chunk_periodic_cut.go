package storage

import (
	"context"
	"errors"
)

// ErrChunkCaptureBusy answers a periodic cut that found another capture
// (explicit, snapshot, detach) holding the capture lock. The timer does not
// wait for it: it tries again at its next poll, by when that capture has
// usually restarted the timer.
var ErrChunkCaptureBusy = errors.New("storage: chunk capture lock held by another capture")

// ChunkPeriodicCutter is the chunk layer's cut for a periodic timer. The
// timer decides a cut is due from the last successful cut it saw; another
// cut (explicit, snapshot, detach, no-op) may still be running then and
// complete before this one gets the capture lock. That cut restarted the
// timer, so the due cut is not taken.
type ChunkPeriodicCutter interface {
	// CutUnlessCutSince cuts unless a cut succeeded since the timer decided
	// (CutSeq is no longer seq, the CutSeq it decided on); skipped reports
	// that it did not cut. Cuts are ordered by CutSeq, never by their wall
	// stamps. It never waits for the capture lock: held by another capture,
	// the answer is ErrChunkCaptureBusy.
	CutUnlessCutSince(ctx context.Context, seq uint64) (r ChunkCutResult, skipped bool, err error)
}

var _ ChunkPeriodicCutter = (*chunkLayerImpl)(nil)

func (l *chunkLayerImpl) CutUnlessCutSince(ctx context.Context, seq uint64) (chunkCutResult, bool, error) {
	if !l.capMu.TryLock() {
		return chunkCutResult{}, false, ErrChunkCaptureBusy
	}
	defer l.unlockCap() // delivers the cut's OnPublishSettled report after the lock
	l.stMu.Lock()
	cur := l.stats.CutSeq
	l.stMu.Unlock()
	if cur != seq {
		return chunkCutResult{}, true, nil
	}
	r, err := l.captureLocked(ctx, false)
	return r, false, err
}
