package storage

import "context"

// ChunkPeriodicCutter is the chunk layer's cut for a periodic timer. The
// timer decides a cut is due from the last successful cut it saw; another
// cut (explicit, snapshot, detach, no-op) may still be running then and
// complete before this one gets the capture lock. That cut restarted the
// timer, so the due cut is not taken.
type ChunkPeriodicCutter interface {
	// CutUnlessCutSince cuts unless a cut succeeded since the timer decided
	// (CutSeq is no longer seq, the CutSeq it decided on); skipped reports
	// that it did not cut. Cuts are ordered by CutSeq, never by their wall
	// stamps.
	CutUnlessCutSince(ctx context.Context, seq uint64) (r ChunkCutResult, skipped bool, err error)
}

var _ ChunkPeriodicCutter = (*chunkLayerImpl)(nil)

func (l *chunkLayerImpl) CutUnlessCutSince(ctx context.Context, seq uint64) (chunkCutResult, bool, error) {
	l.capMu.Lock()
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

// ChunkStagedDiscarder yields stalled uploads to cleaning. A pending publication
// is never discarded, and progress made by another cut invalidates the request.
type ChunkStagedDiscarder interface {
	DiscardStagedIfUnchanged(staged, seq uint64) bool
}

func (l *chunkLayerImpl) DiscardStagedIfUnchanged(staged, seq uint64) bool {
	if !l.capMu.TryLock() {
		return false
	}
	defer l.unlockCap()
	l.stMu.Lock()
	safe := !l.closed && l.pending == nil && !l.fenced && l.detached == nil && l.stats.StagedChunks == staged && l.stats.CutSeq == seq
	l.stMu.Unlock()
	if !safe {
		return false
	}
	l.discardStaged()
	return true
}
