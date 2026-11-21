package storage

import "context"

// ChunkPeriodicCutter is the chunk layer's cut for a periodic timer. The
// timer decides a cut is due from the last successful cut it saw; another
// cut (explicit, snapshot, detach, no-op) may still be running then and
// complete before this one gets the capture lock. That cut restarted the
// timer, so the due cut is not taken.
type ChunkPeriodicCutter interface {
	// CutUnlessCutSince cuts unless a cut succeeded after lastCutNs (the
	// LastCutNs the timer decided on); skipped reports that it did not cut.
	CutUnlessCutSince(ctx context.Context, lastCutNs int64) (r ChunkCutResult, skipped bool, err error)
}

var _ ChunkPeriodicCutter = (*chunkLayerImpl)(nil)

func (l *chunkLayerImpl) CutUnlessCutSince(ctx context.Context, lastCutNs int64) (chunkCutResult, bool, error) {
	l.capMu.Lock()
	defer l.unlockCap() // delivers the cut's OnPublishSettled report after the lock
	l.stMu.Lock()
	last := l.stats.LastCutNs
	l.stMu.Unlock()
	if last > lastCutNs {
		return chunkCutResult{}, true, nil
	}
	r, err := l.captureLocked(ctx, false)
	return r, false, err
}
