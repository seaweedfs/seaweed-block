package storage

import (
	"context"
	"errors"
	"time"
)

// HydrateProgress reports one HydrateAll call.
//   - Filled: chunks this call made local.
//   - Remaining: chunks still absent (HEAD-named, with a block not written
//     locally) when the call returned; 0 means the volume is fully local.
//   - Paused: times the walker yielded to guest read misses.
//   - Deferred: times the host limiter refused a fill (retried later).
type HydrateProgress struct {
	Filled    int
	Remaining int
	Paused    uint64
	Deferred  uint64
}

// Walker pacing: a guest wait or a refused fill is retried after a short
// backoff that doubles up to the cap.
var (
	hydrateAllMinBackoff = 20 * time.Millisecond
	hydrateAllMaxBackoff = 2 * time.Second
)

// HydrateAll makes every chunk local that HEAD names and the volume does not
// hold yet (an independent restore's background hydrate, O-REC-2). It walks
// the absent chunks lowest index first through the same shared fill flights
// as guest read misses (one fetch per chunk, the hydration queue bound and
// the read-miss timeout apply), so it never fetches a chunk twice or races a
// guest fill. It yields while any flight has guest waiters, and admits each
// fill through the host limiter (cfg.Budget) with ctx: the caller sets the
// limiter class on ctx (hostlimit.WithClass(ctx, hostlimit.BackgroundFill)).
// It returns when nothing is absent, when ctx ends, or on a fill error that
// is not a retryable pending state; the caller may call it again.
func (l *chunkLayerImpl) HydrateAll(ctx context.Context) (HydrateProgress, error) {
	var p HydrateProgress
	backoff := hydrateAllMinBackoff
	wait := func() error {
		t := time.NewTimer(backoff)
		defer t.Stop()
		if backoff *= 2; backoff > hydrateAllMaxBackoff {
			backoff = hydrateAllMaxBackoff
		}
		select {
		case <-t.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-l.ctx.Done():
			return errChunkLayerClosed
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			p.Remaining = l.absentHydratable()
			return p, err
		}
		lba, ok, guests := l.nextHydrateTarget()
		if !ok {
			p.Remaining = 0
			return p, nil
		}
		if guests > 0 {
			p.Paused++
			if err := wait(); err != nil {
				p.Remaining = l.absentHydratable()
				return p, err
			}
			continue
		}
		release, err := l.admitFill(ctx)
		if err != nil {
			p.Deferred++
			if err := wait(); err != nil {
				p.Remaining = l.absentHydratable()
				return p, err
			}
			continue
		}
		_, err = l.OnReadMissContext(ctx, lba)
		release()
		switch {
		case err == nil:
			p.Filled++
			backoff = hydrateAllMinBackoff
		case errors.Is(err, ErrChunkHydrationPending):
			// queue full or the read-miss timeout: retry the same chunk later
			if err := wait(); err != nil {
				p.Remaining = l.absentHydratable()
				return p, err
			}
		default:
			p.Remaining = l.absentHydratable()
			return p, err
		}
	}
}

// nextHydrateTarget returns a block of the lowest-index chunk that HEAD names,
// is absent and has a block without a local extent version (localBlocks: a
// chunk whose every block reached the extent needs no fetch; a write still only
// in the WAL does not count, and the fill never overwrites it), and the number
// of flights that have guest waiters.
func (l *chunkLayerImpl) nextHydrateTarget() (lba uint64, ok bool, guests int) {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	for _, f := range l.hydration {
		if f.waiters > 0 {
			guests++
		}
	}
	best := uint64(0)
	for idx := range l.absent {
		if !l.absent[idx] || (ok && idx >= best) {
			continue
		}
		if b, has := l.hydratableBlockLocked(idx); has {
			best, lba, ok = idx, b, true
		}
	}
	return lba, ok, guests
}

// absentHydratable counts the chunks nextHydrateTarget would still visit.
func (l *chunkLayerImpl) absentHydratable() int {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	n := 0
	for idx, absent := range l.absent {
		if _, has := l.hydratableBlockLocked(idx); absent && has {
			n++
		}
	}
	return n
}

// hydratableBlockLocked returns a block of chunk idx that is not written
// locally, if HEAD names the chunk. Caller holds stMu.
func (l *chunkLayerImpl) hydratableBlockLocked(idx uint64) (uint64, bool) {
	if _, named := l.head.Chunks[idx]; !named {
		return 0, false
	}
	first := idx * l.chunkBlocks
	for i := uint64(0); i < l.chunkBlocks; i++ {
		b := first + i
		if b >= uint64(l.s.NumBlocks()) {
			break
		}
		if !l.localBlocks[b] {
			return b, true
		}
	}
	return 0, false
}

// admitFill reserves one chunk of fill payload from the host limiter, if
// there is one; the returned release gives it back.
func (l *chunkLayerImpl) admitFill(ctx context.Context) (func(), error) {
	host := l.cfg.Budget
	if host == nil {
		return func() {}, nil
	}
	ticket, err := host.Admit(ctx, int64(l.chunkBlocks)*int64(l.blockSize), func(error) {})
	if err != nil {
		return nil, err
	}
	return func() { host.Release(ticket) }, nil
}
