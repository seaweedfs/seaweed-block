package storage

import (
	"context"
	"errors"
	"sync"
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
// guest fill. It yields while any flight has guest waiters; its own flights
// have no waiters, so workers serve guest flights first.
//
// Each fill holds one host-limiter permit (cfg.Budget), admitted with ctx and
// no cut payload, the background fill/idle contract (hostlimit Acquire): the
// caller sets the class on ctx (hostlimit.WithClass(ctx,
// hostlimit.BackgroundFill)). The permit belongs to the flight, not the wait:
// it is released only when the flight's I/O is over, also when the walker
// returns early on cancellation, revocation or close. The limiter's abort
// callback (revocation) stops the walker, which then returns the cause.
//
// It returns when nothing is absent, when ctx ends or the permit is revoked,
// or on a fill error that is not a retryable pending state; the caller may
// call it again.
func (l *chunkLayerImpl) HydrateAll(ctx context.Context) (HydrateProgress, error) {
	var p HydrateProgress
	walk, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	backoff := hydrateAllMinBackoff
	quit := func(err error) (HydrateProgress, error) {
		p.Remaining = l.absentHydratable()
		return p, err
	}
	wait := func() error {
		t := time.NewTimer(backoff)
		defer t.Stop()
		if backoff *= 2; backoff > hydrateAllMaxBackoff {
			backoff = hydrateAllMaxBackoff
		}
		select {
		case <-t.C:
			return nil
		case <-walk.Done():
			return context.Cause(walk)
		case <-l.ctx.Done():
			return errChunkLayerClosed
		}
	}
	for {
		if walk.Err() != nil {
			return quit(context.Cause(walk))
		}
		lba, ok, guests := l.nextHydrateTarget()
		if !ok {
			p.Remaining = 0
			return p, nil
		}
		if guests > 0 {
			p.Paused++
			if hook := l.testHydrateAllPause; hook != nil {
				hook()
			}
			if err := wait(); err != nil {
				return quit(err)
			}
			continue
		}
		release, err := l.admitFill(walk, stop)
		if err != nil {
			p.Deferred++
			if err := wait(); err != nil {
				return quit(err)
			}
			continue
		}
		f, err := l.startWalkerFill(lba, release)
		if err != nil {
			if errors.Is(err, ErrChunkHydrationPending) {
				if err := wait(); err != nil { // queue full: retry later
					return quit(err)
				}
				continue
			}
			return quit(err)
		}
		if f == nil {
			continue // resolved meanwhile
		}
		select {
		case <-f.done:
		case <-walk.Done():
			return quit(context.Cause(walk))
		case <-l.ctx.Done():
			return quit(errChunkLayerClosed)
		}
		l.stMu.Lock()
		changed := l.absentGen[f.idx] != f.gen || l.head.Chunks[f.idx] != f.ref
		ferr := f.err
		l.stMu.Unlock()
		switch {
		case changed:
			// a newer absence or HEAD: the next pass looks again
		case ferr == nil:
			p.Filled++
			backoff = hydrateAllMinBackoff
		case errors.Is(ferr, ErrChunkHydrationPending):
			if err := wait(); err != nil { // the read-miss timeout: retry later
				return quit(err)
			}
		default:
			return quit(ferr)
		}
	}
}

// startWalkerFill queues (or joins) the fill flight of lba's chunk without
// becoming a waiter, and hands the permit's release to the flight's
// settlement: a layer-owned goroutine releases it once the flight's I/O is
// over, whatever the walker does meanwhile. A nil flight means nothing is to
// fetch any more (release already called).
func (l *chunkLayerImpl) startWalkerFill(lba uint64, release func()) (*chunkHydrationFlight, error) {
	idx := l.chunkOf(lba)
	l.stMu.Lock()
	defer l.stMu.Unlock()
	if l.closed {
		release()
		return nil, errChunkLayerClosed
	}
	ref, named := l.head.Chunks[idx]
	if !l.absent[idx] || l.localBlocks[lba] || !named {
		release()
		return nil, nil
	}
	f, _, err := l.hydrationFlightLocked(idx, ref)
	if err != nil {
		release()
		return nil, err
	}
	l.wakeHydrationLocked()
	// Close joins wg only after taking stMu and marking closed; every queued
	// flight is settled by closeHydrationLocked, every running one by its worker.
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		<-f.settled
		release()
	}()
	return f, nil
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

// admitFill takes one background permit from the host limiter, if there is
// one: no cut payload (a fill/idle permit carries none and the limiter refuses
// one), the class from ctx. A revocation runs abort, which stops the walker.
// The returned release gives the permit back, once.
func (l *chunkLayerImpl) admitFill(ctx context.Context, abort func(error)) (func(), error) {
	host := l.cfg.Budget
	if host == nil {
		return func() {}, nil
	}
	ticket, err := host.Admit(ctx, 0, abort)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { host.Release(ticket) }) }, nil
}
