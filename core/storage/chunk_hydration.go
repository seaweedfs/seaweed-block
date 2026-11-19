package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrChunkHydrationPending means no guest bytes were admitted: hydration
// is queued/in flight or the caller's wait expired. Owned work may continue.
var ErrChunkHydrationPending = errors.New("storage: chunk hydration pending")

const defaultHydrationWorkers = 2
const defaultHydrationQueueDepth = 64
const defaultHydrationWait = 4 * time.Second

type chunkHydrationFlight struct {
	idx, gen uint64
	ref      chunkRef
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	running  bool
	waiters  int
	err      error
}

func hydrationPending(cause error) error {
	if cause == nil {
		return ErrChunkHydrationPending
	}
	return fmt.Errorf("%w: %w", ErrChunkHydrationPending, cause)
}

// The promise and worker lifetime belong to the layer, not any one caller.
// stMu protects admission, queue membership and waiter counts. Nobody holds
// it (or a store/capture/lifecycle lock) while awaiting or fetching bytes.
func (l *chunkLayerImpl) OnReadMissContext(ctx context.Context, lba uint64) ([]byte, error) {
	idx := l.chunkOf(lba)
	for {
		l.stMu.Lock()
		if l.closed {
			l.stMu.Unlock()
			return nil, errChunkLayerClosed
		}
		if l.fenced {
			l.stMu.Unlock()
			return nil, errChunkLayerFenced
		}
		if !l.absent[idx] || l.localBlocks[lba] {
			l.stMu.Unlock()
			return nil, nil
		}
		ref, ok := l.head.Chunks[idx]
		if !ok {
			l.stMu.Unlock()
			return make([]byte, l.blockSize), nil
		}
		if err := ctx.Err(); err != nil {
			l.stMu.Unlock()
			return nil, hydrationPending(err)
		}
		gen := l.absentGen[idx]
		f := l.hydration[idx]
		stale := f != nil && (f.gen != gen || f.ref != ref)
		if stale {
			f.cancel()
		}
		if f == nil {
			if len(l.hydration) >= l.cfg.HydrationQueueDepth {
				l.stMu.Unlock()
				return nil, hydrationPending(nil)
			}
			if !l.hydrationStarted {
				l.hydration = make(map[uint64]*chunkHydrationFlight)
				l.hydrationWake = make(chan struct{}, 1)
				l.hydrationStarted = true
				l.wg.Add(l.cfg.HydrationWorkers)
				for i := 0; i < l.cfg.HydrationWorkers; i++ {
					go l.hydrationWorker()
				}
			}
			work, cancel := context.WithCancel(l.ctx)
			f = &chunkHydrationFlight{idx: idx, gen: gen, ref: ref, ctx: work, cancel: cancel, done: make(chan struct{})}
			l.hydration[idx] = f
		}
		f.waiters++
		l.wakeHydrationLocked()
		l.stMu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			// A completed fault wins a simultaneous caller deadline.
			select {
			case <-f.done:
			default:
				l.stMu.Lock()
				f.waiters--
				l.stMu.Unlock()
				return nil, hydrationPending(ctx.Err())
			}
		}
		l.stMu.Lock()
		f.waiters--
		closed := l.closed
		changed := l.absentGen[idx] != f.gen || l.head.Chunks[idx] != f.ref
		err := f.err
		l.stMu.Unlock()
		if closed {
			return nil, errChunkLayerClosed
		}
		if changed {
			continue
		}
		if err != nil {
			return nil, err
		}
		// A local override or an intervening HEAD/absence change is resolved
		// through the normal path, under the same original caller deadline.
	}
}

func (l *chunkLayerImpl) wakeHydrationLocked() {
	select {
	case l.hydrationWake <- struct{}{}:
	default:
	}
}

func (l *chunkLayerImpl) hydrationWorker() {
	defer l.wg.Done()
	for {
		l.stMu.Lock()
		if l.closed {
			l.stMu.Unlock()
			return
		}
		var next *chunkHydrationFlight
		for _, f := range l.hydration {
			if !f.running && (next == nil || (f.waiters > 0 && next.waiters == 0)) {
				next = f
			}
		}
		if next == nil {
			l.stMu.Unlock()
			select {
			case <-l.hydrationWake:
				continue
			case <-l.ctx.Done():
				return
			}
		}
		next.running = true
		l.wakeHydrationLocked()
		l.stMu.Unlock()
		fetch, cancel := context.WithTimeout(next.ctx, l.cfg.ReadMissTimeout)
		_, err := l.fillOnce(fetch, next.idx, next.gen, next.ref)
		cancel()
		next.cancel()
		l.stMu.Lock()
		next.err = err
		delete(l.hydration, next.idx)
		close(next.done)
		l.wakeHydrationLocked()
		l.stMu.Unlock()
	}
}

// Caller holds stMu, before Close begins joining wg. Running fills remain
// owned until their worker finishes; queued promises settle without I/O.
func (l *chunkLayerImpl) closeHydrationLocked() {
	for idx, f := range l.hydration {
		f.cancel()
		if !f.running {
			f.err = errChunkLayerClosed
			delete(l.hydration, idx)
			close(f.done)
		}
	}
}
