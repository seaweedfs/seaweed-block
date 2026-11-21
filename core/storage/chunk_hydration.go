package storage

import (
	"context"
	"errors"
	"fmt"
)

// ErrChunkHydrationPending means no guest bytes were admitted: hydration
// is queued/in flight or the caller's wait expired. Owned work may continue.
var ErrChunkHydrationPending = errors.New("storage: chunk hydration pending")

const defaultHydrationWorkers = 2
const defaultHydrationQueueDepth = 64

// ChunkHydrationStats separates wait refusals from owned fetch outcomes.
// A wait refusal is not necessarily a wire completion (a caller may cancel).
type ChunkHydrationStats struct {
	WorkerLimit  int    // fetch workers allowed (--chunk-hydration-workers)
	QueueDepth   int    // fetch queue capacity (--chunk-hydration-queue-depth)
	Active       int    // fetches running now
	Queued       int    // fetches waiting for a worker now
	WaitCalls    uint64 // reads that waited for a chunk to arrive
	Joined       uint64 // of those, reads that joined a fetch already running
	WaitRefusals uint64 // waits that ended without the chunk (the caller cancelled, or the fetch was not admitted)
	Fetches      uint64 // fetches started
	Completed    uint64 // fetches that installed the chunk
	FetchErrors  uint64 // fetches that failed
}

type chunkHydrationFlight struct {
	idx, gen uint64
	ref      chunkRef
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	settled  chan struct{} // closed once its I/O is over: after fillOnce, or removal unrun
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
	counted := false
	for {
		l.stMu.Lock()
		if l.closed {
			l.stMu.Unlock()
			return nil, errChunkLayerClosed
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
		if !counted {
			l.stats.Hydration.WaitCalls++
			counted = true
		}
		if err := ctx.Err(); err != nil {
			l.stats.Hydration.WaitRefusals++
			l.stMu.Unlock()
			return nil, hydrationPending(err)
		}
		f, joined, err := l.hydrationFlightLocked(idx, ref)
		if err != nil {
			l.stats.Hydration.WaitRefusals++
			l.stMu.Unlock()
			return nil, err
		}
		if joined {
			l.stats.Hydration.Joined++
		}
		f.waiters++
		l.wakeHydrationLocked()
		l.stMu.Unlock()
		if hook := l.testHydrationWait; hook != nil {
			hook(idx)
		}
		select {
		case <-l.ctx.Done():
			l.stMu.Lock()
			f.waiters--
			l.stMu.Unlock()
			return nil, errChunkLayerClosed
		case <-f.done:
		case <-ctx.Done():
			// A completed fault wins a simultaneous caller deadline.
			select {
			case <-f.done:
			default:
				l.stMu.Lock()
				f.waiters--
				l.stats.Hydration.WaitRefusals++
				l.stMu.Unlock()
				return nil, hydrationPending(ctx.Err())
			}
		}
		l.stMu.Lock()
		f.waiters--
		closed := l.closed
		changed := l.absentGen[idx] != f.gen || l.head.Chunks[idx] != f.ref
		err = f.err
		if !closed && !changed && errors.Is(err, ErrChunkHydrationPending) {
			l.stats.Hydration.WaitRefusals++
		}
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

// hydrationFlightLocked returns the flight for chunk idx's current absence
// (generation and HEAD ref): an existing one (joined) or a new queued one. A
// stale flight of an older generation is settled and replaced. It refuses with
// hydrationPending when the queue is full. The caller holds stMu, counts its
// own waiter and wakes the workers.
func (l *chunkLayerImpl) hydrationFlightLocked(idx uint64, ref chunkRef) (*chunkHydrationFlight, bool, error) {
	gen := l.absentGen[idx]
	f := l.hydration[idx]
	if f != nil && (f.gen != gen || f.ref != ref) {
		f.cancel()
		// The old worker remains owned and joined, but cannot delay a
		// new generation or remove its replacement when it finishes.
		delete(l.hydration, idx)
		close(f.done)
		if !f.running {
			close(f.settled) // never picked up: no I/O will run
		}
		f = nil
	}
	if f != nil {
		return f, true, nil
	}
	if len(l.hydration) >= l.cfg.HydrationQueueDepth {
		return nil, false, hydrationPending(nil)
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
	f = &chunkHydrationFlight{idx: idx, gen: gen, ref: ref, ctx: work, cancel: cancel, done: make(chan struct{}), settled: make(chan struct{})}
	l.hydration[idx] = f
	return f, false, nil
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
		l.stats.Hydration.Active++
		l.stats.Hydration.Fetches++
		l.wakeHydrationLocked()
		l.stMu.Unlock()
		fetch, cancel := context.WithTimeout(next.ctx, l.cfg.ReadMissTimeout)
		resolved, err := l.fillOnce(fetch, next.idx, next.gen, next.ref)
		cancel()
		next.cancel()
		close(next.settled)
		l.stMu.Lock()
		if hydrationContextCauseOnly(err) {
			err = hydrationPending(err)
		}
		if err != nil {
			l.stats.Hydration.FetchErrors++
		} else if resolved {
			l.stats.Hydration.Completed++
		}
		l.stats.Hydration.Active--
		next.err = err
		if l.hydration[next.idx] == next {
			delete(l.hydration, next.idx)
			close(next.done)
		}
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
			close(f.settled)
		}
	}
}

// Joined physical/unknown errors must not become retryable just because they
// also contain a context cause.
func hydrationContextCauseOnly(err error) bool {
	if err == context.DeadlineExceeded || err == context.Canceled {
		return true
	}
	if err == nil {
		return false
	}
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		causes := j.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !hydrationContextCauseOnly(cause) {
				return false
			}
		}
		return true
	}
	return hydrationContextCauseOnly(errors.Unwrap(err))
}
