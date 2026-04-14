package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var errGroupCommitShutdown = errors.New("storage: group committer shut down")

// groupCommitter batches concurrent SyncCache requests into a single
// fsync. This amortizes the cost of fsync across many writers and is
// the difference between "fsync per write" and "fsync per millisecond
// regardless of write rate".
//
// Lifetime: NewGroupCommitter() → Run() in a goroutine →
// SyncCache() from any number of writers → Stop() shuts the loop down
// and fails any waiters that arrive after.
type groupCommitter struct {
	syncFunc func() error
	maxDelay time.Duration
	maxBatch int

	mu       sync.Mutex
	pending  []chan error
	stopped  bool
	notifyCh chan struct{}
	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	syncCount atomic.Uint64
}

// groupCommitterConfig is the constructor arg for newGroupCommitter.
type groupCommitterConfig struct {
	SyncFunc func() error  // required: the actual fsync (or analogue)
	MaxDelay time.Duration // upper bound on per-batch waiting; default 1ms
	MaxBatch int           // flush immediately if this many waiters accumulate; default 64
}

func newGroupCommitter(cfg groupCommitterConfig) *groupCommitter {
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = 1 * time.Millisecond
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 64
	}
	return &groupCommitter{
		syncFunc: cfg.SyncFunc,
		maxDelay: cfg.MaxDelay,
		maxBatch: cfg.MaxBatch,
		notifyCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// run drives the batching loop. Call this once in a goroutine.
func (gc *groupCommitter) run() {
	defer close(gc.done)
	for {
		select {
		case <-gc.stopCh:
			gc.markStoppedAndDrain()
			return
		case <-gc.notifyCh:
		}

		// Collect more waiters, capped by maxBatch and maxDelay.
		deadline := time.NewTimer(gc.maxDelay)
	collect:
		for {
			gc.mu.Lock()
			n := len(gc.pending)
			gc.mu.Unlock()
			if n >= gc.maxBatch {
				break
			}
			select {
			case <-deadline.C:
				break collect
			case <-gc.stopCh:
				deadline.Stop()
				gc.markStoppedAndDrain()
				return
			case <-gc.notifyCh:
				// keep collecting
			}
		}
		deadline.Stop()

		gc.mu.Lock()
		batch := gc.pending
		gc.pending = nil
		gc.mu.Unlock()

		if len(batch) == 0 {
			continue
		}

		err := gc.fsyncSafe()
		gc.syncCount.Add(1)
		for _, ch := range batch {
			ch <- err
		}
	}
}

// fsyncSafe runs syncFunc with panic recovery so a misbehaving sync
// doesn't take the whole loop down silently.
func (gc *groupCommitter) fsyncSafe() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("storage: group committer syncFunc panic")
		}
	}()
	return gc.syncFunc()
}

func (gc *groupCommitter) markStoppedAndDrain() {
	gc.mu.Lock()
	gc.stopped = true
	pending := gc.pending
	gc.pending = nil
	gc.mu.Unlock()
	for _, ch := range pending {
		ch <- errGroupCommitShutdown
	}
}

// SyncCache requests an fsync. Blocks until the next batch flushes
// (or the committer shuts down). Returns the result of the fsync.
func (gc *groupCommitter) SyncCache() error {
	ch := make(chan error, 1)
	gc.mu.Lock()
	if gc.stopped {
		gc.mu.Unlock()
		return errGroupCommitShutdown
	}
	gc.pending = append(gc.pending, ch)
	gc.mu.Unlock()

	// Nudge the run loop. notifyCh has buffer 1; a stale notification is harmless.
	select {
	case gc.notifyCh <- struct{}{}:
	default:
	}
	return <-ch
}

// Stop shuts down the committer. Idempotent. Pending waiters receive
// errGroupCommitShutdown.
func (gc *groupCommitter) Stop() {
	gc.stopOnce.Do(func() {
		close(gc.stopCh)
		<-gc.done
	})
}

// SyncCount returns the number of fsync operations performed. Useful
// for tests asserting on batching behavior.
func (gc *groupCommitter) SyncCount() uint64 { return gc.syncCount.Load() }
