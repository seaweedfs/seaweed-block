package durable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

const precopyQueueBudget = 5 * time.Second

type precopyWaiter struct {
	ready    chan struct{}
	deadline time.Time
	err      error
	signaled bool
}
type precopyAdmission struct {
	active    bool
	started   time.Time
	drained   chan struct{}
	drainDone bool
	waiters   []*precopyWaiter
}

// Lock order: no store, provider, capture or projection call while holding mu.
// Physical I/O is counted only after the complete gate. Drain waits without mu
// or ioMu, before the storage capture barrier, so admitted writers can finish.
func (b *StorageBackend) BeginPrecopyPause(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed || !b.operational.Load() || b.observerErr != nil || b.precopy != nil {
		b.mu.Unlock()
		return nil, fmt.Errorf("%w: pre-copy admission unavailable", frontend.ErrBackendBusy)
	}
	p := &precopyAdmission{active: true, started: time.Now(), drained: make(chan struct{})}
	b.precopy = p
	if b.admittedIO == 0 {
		close(p.drained)
		p.drainDone = true
	}
	b.mu.Unlock()
	var once sync.Once
	resume := func() {
		once.Do(func() {
			b.mu.Lock()
			if b.precopy == p {
				b.finishPrecopyLocked()
			}
			b.mu.Unlock()
		})
	}
	select {
	case <-p.drained:
		b.mu.Lock()
		valid := b.precopy == p && p.active && b.operational.Load() && !b.closed
		b.mu.Unlock()
		if !valid {
			resume()
			return nil, frontend.ErrNotReady
		}
		if err := ctx.Err(); err != nil {
			resume()
			return nil, err
		}
		return resume, nil
	case <-ctx.Done():
		resume()
		return nil, ctx.Err()
	}
}

// Caller owns mu. INV-CUT-PRECOPY-001: ending the pause releases every
// waiter in FIFO admission order in this one lock hold, then normal gate checks
// and completion run independently. New arrivals cannot join the ended pause.
func (b *StorageBackend) finishPrecopyLocked() {
	p := b.precopy
	if p == nil {
		return
	}
	p.active = false
	b.precopy = nil
	for _, w := range p.waiters {
		// INV-CUT-PRECOPY-001: expiry versus grant is decided under mu.
		if !time.Now().Before(w.deadline) {
			w.err = context.DeadlineExceeded
		}
		w.signaled = true
		close(w.ready)
		if hook := b.testPrecopyGranted; hook != nil && w.err == nil {
			hook(w)
		}
	}
}

func (b *StorageBackend) removePrecopyWaiter(p *precopyAdmission, w *precopyWaiter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.precopy != p {
		return
	}
	for i, v := range p.waiters {
		if v == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			break
		}
	}
}

// An unqueued caller races the pause only at admit's final registration check.
// Waiting callers own no I/O lock and execute no storage/observer operation.
func (b *StorageBackend) admitPrecopy(ctx context.Context, admit func() (func(), error)) (func(), error) {
	arrival := time.Now() // INV-CUT-PRECOPY-001: one command-owned wait budget.
	for {
		b.mu.Lock()
		p := b.precopy

		b.mu.Unlock()
		if p != nil {
			if err := b.readyCheck(); err != nil && !errors.Is(err, frontend.ErrBackendBusy) {
				return nil, err
			}
			b.mu.Lock()
			if b.precopy != p {
				b.mu.Unlock()
				continue
			}
			deadline := arrival.Add(precopyQueueBudget)
			if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
				deadline = d
			}
			w := &precopyWaiter{ready: make(chan struct{}), deadline: deadline}
			p.waiters = append(p.waiters, w)
			b.mu.Unlock()
			if hook := b.testPrecopyQueued; hook != nil {
				hook()
			}
			waitCtx, cancel := context.WithDeadline(ctx, deadline)
			select {
			case <-w.ready:
			case <-waitCtx.Done():
			}
			b.mu.Lock()
			cause := waitCtx.Err()
			if w.signaled {
				cause = w.err
			}
			b.mu.Unlock()
			// A timely queue grant cannot outlive the command's own deadline.
			if cause == nil {
				cause = ctx.Err()
			}
			cancel()
			if cause != nil {
				b.removePrecopyWaiter(p, w)
				return nil, b.attachReadyTimeout(cause)
			}
		}
		release, err := admit()
		if err != nil {
			b.mu.Lock()
			pending := b.precopy != nil
			b.mu.Unlock()
			if pending {
				continue
			}
		}
		return release, err
	}
}
