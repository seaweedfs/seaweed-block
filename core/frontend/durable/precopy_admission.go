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
				b.finishPrecopyLocked(b.operational.Load() && !b.closed)
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

func signalPrecopy(w *precopyWaiter) {
	if !w.signaled {
		close(w.ready)
		w.signaled = true
	}
}

// Caller owns mu. Resume admits the old queue serially; a terminal stop wakes
// every command to observe the terminal gate, without issuing storage I/O.
func (b *StorageBackend) finishPrecopyLocked(resume bool) {
	p := b.precopy
	if p == nil {
		return
	}
	p.active = false
	if !resume {
		for _, w := range p.waiters {
			signalPrecopy(w)
		}
		b.precopy = nil
	} else if len(p.waiters) == 0 {
		b.precopy = nil
	} else {
		signalPrecopy(p.waiters[0])
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
	if !p.active {
		if len(p.waiters) == 0 {
			b.precopy = nil
		} else {
			signalPrecopy(p.waiters[0])
		}
	}
}

// An unqueued caller races the pause only at admit's final registration check.
// Waiting callers own no I/O lock and execute no storage/observer operation.
func (b *StorageBackend) admitPrecopy(ctx context.Context, admit func() (func(), error)) (func(), error) {
	for {
		b.mu.Lock()
		p := b.precopy
		if p == nil {
			b.mu.Unlock()
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
		b.mu.Unlock()
		if err := b.readyCheck(); err != nil && !errors.Is(err, frontend.ErrBackendBusy) {
			return nil, err
		}
		b.mu.Lock()
		if b.precopy != p {
			b.mu.Unlock()
			continue
		}
		w := &precopyWaiter{ready: make(chan struct{})}
		p.waiters = append(p.waiters, w)
		if !p.active && len(p.waiters) == 1 {
			signalPrecopy(w)
		}
		deadline := p.started.Add(precopyQueueBudget)
		b.mu.Unlock()
		if hook := b.testPrecopyQueued; hook != nil {
			hook()
		}
		waitCtx, cancel := context.WithDeadline(ctx, deadline)
		select {
		case <-w.ready:
		case <-waitCtx.Done():
		}
		cause := waitCtx.Err()
		cancel()
		if cause != nil {
			b.removePrecopyWaiter(p, w)
			return nil, b.attachReadyTimeout(cause)
		}
		release, err := admit()
		if err != nil {
			b.removePrecopyWaiter(p, w)
			return nil, err
		}
		return func() { release(); b.removePrecopyWaiter(p, w) }, nil
	}
}
