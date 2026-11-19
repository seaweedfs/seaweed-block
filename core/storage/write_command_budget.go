package storage

import (
	"context"
	"time"
)

// Compute once, before capture/COW/lifecycle admission. Keep append/completion
// headroom instead of spending the entire command budget in WAL admission.
// This is cooperative: it cannot interrupt a mutex or physical I/O in progress.
func (s *WALStore) writeAdmissionDeadline(ctx context.Context) time.Time {
	now := time.Now()
	deadline, hasDeadline := ctx.Deadline()
	if s.v3 != nil {
		local := now.Add(s.admissionTimeout)
		if !hasDeadline || local.Before(deadline) {
			deadline = local
		}
		reserve := min(time.Second, max(time.Duration(0), deadline.Sub(now)/4))
		return deadline.Add(-reserve)
	}
	if hasDeadline {
		return deadline
	}
	return time.Time{} // preserve legacy admission policy
}

func (s *WALStore) writeBudgetError(ctx context.Context, deadline time.Time) error {
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	if ctx.Err() == context.Canceled {
		return context.Canceled
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return ErrWALPressureTimeout
	}
	return nil
}

// A parked command owns no new physical I/O. Stop waiting for admission locks
// on expiry without a helper goroutine that could acquire them after return.
func (s *WALStore) acquireWriteLock(ctx context.Context, deadline time.Time, try func() bool, lock func()) error {
	if deadline.IsZero() && ctx.Done() == nil {
		lock()
		return nil
	}
	if err := s.writeBudgetError(ctx, deadline); err != nil {
		return err
	}
	if try() {
		return nil
	}
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if err := s.writeBudgetError(ctx, deadline); err != nil {
			return err
		}
		if try() {
			return nil
		}
		select {
		case <-ctx.Done():
			return s.writeBudgetError(ctx, deadline)
		case <-tick.C:
		}
	}
}

// The legacy controller's callbacks take blocking locks. A v3 guest write
// samples the same descriptor/floor/writer tuple with its command deadline;
// failure to acquire a lock refuses admission, never grants capacity or reuse.
func (s *WALStore) writePressureSample(ctx context.Context, deadline time.Time) func() (float64, error) {
	if s.v3 == nil {
		return nil
	}
	return func() (float64, error) {
		if err := s.acquireWriteLock(ctx, deadline, s.mu.TryRLock, s.mu.RLock); err != nil {
			return 0, err
		}
		defer s.mu.RUnlock()
		if s.closed {
			return 0, errVolumeClosed
		}
		v := s.v3
		w := v.w
		if w == nil {
			return 0, nil
		} // append still refuses the missing binding
		p := v.pub
		if err := s.acquireWriteLock(ctx, deadline, p.descMu.TryLock, p.descMu.Lock); err != nil {
			return 0, err
		}
		floor, valid := p.reuseFloorLocked()
		d := p.cur
		p.descMu.Unlock() // preserve snapshot ordering: no descriptor -> writer edge
		if err := s.acquireWriteLock(ctx, deadline, w.mu.TryLock, w.mu.Lock); err != nil {
			return 0, err
		}
		defer w.mu.Unlock()
		if !valid || d.WALSize == 0 || d.Nonce != w.nonce || w.head < floor || w.head-floor > d.WALSize {
			return 0, nil
		}
		return float64(w.head-floor) / float64(d.WALSize), nil
	}
}
