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
