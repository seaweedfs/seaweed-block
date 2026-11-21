package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrFrozenTailTooLarge = errors.New("storage: frozen tail does not fit remaining freeze budget")
var ErrInvalidFreezeBudget = errors.New("storage: invalid guest freeze budget")

// FrozenDetachOptions is supplied by the trusted guest orchestrator. It owns
// the maximum two re-freeze cycles; AllowOverBudget explicitly requests its
// final attempt. It never bypasses authority, lifecycle or CAS settlement.
type FrozenDetachOptions struct {
	StartedAt       time.Time
	AllowOverBudget bool
}
type frozenDetachKey struct{}

func WithFrozenDetach(ctx context.Context, opts FrozenDetachOptions) (context.Context, error) {
	now := time.Now()
	if opts.StartedAt.IsZero() || opts.StartedAt.Before(time.Unix(0, 1)) || opts.StartedAt.After(now) {
		return nil, ErrInvalidFreezeBudget
	}
	// Convert remote wall time to a local monotonic origin once at admission.
	opts.StartedAt = now.Add(-now.Sub(opts.StartedAt))
	return context.WithValue(ctx, frozenDetachKey{}, opts), nil
}
func FrozenDetachFromContext(ctx context.Context) (FrozenDetachOptions, bool) {
	v, ok := ctx.Value(frozenDetachKey{}).(FrozenDetachOptions)
	return v, ok
}
func (r *PrecopyReport) admitFrozen(opts FrozenDetachOptions) bool {
	r.GuestFrozen = true
	r.GuestFreezeElapsed = time.Since(opts.StartedAt)
	r.RemainingFreeze = max(time.Duration(0), precopyFreezeLimit-r.GuestFreezeElapsed)
	known := r.Reason != "samples_unknown" && r.Estimate > 0
	if opts.AllowOverBudget {
		r.Reason = "over_budget"
		r.OverBudget = true
	} else if !known || r.Estimate > r.RemainingFreeze {
		r.Reason = "frozen_tail_too_large"
		return false
	} else {
		r.Reason = "fit"
	}
	r.DeclaredFreeze = r.GuestFreezeElapsed + r.Estimate
	if !known {
		r.DeclaredFreeze = max(r.DeclaredFreeze, precopyFreezeLimit)
	}
	return true
}
func frozenRefusal(r PrecopyReport) error {
	return fmt.Errorf("%w: reason=%s estimate=%s remaining=%s elapsed=%s", ErrFrozenTailTooLarge, r.Reason, r.Estimate, r.RemainingFreeze, r.GuestFreezeElapsed)
}
