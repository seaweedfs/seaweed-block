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

// FrozenBudget reports the storage-owned admission forecast and quiesce slack.
// A tail above the existing 60s ceiling is a saturated rejection sentinel,
// not a complete duration: it stays known but offers zero quiesce slack.
// INV-WS-FREEZE-ACCOUNTING-001: same-host durations; admission still rechecks.
type FrozenBudget struct {
	FrozenEstimate      time.Duration `json:"frozenEstimateNs"`
	FrozenEstimateKnown bool          `json:"frozenEstimateKnown"`
	FreezeLimit         time.Duration `json:"freezeLimitNs"`
	MaxRefreezeQuiesce  time.Duration `json:"maxRefreezeQuiesceNs"`
}

func (r PrecopyReport) frozenBudgetEstimate() FrozenBudget {
	b := FrozenBudget{FrozenEstimate: r.Estimate, FrozenEstimateKnown: r.Reason != "samples_unknown" && r.Estimate > 0, FreezeLimit: precopyFreezeLimit}
	if b.FrozenEstimateKnown {
		b.MaxRefreezeQuiesce = max(time.Duration(0), b.FreezeLimit-b.FrozenEstimate)
	}
	return b
}

// FrozenRefusal keeps the admission observation attached to the typed error.
type FrozenRefusal struct{ Report PrecopyReport }

func (e *FrozenRefusal) Error() string {
	r := e.Report
	return fmt.Sprintf("%s: reason=%s estimate=%s remaining=%s elapsed=%s", ErrFrozenTailTooLarge, r.Reason, r.Estimate, r.RemainingFreeze, r.GuestFreezeElapsed)
}
func (e *FrozenRefusal) Unwrap() error { return ErrFrozenTailTooLarge }

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
	return r.admitFrozenAt(opts, time.Now())
}
func (r *PrecopyReport) observeFrozenAt(opts FrozenDetachOptions, now time.Time) {
	r.FrozenBudget = r.frozenBudgetEstimate()
	r.GuestFrozen = true
	r.GuestFreezeElapsed = max(time.Duration(0), now.Sub(opts.StartedAt))
	budget := r.FrozenBudget
	r.RemainingFreeze = max(time.Duration(0), budget.FreezeLimit-r.GuestFreezeElapsed)
}

func (r *PrecopyReport) admitFrozenAt(opts FrozenDetachOptions, now time.Time) bool {
	r.observeFrozenAt(opts, now)
	known := r.FrozenBudget.FrozenEstimateKnown
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
	return &FrozenRefusal{Report: r}
}

// IsReplay distinguishes a replayed applied point from a newly completed freeze.
// The original report remains unchanged on the wire.
func (r PrecopyReport) IsReplay() bool { return r.replayed }

func (l *chunkLayerImpl) frozenNow() time.Time {
	now := time.Now()
	if hook := l.testFrozenNow; hook != nil {
		now = hook()
	}
	return now
}

// M5 frozen-detach report: observe before drain/reason handling, without
// deciding admission or overriding an existing refusal.
func (l *chunkLayerImpl) observeFrozen(r *PrecopyReport, opts FrozenDetachOptions, enabled bool) {
	if enabled {
		r.observeFrozenAt(opts, l.frozenNow())
	}
}

func (l *chunkLayerImpl) admitFrozen(r *PrecopyReport, opts FrozenDetachOptions) bool {
	return r.admitFrozenAt(opts, l.frozenNow())
}

// RememberDetachReport completes the telemetry kept with an applied or still
// pending point. The durable provider calls it after its final settlement spans.
// It changes no publication identity or authority state.
func (l *chunkLayerImpl) RememberDetachReport(report PrecopyReport) {
	if report.IsReplay() {
		return
	}
	l.stMu.Lock()
	defer l.stMu.Unlock()
	if l.detached != nil {
		l.detached.Precopy = report
	}
	if l.pendingDetach {
		l.pendingCut.Precopy = report
	}
}
