package frontend

import (
	"context"
	"errors"
	"time"
)

const WriteCommandBudget = 4 * time.Second

// WithWriteBudget preserves an earlier caller deadline, including time spent
// queued before dispatch. A zero arrival denotes a direct backend call.
func WithWriteBudget(ctx context.Context, arrival time.Time) (context.Context, context.CancelFunc) {
	if arrival.IsZero() {
		arrival = time.Now()
	}
	deadline := arrival.Add(WriteCommandBudget)
	if prior, ok := ctx.Deadline(); ok && !deadline.Before(prior) {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}

// IsWriteDeadline recognizes only an expired caller's pure deadline cause.
// Joined physical/unknown errors retain their terminal classification.
func IsWriteDeadline(ctx context.Context, err error) bool {
	return ctx.Err() == context.DeadlineExceeded && deadlineCausesOnly(err)
}
func deadlineCausesOnly(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !deadlineCausesOnly(cause) {
				return false
			}
		}
		return true
	}
	return deadlineCausesOnly(errors.Unwrap(err))
}
