package frontend

import (
	"context"
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
