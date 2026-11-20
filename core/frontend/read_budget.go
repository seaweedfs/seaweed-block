package frontend

import (
	"context"
	"time"
)

const ReadCommandBudget = WriteCommandBudget

// WithReadBudget includes queue residence and all cooperative attachment and
// hydration waits. A previously established, earlier deadline is never reset.
func WithReadBudget(ctx context.Context, arrival time.Time) (context.Context, context.CancelFunc) {
	return WithWriteBudget(ctx, arrival)
}
