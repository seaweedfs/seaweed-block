package durable_test

import (
	"context"
	"testing"
	"time"
)

// hangGuard returns a context that only guards against a hung test. It ends
// shortly before the test binary's own deadline (go test -timeout), not at a
// fixed wall-clock bound, so a loaded host slows these tests down instead of
// failing them: what they assert is the order and outcome of the operations.
func hangGuard(t *testing.T) context.Context {
	t.Helper()
	deadline, ok := t.Deadline()
	if !ok {
		deadline = time.Now().Add(10 * time.Minute)
	}
	if margin := 30 * time.Second; time.Until(deadline) > 2*margin {
		deadline = deadline.Add(-margin)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	t.Cleanup(cancel)
	return ctx
}
