package durable_test

import (
	"context"
	"testing"
	"time"
)

// hangGuard returns a context that only guards against a hung test. It ends
// before the test binary's own deadline (go test -timeout), not at a fixed
// wall-clock bound, so a loaded host slows these tests down instead of
// failing them: what they assert is the order and outcome of the operations.
func hangGuard(t *testing.T) context.Context {
	t.Helper()
	deadline, ok := t.Deadline()
	ctx, cancel := context.WithDeadline(context.Background(), hangGuardDeadline(time.Now(), deadline, ok))
	t.Cleanup(cancel)
	return ctx
}

const (
	hangGuardNoDeadline = 10 * time.Minute // no -timeout: still bounded
	hangGuardMaxMargin  = 30 * time.Second
)

// hangGuardDeadline leaves a tenth of the remaining budget (at most 30 s)
// before the binary's deadline, so the guard fires first and the test still
// has time to report and clean up, however short the budget. An expired
// budget gives an expired guard: the test fails at once instead of racing the
// binary's own timeout.
func hangGuardDeadline(now, deadline time.Time, ok bool) time.Time {
	if !ok {
		return now.Add(hangGuardNoDeadline)
	}
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return deadline
	}
	margin := remaining / 10
	if margin > hangGuardMaxMargin {
		margin = hangGuardMaxMargin
	}
	return deadline.Add(-margin)
}

func TestHangGuardDeadline(t *testing.T) {
	now := time.Unix(1700000000, 0)
	for _, c := range []struct {
		name     string
		deadline time.Time
		ok       bool
		want     time.Time
	}{
		{"no deadline", time.Time{}, false, now.Add(hangGuardNoDeadline)},
		{"long budget", now.Add(time.Hour), true, now.Add(time.Hour - hangGuardMaxMargin)},
		{"short budget", now.Add(20 * time.Second), true, now.Add(18 * time.Second)},
		{"nearly expired", now.Add(100 * time.Millisecond), true, now.Add(90 * time.Millisecond)},
		{"expired", now.Add(-time.Second), true, now.Add(-time.Second)},
	} {
		if got := hangGuardDeadline(now, c.deadline, c.ok); !got.Equal(c.want) {
			t.Errorf("%s: guard at %v, want %v", c.name, got.Sub(now), c.want.Sub(now))
		}
	}
}
