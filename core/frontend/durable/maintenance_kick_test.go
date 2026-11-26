package durable

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// countingPass counts maintenance passes; fail(n) is pass n's result.
type countingPass struct {
	n    chan int
	seen int
	fail func(n int) error
}

func (c *countingPass) run(context.Context, string) error {
	c.seen++
	c.n <- c.seen
	if c.fail != nil {
		return c.fail(c.seen)
	}
	return nil
}

func (c *countingPass) next(t *testing.T, within time.Duration) int {
	t.Helper()
	select {
	case n := <-c.n:
		return n
	case <-time.After(within):
		t.Fatalf("no maintenance pass within %s", within)
		return 0
	}
}

func (c *countingPass) none(t *testing.T, for_ time.Duration) {
	t.Helper()
	select {
	case n := <-c.n:
		t.Fatalf("unexpected maintenance pass %d", n)
	case <-time.After(for_):
	}
}

// With the shipped hour-long interval, a kick (as after a snapshot delete)
// runs one pass at once; a pass is not repeated until the next kick or
// interval, and kicks given while one is pending coalesce.
func TestMaintenanceKickRunsAPassNow(t *testing.T) {
	c := &countingPass{n: make(chan int, 8)}
	p := &DurableProvider{cfg: ProviderConfig{ChunkMaintenanceInterval: time.Hour, ChunkMaintenance: c.run}}
	kick := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.chunkMaintenanceLoop(ctx, "v1", maintenanceStubLayer{}, func() bool { return true }, kick)
	c.none(t, 200*time.Millisecond) // the first periodic pass is an hour away
	kick <- struct{}{}
	if n := c.next(t, 5*time.Second); n != 1 {
		t.Fatalf("pass %d after the kick; want 1", n)
	}
	c.none(t, 200*time.Millisecond)
	kick <- struct{}{}
	select {
	case kick <- struct{}{}: // the loop may not have taken the first yet
	default:
	}
	if n := c.next(t, 5*time.Second); n != 2 {
		t.Fatalf("pass %d after the second kick; want 2", n)
	}
}

// A kicked pass the volume refused as busy (a cut session open: the
// cleaning never runs during one) is tried again after the short retry,
// not an interval later.
func TestMaintenanceKickedBusyPassRetriesSoon(t *testing.T) {
	old := maintenanceKickRetry
	maintenanceKickRetry = 50 * time.Millisecond
	defer func() { maintenanceKickRetry = old }()
	c := &countingPass{n: make(chan int, 8), fail: func(n int) error {
		if n == 1 {
			return fmt.Errorf("cleaning: %w", layout.ErrBusy)
		}
		return nil
	}}
	p := &DurableProvider{cfg: ProviderConfig{ChunkMaintenanceInterval: time.Hour, ChunkMaintenance: c.run}}
	kick := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.chunkMaintenanceLoop(ctx, "v1", maintenanceStubLayer{}, func() bool { return true }, kick)
	kick <- struct{}{}
	if n := c.next(t, 5*time.Second); n != 1 {
		t.Fatalf("pass %d; want 1", n)
	}
	if n := c.next(t, 5*time.Second); n != 2 {
		t.Fatalf("pass %d after the busy refusal; want the retry (2)", n)
	}
	c.none(t, 300*time.Millisecond) // the retry succeeded: back to the interval
}

// KickMaintenance reaches only a volume whose maintenance loop runs.
func TestKickMaintenanceReachesOnlyARunningLoop(t *testing.T) {
	kick := make(chan struct{}, 1)
	p := &DurableProvider{volumes: map[string]*volHandle{"on": {maintKick: kick}, "off": {}}}
	if !p.KickMaintenance("on") || len(kick) != 1 {
		t.Fatal("a kick did not reach the running loop")
	}
	if !p.KickMaintenance("on") || len(kick) != 1 {
		t.Fatal("a second kick before the pass did not coalesce")
	}
	if p.KickMaintenance("off") || p.KickMaintenance("absent") {
		t.Fatal("a kick reported success with no maintenance loop")
	}
}
