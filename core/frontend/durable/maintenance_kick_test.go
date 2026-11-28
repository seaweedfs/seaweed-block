package durable

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// countingPass counts maintenance passes; fail(n) is pass n's result and
// hold(n), when set, blocks pass n until it returns.
type countingPass struct {
	mu   sync.Mutex
	n    chan int
	seen int
	fail func(n int) error
	hold func(n int)
}

func (c *countingPass) run(context.Context, string) error {
	c.mu.Lock()
	c.seen++
	n := c.seen
	c.mu.Unlock()
	c.n <- n
	if c.hold != nil {
		c.hold(n)
	}
	if c.fail != nil {
		return c.fail(n)
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

func runLoop(t *testing.T, p *DurableProvider, kick chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.chunkMaintenanceLoop(ctx, "v1", maintenanceStubLayer{}, func() bool { return true }, kick, nil)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the maintenance loop did not stop")
		}
	})
}

// With the shipped hour-long interval, a kick (as after a snapshot delete)
// runs one pass at once, and a pass is not repeated until the next kick.
func TestMaintenanceKickRunsAPassNow(t *testing.T) {
	c := &countingPass{n: make(chan int, 8)}
	p := &DurableProvider{cfg: ProviderConfig{ChunkMaintenanceInterval: time.Hour, ChunkMaintenance: c.run}}
	kick := make(chan struct{}, 1)
	runLoop(t, p, kick)
	c.none(t, 200*time.Millisecond) // the first periodic pass is an hour away
	kick <- struct{}{}
	if n := c.next(t, 5*time.Second); n != 1 {
		t.Fatalf("pass %d after the kick; want 1", n)
	}
	c.none(t, 200*time.Millisecond)
	kick <- struct{}{}
	if n := c.next(t, 5*time.Second); n != 2 {
		t.Fatalf("pass %d after the second kick; want 2", n)
	}
	c.none(t, 200*time.Millisecond)
}

// Kicks given while a pass runs coalesce into exactly one follow-up pass.
func TestMaintenanceKicksDuringAPassCoalesce(t *testing.T) {
	release := make(chan struct{})
	c := &countingPass{n: make(chan int, 8), hold: func(n int) {
		if n == 1 {
			<-release
		}
	}}
	p := &DurableProvider{cfg: ProviderConfig{ChunkMaintenanceInterval: time.Hour, ChunkMaintenance: c.run}}
	kick := make(chan struct{}, 1)
	runLoop(t, p, kick)
	kick <- struct{}{}
	if n := c.next(t, 5*time.Second); n != 1 {
		t.Fatalf("pass %d; want 1", n)
	}
	for i := 0; i < 5; i++ { // the pass is held: these coalesce into one token
		select {
		case kick <- struct{}{}:
		default:
		}
	}
	close(release)
	if n := c.next(t, 5*time.Second); n != 2 {
		t.Fatalf("pass %d after the held pass; want one follow-up (2)", n)
	}
	c.none(t, 300*time.Millisecond)
}

// A kicked pass the volume refused as busy (a cut session open: the
// cleaning never runs during one) keeps the kick pending: every retry comes
// after the short delay, however many times it is refused, until a pass is
// not refused; then the loop is back on the interval.
func TestMaintenanceKickedBusyPassRetriesSoon(t *testing.T) {
	old := maintenanceKickRetry
	maintenanceKickRetry = 50 * time.Millisecond
	defer func() { maintenanceKickRetry = old }()
	c := &countingPass{n: make(chan int, 8), fail: func(n int) error {
		if n <= 2 {
			return fmt.Errorf("cleaning: %w", layout.ErrBusy)
		}
		return nil
	}}
	p := &DurableProvider{cfg: ProviderConfig{ChunkMaintenanceInterval: time.Hour, ChunkMaintenance: c.run}}
	kick := make(chan struct{}, 1)
	runLoop(t, p, kick)
	kick <- struct{}{}
	for want := 1; want <= 3; want++ {
		if n := c.next(t, 5*time.Second); n != want {
			t.Fatalf("pass %d; want %d (busy, busy, then the pass that succeeds)", n, want)
		}
	}
	c.none(t, 300*time.Millisecond) // the kick is served: back to the interval
}

// KickMaintenance reaches only a volume whose maintenance loop runs: not
// one without the loop, not one whose loop was stopped, not one whose loop
// ended (a fence or lineage change) without being re-armed.
func TestKickMaintenanceReachesOnlyARunningLoop(t *testing.T) {
	running, ended := make(chan struct{}), make(chan struct{})
	close(ended)
	noop := func() {}
	kicks := map[string]chan struct{}{"running": make(chan struct{}, 1), "stopped": make(chan struct{}, 1), "ended": make(chan struct{}, 1)}
	p := &DurableProvider{volumes: map[string]*volHandle{
		"running": {maintKick: kicks["running"], chunkStop: noop, chunkDone: running},
		"stopped": {maintKick: kicks["stopped"]},
		"ended":   {maintKick: kicks["ended"], chunkStop: noop, chunkDone: ended},
		"off":     {chunkStop: noop, chunkDone: running},
	}}
	if !p.KickMaintenance("running") || len(kicks["running"]) != 1 {
		t.Fatal("a kick did not reach the running loop")
	}
	if !p.KickMaintenance("running") || len(kicks["running"]) != 1 {
		t.Fatal("a second kick before the pass did not coalesce")
	}
	for _, v := range []string{"stopped", "ended", "off", "absent"} {
		if p.KickMaintenance(v) {
			t.Fatalf("a kick to %q reported a running loop", v)
		}
	}
	if len(kicks["stopped"]) != 0 || len(kicks["ended"]) != 0 {
		t.Fatal("a kick was queued for a loop that is not running")
	}
}
