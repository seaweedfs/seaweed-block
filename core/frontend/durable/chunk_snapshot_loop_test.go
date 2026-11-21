package durable

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// snapStubLayer is an attached, unfenced layer with no absent chunks.
type snapStubLayer struct{ storage.ChunkLayer }

func (snapStubLayer) Fenced() bool                   { return false }
func (snapStubLayer) Stats() storage.ChunkLayerStats { return storage.ChunkLayerStats{} }

// stepClock is a wall clock a test can step.
type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) get() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *stepClock) set(t time.Time) { c.mu.Lock(); c.now = t; c.mu.Unlock() }

// snapLoopFixture is a provider running the scheduled-snapshot loop of one
// volume on a stepped clock, with a 10 ms wake bound. take is called for
// every attempt; the loop is stopped and joined at cleanup.
func snapLoopFixture(t *testing.T, take func(context.Context) error) (*DurableProvider, *volHandle, *stepClock) {
	t.Helper()
	wake := snapshotWake
	snapshotWake = 10 * time.Millisecond
	t.Cleanup(func() { snapshotWake = wake })
	clock := &stepClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}
	layer := &snapStubLayer{}
	h := &volHandle{chunk: layer, chunkLineage: chunkLineage{"v1", "r1", 3, 1}}
	p := &DurableProvider{volumes: map[string]*volHandle{"v1": h}, view: precopyQueueView{id}, testSnapshotClock: clock.get,
		cfg: ProviderConfig{ChunkSnapshotPeriod: 24 * time.Hour,
			ChunkScheduledSnapshot: func(ctx context.Context, _ string, _ time.Time, _ func(context.Context) (storage.ChunkHead, error)) error {
				return take(ctx)
			}}}
	p.mu.Lock()
	p.startChunkLoop("v1", h, layer)
	p.mu.Unlock()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { p.stopChunkLoop(h); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the snapshot loop was not joined")
		}
	})
	return p, h, clock
}

func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s not reached", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A wall-clock step forwards past the due time is observed within the wake
// bound: the attempt runs without waiting out the elapsed-time timer, and
// its success is recorded at the stepped wall time.
func TestScheduledSnapshotLoop_ForwardWallStepIsObserved(t *testing.T) {
	var calls atomic.Int64
	p, h, clock := snapLoopFixture(t, func(context.Context) error { calls.Add(1); return nil })
	due := h.snapshots.status(clock.get()).NextDueNs
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("an attempt ran before its slot")
	}
	stepped := time.Unix(0, due).Add(time.Second)
	clock.set(stepped)
	waitUntil(t, "the attempt after a forward step", func() bool { return calls.Load() == 1 })
	waitUntil(t, "the recorded success", func() bool { return h.snapshots.status(p.snapshotNow()).LastOkNs == stepped.UnixNano() })
}

// A wall-clock step backwards realigns the next attempt to the first slot
// after the stepped now (it does not wait the step out), and that slot is
// then taken.
func TestScheduledSnapshotLoop_BackwardWallStepRealigns(t *testing.T) {
	var calls atomic.Int64
	_, h, clock := snapLoopFixture(t, func(context.Context) error { calls.Add(1); return nil })
	back := clock.get().Add(-3 * 24 * time.Hour)
	clock.set(back)
	waitUntil(t, "the realigned slot", func() bool {
		next := time.Unix(0, h.snapshots.status(clock.get()).NextDueNs)
		return next.After(back) && next.Sub(back) <= 24*time.Hour
	})
	if calls.Load() != 0 {
		t.Fatal("a backward step triggered an attempt")
	}
	clock.set(time.Unix(0, h.snapshots.status(clock.get()).NextDueNs).Add(time.Second))
	waitUntil(t, "the attempt at the realigned slot", func() bool { return calls.Load() == 1 })
}

// Overdue follows the wall clock: a success recorded by the loop, then a
// wall step forwards past one period plus the retry cap, reads as overdue.
func TestScheduledSnapshotLoop_OverdueFollowsTheWallClock(t *testing.T) {
	var calls atomic.Int64
	p, h, clock := snapLoopFixture(t, func(context.Context) error { calls.Add(1); return nil })
	clock.set(time.Unix(0, h.snapshots.status(clock.get()).NextDueNs))
	waitUntil(t, "the first success", func() bool { return h.snapshots.status(p.snapshotNow()).LastOkNs != 0 })
	clock.set(clock.get().Add(2 * 24 * time.Hour))
	if !h.snapshots.status(p.snapshotNow()).Overdue {
		t.Fatal("two days after the last success on the wall clock, not overdue")
	}
}

// A re-armed loop (stopped and started again on the same handle, as after a
// refused detach) keeps the schedule: the failed attempt, its count and
// backoff survive.
func TestScheduledSnapshotLoop_RearmKeepsTheSchedule(t *testing.T) {
	p, h, clock := snapLoopFixture(t, func(context.Context) error { return &SnapshotFailure{Code: "remote_unavailable"} })
	clock.set(time.Unix(0, h.snapshots.status(clock.get()).NextDueNs))
	waitUntil(t, "the failure", func() bool { return h.snapshots.status(clock.get()).Attempts == 1 })
	slot := h.snapshots
	p.stopChunkLoop(h)
	p.mu.Lock()
	p.startChunkLoop("v1", h, h.chunk)
	p.mu.Unlock()
	if st := h.snapshots.status(clock.get()); h.snapshots != slot || st.Attempts != 1 || st.LastFailure == nil {
		t.Fatalf("after re-arm: %+v (same schedule %v); want the failure kept", st, h.snapshots == slot)
	}
}

// INV-MAINT-JOIN for the scheduled-snapshot worker: stopping the loop
// cancels an attempt in flight and returns only after the attempt has
// returned.
func TestScheduledSnapshotLoop_StopJoinsTheAttempt(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(finish)
	p, h, clock := snapLoopFixture(t, func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		<-release // returns only when the test allows it
		return errors.New("stopped")
	})
	clock.set(time.Unix(0, h.snapshots.status(clock.get()).NextDueNs))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no attempt started")
	}
	stopped := make(chan struct{})
	go func() { p.stopChunkLoop(h); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned while the attempt was still running")
	case <-time.After(100 * time.Millisecond):
	}
	finish()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return after the attempt")
	}
}

// The production clock carries no monotonic reading, so durations and
// overdue between its readings follow wall-clock steps.
func TestScheduledSnapshotWallClockHasNoMonotonicReading(t *testing.T) {
	now := snapshotWallNow()
	if now != now.Round(0) {
		t.Fatalf("snapshotWallNow %v carries a monotonic reading", now)
	}
	if p := (&DurableProvider{}); p.snapshotNow() != p.snapshotNow().Round(0) {
		t.Fatal("the provider's snapshot clock carries a monotonic reading")
	}
}
