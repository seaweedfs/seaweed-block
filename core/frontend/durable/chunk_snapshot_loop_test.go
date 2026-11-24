package durable

// The scheduled-snapshot loop on virtual time (testing/synctest): the
// bubble's clock is the monotonic clock the schedule runs on.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// snapStubLayer is an attached, unfenced layer with no absent chunks.
type snapStubLayer struct{ storage.ChunkLayer }

func (snapStubLayer) Fenced() bool                   { return false }
func (snapStubLayer) Stats() storage.ChunkLayerStats { return storage.ChunkLayerStats{} }

// snapLoop sets up volume v1's scheduled-snapshot loop (period one day) in
// the current bubble; take runs each attempt, and a successful attempt's
// slot becomes the newest published scheduled snapshot. start arms the
// loop; stop stops it and reports whether it was joined within an hour of
// virtual time.
func snapLoop(take func(context.Context) error) (h *volHandle, start func(), stop func() bool) {
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}
	layer := &snapStubLayer{}
	h = &volHandle{chunk: layer, chunkLineage: chunkLineage{"v1", "r1", 3, 1}}
	var mu sync.Mutex
	var newest time.Time
	p := &DurableProvider{volumes: map[string]*volHandle{"v1": h}, view: precopyQueueView{id},
		cfg: ProviderConfig{ChunkSnapshotPeriod: 24 * time.Hour,
			ChunkScheduledNewest: func(context.Context, string) (time.Time, error) {
				mu.Lock()
				defer mu.Unlock()
				return newest, nil
			},
			ChunkScheduledSnapshot: func(ctx context.Context, _ string, at time.Time, _ func(context.Context) (storage.ChunkHead, error)) error {
				err := take(ctx)
				if err == nil {
					mu.Lock()
					newest = at
					mu.Unlock()
				}
				return err
			}}}
	start = func() {
		p.mu.Lock()
		p.startChunkLoop("v1", h, layer)
		p.mu.Unlock()
	}
	stop = func() bool {
		done := make(chan struct{})
		go func() { p.stopChunkLoop(h); close(done) }()
		select {
		case <-done:
			return true
		case <-time.After(time.Hour):
			return false
		}
	}
	return h, start, stop
}

// recv waits for ch for at most an hour of virtual time.
func recv(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Hour):
		t.Fatalf("%s did not happen", what)
	}
}

// The first attempt is at once, for the slot holding the start; each later
// one at the start of the next slot, one period apart.
func TestScheduledSnapshotLoop_TakesEachSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		h, start, stop := snapLoop(func(context.Context) error { calls.Add(1); return nil })
		start()
		defer func() {
			if !stop() {
				t.Error("the loop was not joined")
			}
		}()
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatalf("%d attempts at the start; want 1 at once", calls.Load())
		}
		next := h.snapshots.due()
		if st := h.snapshots.status(time.Now()); next.Sub(time.Unix(0, st.NewestNs)) != 24*time.Hour {
			t.Fatalf("next decision %v after the first slot; want one period", next.Sub(time.Unix(0, st.NewestNs)))
		}
		time.Sleep(time.Until(next) - time.Second)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatal("an attempt ran before the next slot")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if calls.Load() != 2 {
			t.Fatalf("%d attempts after the next slot began; want 2", calls.Load())
		}
	})
}

// A re-armed loop (stopped and started again on the same handle, as after
// a refused detach) keeps the volume's schedule: the failure, its count and
// its retry time survive, and the retry runs at that time.
func TestScheduledSnapshotLoop_RearmKeepsTheSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		h, start, stop := snapLoop(func(context.Context) error {
			calls.Add(1)
			return &SnapshotFailure{Code: "remote_unavailable"}
		})
		start()
		running := true
		defer func() { // registered before any check: whichever loop runs is joined
			if running && !stop() {
				t.Error("the loop was not joined")
			}
		}()
		time.Sleep(time.Until(h.snapshots.due()) + time.Second)
		synctest.Wait()
		slot, retry := h.snapshots, h.snapshots.due()
		if st := slot.status(time.Now()); st.Attempts != 1 || st.LastFailure == nil {
			t.Fatalf("after the first attempt: %+v; want one failure", st)
		}
		running = false
		if !stop() {
			t.Fatal("the loop was not joined")
		}
		start()
		running = true
		if h.snapshots != slot || h.snapshots.due() != retry || slot.status(time.Now()).Attempts != 1 {
			t.Fatalf("after re-arm: same schedule %v, due %v (want %v), %+v", h.snapshots == slot, h.snapshots.due(), retry, slot.status(time.Now()))
		}
		time.Sleep(time.Until(retry) + time.Second)
		synctest.Wait()
		if calls.Load() != 2 || slot.status(time.Now()).Attempts != 2 {
			t.Fatalf("%d attempts after the retry time; want the re-armed loop to retry once", calls.Load())
		}
	})
}

// INV-MAINT-JOIN for the scheduled-snapshot worker: stopping the loop
// cancels an attempt in flight (the attempt observes the cancellation) and
// returns only after the attempt has returned. The attempt is released
// before the loop is joined, whatever the outcome.
func TestScheduledSnapshotLoop_StopJoinsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		h, start, stop := snapLoop(func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			<-release // returns only when the test allows it
			return errors.New("stopped")
		})
		start()
		joined := make(chan bool, 1)
		stopStarted := false
		var released atomic.Bool
		free := func() {
			if released.CompareAndSwap(false, true) {
				close(release)
			}
		}
		// Registered before any check, run last: release the attempt, then
		// join the stop (bounded), starting it if the test never did.
		defer func() {
			free()
			if !stopStarted {
				joined <- stop()
			}
			select {
			case ok := <-joined:
				if !ok {
					t.Error("stop was not joined after the attempt returned")
				}
			case <-time.After(2 * time.Hour):
				t.Error("stop did not return")
			}
		}()
		time.Sleep(time.Until(h.snapshots.due()) + time.Second)
		recv(t, entered, "the attempt")
		stopStarted = true
		go func() { joined <- stop() }()
		recv(t, cancelled, "the attempt seeing the cancellation")
		synctest.Wait()
		select {
		case ok := <-joined:
			joined <- ok // leave it for the deferred join
			t.Fatal("stop returned while the attempt was still running")
		default:
		}
		free()
	})
}

// With every attempt failing, the schedule is overdue once no success
// came within one period plus the retry cap from the start; never before.
func TestScheduledSnapshotLoop_OverdueWithNoSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, start, stop := snapLoop(func(context.Context) error { return &SnapshotFailure{Code: "remote_unavailable"} })
		begin := time.Now()
		start()
		defer func() {
			if !stop() {
				t.Error("the loop was not joined")
			}
		}()
		limit := 24*time.Hour + snapshotRetryMax
		time.Sleep(time.Until(begin.Add(limit)))
		synctest.Wait()
		if st := h.snapshots.status(time.Now()); st.Overdue || st.LastOkNs != 0 {
			t.Fatalf("at one period plus the retry cap: %+v; want not yet overdue", st)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if st := h.snapshots.status(time.Now()); !st.Overdue || st.LastFailure == nil {
			t.Fatalf("past one period plus the retry cap with only failures: %+v; want overdue with the failure", st)
		}
	})
}
