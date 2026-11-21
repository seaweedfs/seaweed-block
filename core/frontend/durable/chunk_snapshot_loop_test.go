package durable

// The scheduled-snapshot loop on virtual time (testing/synctest): the
// bubble's clock is the monotonic clock the schedule runs on.

import (
	"context"
	"errors"
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
// the current bubble; take runs each attempt. start arms the loop; stop
// stops it and reports whether it was joined within an hour of virtual
// time.
func snapLoop(take func(context.Context) error) (h *volHandle, start func(), stop func() bool) {
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}
	layer := &snapStubLayer{}
	h = &volHandle{chunk: layer, chunkLineage: chunkLineage{"v1", "r1", 3, 1}}
	p := &DurableProvider{volumes: map[string]*volHandle{"v1": h}, view: precopyQueueView{id},
		cfg: ProviderConfig{ChunkSnapshotPeriod: 24 * time.Hour,
			ChunkScheduledSnapshot: func(ctx context.Context, _ string, _ time.Time, _ func(context.Context) (storage.ChunkHead, error)) error {
				return take(ctx)
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

// The first attempt is at the start plus the volume's offset and the next
// one a period later, both on the monotonic clock.
func TestScheduledSnapshotLoop_TakesEachSlotOnMonotonicTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		h, start, stop := snapLoop(func(context.Context) error { calls.Add(1); return nil })
		begin := time.Now()
		start()
		defer func() {
			if !stop() {
				t.Error("the loop was not joined")
			}
		}()
		first := h.snapshots.due()
		if off := first.Sub(begin); off < 0 || off >= 24*time.Hour {
			t.Fatalf("first slot %v after the start; want within one period", off)
		}
		time.Sleep(time.Until(first) - time.Second)
		synctest.Wait()
		if calls.Load() != 0 {
			t.Fatal("an attempt ran before its slot")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatalf("%d attempts at the first slot; want 1", calls.Load())
		}
		if next := h.snapshots.due(); next.Sub(first) != 24*time.Hour {
			t.Fatalf("next slot %v after the first; want one period", next.Sub(first))
		}
		time.Sleep(24 * time.Hour)
		synctest.Wait()
		if calls.Load() != 2 {
			t.Fatalf("%d attempts after one period; want 2", calls.Load())
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
		time.Sleep(time.Until(h.snapshots.due()) + time.Second)
		synctest.Wait()
		slot, retry := h.snapshots, h.snapshots.due()
		if st := slot.status(time.Now()); st.Attempts != 1 || st.LastFailure == nil {
			t.Fatalf("after the first attempt: %+v; want one failure", st)
		}
		if !stop() {
			t.Fatal("the loop was not joined")
		}
		start()
		defer func() {
			if !stop() {
				t.Error("the re-armed loop was not joined")
			}
		}()
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
		stopping := false
		defer func() { // runs after the release below: join the stop, bounded
			if !stopping {
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
		var released atomic.Bool
		defer func() {
			if released.CompareAndSwap(false, true) {
				close(release)
			}
		}()
		time.Sleep(time.Until(h.snapshots.due()) + time.Second)
		<-entered
		stopping = true
		go func() { joined <- stop() }()
		<-cancelled // the attempt saw the cancellation
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("stop returned while the attempt was still running")
		default:
		}
		if released.CompareAndSwap(false, true) {
			close(release)
		}
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
