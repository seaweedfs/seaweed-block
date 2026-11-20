package durable

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cumulativeReadStorage struct {
	storage.LogicalStorage
	remaining time.Duration
}

func (s *cumulativeReadStorage) Read(uint32) ([]byte, error) {
	time.Sleep(2750 * time.Millisecond)
	return make([]byte, 4096), nil
}
func (s *cumulativeReadStorage) ReadContext(ctx context.Context, _ uint32) ([]byte, error) {
	deadline, _ := ctx.Deadline()
	s.remaining = time.Until(deadline)
	timer := time.NewTimer(2750 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return make([]byte, 4096), nil
	case <-ctx.Done():
		return nil, errors.Join(storage.ErrChunkHydrationPending, ctx.Err())
	}
}
func TestAttachAndHydrationUseOneArrivalDeadline(t *testing.T) {
	id := frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1}
	store := &cumulativeReadStorage{}
	b := &StorageBackend{storage: store, blockSize: 4096, id: id, view: readinessProjection{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1, Healthy: true}}
	b.setChunkAttachPending("pending")
	var once sync.Once
	var timer *time.Timer
	fired := make(chan struct{})
	b.testBeforeReadReadyWait = func() {
		once.Do(func() {
			timer = time.AfterFunc(2500*time.Millisecond, func() { defer close(fired); b.SetOperational(true, "attached") })
		})
	}
	defer func() {
		if timer != nil && !timer.Stop() {
			<-fired
		}
	}()
	start := time.Now()
	n, err := b.Read(context.Background(), 0, make([]byte, 4096))
	elapsed := time.Since(start)
	if n != 0 || !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) || elapsed >= 5*time.Second || store.remaining > 2*time.Second {
		t.Fatalf("n=%d err=%v elapsed=%s hydration remaining=%s", n, err, elapsed, store.remaining)
	}
	t.Logf("combined attach+hydration: %s; remaining at hydration: %s", elapsed, store.remaining)
}

// Both the command context and the capped attach context use this same
// absolute deadline. Hold the registered wait until it has expired so
// notification selection cannot accidentally make the oracle pass.
type combinedReadProbe struct {
	storage.LogicalStorage
	calls atomic.Int32
}

func (s *combinedReadProbe) Read(uint32) ([]byte, error) {
	s.calls.Add(1)
	return make([]byte, 4096), nil
}
func (s *combinedReadProbe) ReadContext(context.Context, uint32) ([]byte, error) {
	s.calls.Add(1)
	return make([]byte, 4096), nil
}
func TestCombinedRead_TiedAttachAndCommandDeadline(t *testing.T) {
	for _, state := range []string{"pending", "published", "fenced"} {
		t.Run(state, func(t *testing.T) {
			id := frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1}
			probe := &combinedReadProbe{}
			b := &StorageBackend{storage: probe, blockSize: 4096, id: id, view: readinessProjection{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1, Healthy: true}}
			b.setChunkAttachPending("pending")
			// Queue residence consumes three seconds of the original four-second
			// allowance. The attach child's three-second limit is capped by it.
			ctx, cancel := frontend.WithReadBudget(context.Background(), time.Now().Add(-3*time.Second))
			defer cancel()
			registered, resume, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(resume) }) }
			b.testBeforeReadReadyWait = func() { once.Do(func() { close(registered) }); <-resume }
			result := make(chan error, 1)
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-joined:
				case <-time.After(time.Second):
					t.Error("combined read did not settle")
				}
			})
			start := time.Now()
			go func() {
				defer close(joined)
				n, err := b.Read(ctx, 0, make([]byte, 4096))
				if n != 0 {
					t.Errorf("expired read returned %d bytes", n)
				}
				result <- err
			}()
			select {
			case <-registered:
			case <-ctx.Done():
				t.Fatal("attach waiter not registered before deadline")
			}
			<-ctx.Done()
			switch state {
			case "published":
				b.SetOperational(true, "attached at expiry")
			case "fenced":
				b.mu.Lock()
				b.chunkFenced = func() bool { return true }
				b.mu.Unlock()
				b.SetOperational(false, "fenced at expiry")
			}
			release()
			select {
			case err := <-result:
				if state == "fenced" {
					if !errors.Is(err, frontend.ErrStalePrimary) || errors.Is(err, frontend.ErrBackendBusy) {
						t.Fatalf("deadline hid fence: %v", err)
					}
				} else if !errors.Is(err, frontend.ErrNotReady) || !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, storage.ErrChunkHydrationPending) {
					t.Fatalf("attach classification must win before hydration: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("expired combined read did not return")
			}
			if probe.calls.Load() != 0 {
				t.Fatal("expired command entered storage")
			}
			if time.Since(start) >= 5*time.Second {
				t.Fatal("cooperative combined deadline exceeded command budget")
			}
		})
	}
}
func TestCombinedRead_ExpiryAfterAdmissionKeepsAttachClassification(t *testing.T) {
	id := frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1}
	probe := &combinedReadProbe{}
	b := &StorageBackend{storage: probe, blockSize: 4096, id: id, view: readinessProjection{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1, Healthy: true}}
	b.SetOperational(true, "attached")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	b.testBeforeIOLock = func() { <-ctx.Done() }
	n, err := b.Read(ctx, 0, make([]byte, 4096))
	if n != 0 || probe.calls.Load() != 0 || !errors.Is(err, frontend.ErrNotReady) || !errors.Is(err, frontend.ErrBackendBusy) || errors.Is(err, storage.ErrChunkHydrationPending) {
		t.Fatalf("post-admission expiry: n=%d calls=%d err=%v", n, probe.calls.Load(), err)
	}
}
