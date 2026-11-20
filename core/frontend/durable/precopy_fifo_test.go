package durable

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

type precopyQueueView struct{ id frontend.Identity }

func (v precopyQueueView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: v.id.VolumeID, ReplicaID: v.id.ReplicaID, Epoch: v.id.Epoch, EndpointVersion: v.id.EndpointVersion, Healthy: true}
}

type precopyQueueObserver struct {
	observe func(byte)
	sync    func(context.Context) error
}

func (o precopyQueueObserver) Observe(_ context.Context, _ uint32, _ uint64, p []byte) error {
	if o.observe != nil {
		o.observe(p[0])
	}
	return nil
}
func (o precopyQueueObserver) Sync(ctx context.Context, _ uint64) error {
	if o.sync != nil {
		return o.sync(ctx)
	}
	return nil
}
func precopyMemoryBackend(t *testing.T) *StorageBackend {
	t.Helper()
	s := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = s.Close() })
	id := frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1}
	b := NewStorageBackend(s, precopyQueueView{id}, id)
	b.SetOperational(true, "test")
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestPrecopyQueueResumesInFIFOOrder(t *testing.T) {
	b := precopyMemoryBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	queued := make(chan struct{}, 4)
	b.testPrecopyQueued = func() { queued <- struct{}{} }
	first, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unpark := func() { once.Do(func() { close(release) }) }
	seen := make(chan byte, 4)
	if err := b.SetWriteObserver(precopyQueueObserver{observe: func(v byte) {
		seen <- v
		if v == 1 {
			close(first)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}}); err != nil {
		t.Fatal(err)
	}
	resume, err := b.BeginPrecopyPause(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	joined := make(chan struct{})
	errs := make(chan error, 4)
	t.Cleanup(func() {
		cancel()
		unpark()
		resume()
		_ = b.Close()
		go func() { wg.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("queue workers did not join")
		}
	})
	start := func(v byte) {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := b.Write(ctx, 0, bytes.Repeat([]byte{v}, 4096)); errs <- err }()
		select {
		case <-queued:
		case <-ctx.Done():
			t.Fatal("command was not queued")
		}
	}
	start(1)
	start(2)
	start(3)
	resume()
	select {
	case <-first:
	case <-ctx.Done():
		t.Fatal("first queued write did not enter")
	}
	start(4) // New arrivals cannot bypass the old queue during an aborted cut.
	if v := <-seen; v != 1 {
		t.Fatalf("first write=%d", v)
	}
	select {
	case v := <-seen:
		t.Fatalf("write %d bypassed held FIFO head", v)
	case <-time.After(30 * time.Millisecond):
	}
	unpark()
	for _, want := range []byte{2, 3, 4} {
		select {
		case got := <-seen:
			if got != want {
				t.Fatalf("write=%d want%d", got, want)
			}
		case <-ctx.Done():
			t.Fatal("FIFO queue did not drain")
		}
	}
	for i := 0; i < 4; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("queued write did not finish")
		}
	}
	data := make([]byte, 4096)
	if _, err := b.Read(ctx, 0, data); err != nil || data[0] != 4 {
		t.Fatalf("ordered final bytes=%d err=%v", data[0], err)
	}
}

func TestPrecopyDrainTimeoutReopensWithoutWaitingUnderCapture(t *testing.T) {
	b := precopyMemoryBackend(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unpark := func() { once.Do(func() { close(release) }) }
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(func() {
		unpark()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Sync did not join")
		}
	})
	if err := b.SetWriteObserver(precopyQueueObserver{sync: func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	go func() { defer close(done); _ = b.Sync(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("Sync not admitted")
	}
	drain, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	resume, err := b.BeginPrecopyPause(drain)
	if !errors.Is(err, context.DeadlineExceeded) || resume != nil {
		t.Fatalf("drain=%v release=%v", err, resume != nil)
	}
	if _, err := b.Write(ctx, 4096, bytes.Repeat([]byte{9}, 4096)); err != nil {
		t.Fatal("drain refusal did not reopen admission:", err)
	}
	unpark()
}

func TestPrecopyQueueUsesFiveSecondCutoverBudget(t *testing.T) {
	b := precopyMemoryBackend(t)
	if !b.TryStopIO("final cut") {
		t.Fatal("freeze failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	done := make(chan struct{})
	var result error
	t.Cleanup(func() {
		cancel()
		b.SetOperational(false, "cleanup")
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("queue did not join")
		}
	})
	go func() { defer close(done); _, result = b.Write(ctx, 0, make([]byte, 4096)) }()
	select {
	case <-done:
		t.Fatalf("queue expired before cutover budget: %v", result)
	case <-time.After(3200 * time.Millisecond):
	}
	select {
	case <-done:
		if !errors.Is(result, frontend.ErrBackendBusy) || !errors.Is(result, context.DeadlineExceeded) {
			t.Fatalf("deadline=%v", result)
		}
	case <-ctx.Done():
		t.Fatal("five-second queue budget was not enforced")
	}
}
