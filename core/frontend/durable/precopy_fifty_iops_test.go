package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Backend Write+Sync models the guest's 4KiB synchronous command stream. The
// peer reply is held until a separate control probe witnesses closed admission,
// then released after20ms. This is a forced schedule, not physical NVMe proof.
func TestPrecopyFiftyIOPSSyncConvergesFirstAttempt(t *testing.T) {
	precopyFiftyIOPS(t, 3, false)
}

func TestPrecopyFiftyIOPSColdCostsKeepAdmissionHonest(t *testing.T) {
	precopyFiftyIOPS(t, 1, true)
}

func precopyFiftyIOPS(t *testing.T, samples byte, wantUnknown bool) {
	t.Helper()
	remote := storage.NewMemChunkRemote()
	p := precopyProvider(t, remote, &parkView{stubView: v1View()})
	openRecovered(t, p)
	backend, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(1); i <= samples; i++ {
		if _, err := backend.Write(context.Background(), 4096, block(i)); err != nil {
			t.Fatal(err)
		}
		if err := backend.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	releasePeer := func() { releaseOnce.Do(func() { close(release) }) }
	if err := backend.(*durable.StorageBackend).SetWriteObserver(writeObserverFunc{sync: func(ctx context.Context, _ uint64) error {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	writerDone, probeDone := make(chan struct{}), make(chan struct{})
	var stopped atomic.Bool
	var guestErrors atomic.Int64
	t.Cleanup(func() {
		stopped.Store(true)
		releasePeer()
		cancel()
		for _, done := range []chan struct{}{writerDone, probeDone} {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("writer/probe did not join")
			}
		}
	})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			_, err := backend.Write(ctx, 4096, block(0x5b))
			if err == nil {
				err = backend.Sync(ctx)
			}
			if err != nil {
				if !stopped.Load() {
					guestErrors.Add(1)
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		close(probeDone)
		t.Fatal("synchronous writer not admitted")
	}
	go func() {
		defer close(probeDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			probe, stop := context.WithTimeout(ctx, 5*time.Millisecond)
			_, err := backend.Write(probe, 8192, block(0x6d))
			stop()
			if errors.Is(err, frontend.ErrBackendBusy) && errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(20 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					releasePeer()
				case <-ctx.Done():
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	result, _, err := p.DetachVolume(ctx, "v1")
	stopped.Store(true)
	t.Logf("attempts=%d drain=%s estimate=%s frozen=%t guest_errors=%d", result.Precopy.Attempts, result.Precopy.Drain, result.Precopy.Estimate, result.Precopy.Frozen, guestErrors.Load())
	if wantUnknown {
		if result.Precopy.Frozen {
			// Staging or later rounds may leave an empty tail, which needs
			// only measured fixed cost. A nonempty first tail still needs
			// three upload samples; the one-sample fixture cannot supply them.
			if err != nil || (result.Precopy.Attempts == 1 && result.Precopy.TailBytes != 0) || guestErrors.Load() != 0 || !bytes.Equal(headBlock(t, remote, 1), block(0x5b)) {
				t.Fatalf("cold samples claimed premature fit: report=%+v guest_errors=%d err=%v", result.Precopy, guestErrors.Load(), err)
			}
			return
		}
		if !errors.Is(err, storage.ErrPrecopyUnknown) || guestErrors.Load() != 0 {
			t.Fatalf("unknown cost did not preserve live service: report=%+v guest_errors=%d err=%v", result.Precopy, guestErrors.Load(), err)
		}
		if _, err := backend.Write(ctx, 12288, block(0x7d)); err != nil {
			t.Fatal("unknown-cost refusal stopped the frontend:", err)
		}
		return
	}
	if err != nil || !result.Precopy.Frozen || result.Precopy.Attempts != 1 {
		t.Fatalf("50IOPS cutover: report=%+v err=%v", result.Precopy, err)
	}
	if guestErrors.Load() != 0 {
		t.Fatalf("guest errors before cutover: %d", guestErrors.Load())
	}
	if !bytes.Equal(headBlock(t, remote, 1), block(0x5b)) {
		t.Fatal("final cut lost synchronous writer bytes")
	}
}
