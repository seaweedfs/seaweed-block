package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// External observers see the committed takeover HEAD while attachment is
// held in the provider-owned worker, without a separate test goroutine.
type heldTakeoverClaim struct {
	storage.ChunkRemote
	claimed           chan struct{}
	release           chan struct{}
	once, releaseOnce sync.Once
	holdFor           time.Duration
	readRegistered    chan struct{}
}

func (r *heldTakeoverClaim) unblock() { r.releaseOnce.Do(func() { close(r.release) }) }
func (r *heldTakeoverClaim) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	gen, err := r.ChunkRemote.CASHead(ctx, expected, next)
	if err != nil || next.Epoch != 4 {
		return gen, err
	}
	r.once.Do(func() { close(r.claimed) })
	var expired <-chan time.Time
	if r.holdFor > 0 {
		select {
		case <-r.readRegistered:
		case <-r.release:
			return gen, nil
		case <-ctx.Done():
			return gen, ctx.Err()
		}
		timer := time.NewTimer(r.holdFor)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case <-r.release:
		return gen, nil
	case <-expired:
		return gen, nil
	case <-ctx.Done():
		return gen, ctx.Err()
	}
}

func pendingTakeoverRead(t *testing.T, hold time.Duration) (*durable.DurableProvider, frontend.Backend, *nvme.IOHandler) {
	t.Helper()
	remote := &heldTakeoverClaim{ChunkRemote: seededRemote(t), claimed: make(chan struct{}), release: make(chan struct{}), holdFor: hold, readRegistered: make(chan struct{})}
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	p := chunkProvider(t, t.TempDir(), remote, view, 0)
	// Registered after provider cleanup, so release precedes its worker join.
	t.Cleanup(remote.unblock)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 2}))
	select {
	case <-remote.claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("takeover CAS never became visible")
	}
	head, err := remote.GetHead(context.Background())
	if err != nil || head.Epoch != 4 || head.Holder != "r1" {
		t.Fatalf("takeover HEAD: %+v, %v", head, err)
	}
	backend, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	var firstWait sync.Once
	durable.SetTestBeforeAttachWait(backend, func() { firstWait.Do(func() { close(remote.readRegistered) }) })
	handler := nvme.NewIOHandler(nvme.HandlerConfig{Backend: backend, BlockSize: 4096, VolumeSize: pcBlocks * 4096, NSID: 1})
	return p, backend, handler
}

// Model Linux max_retries=5 and CRD=0: at most six attempts, no sleeps.
// A status-only fix exhausts them during attachment and becomes guest EIO.
func TestWALStoreChunk_TakeoverReadBeforeAttachIsRetryable(t *testing.T) {
	_, _, handler := pendingTakeoverRead(t, 2500*time.Millisecond)
	command := nvme.IOCommand{Opcode: 0x02, NSID: 1, SLBA: 33, NLB: 1}
	start := time.Now()
	for attempt := 0; attempt <= 5; attempt++ {
		commandStart := time.Now()
		result := handler.Handle(context.Background(), command)
		if elapsed := time.Since(commandStart); elapsed >= 5*time.Second {
			t.Fatalf("guest command took %s", elapsed)
		}
		if result.AsError() == nil {
			if !bytes.Equal(result.Data, block(0x33)) {
				t.Fatal("guest read returned incorrect HEAD bytes")
			}
			t.Logf("guest read succeeded on attempt%d after%s (max5 immediate retries)", attempt+1, time.Since(start))
			return
		}
		if result.SCT != nvme.SCTGeneric || result.SC != nvme.SCNamespaceNotReady {
			t.Fatalf("guest read terminal status: %v", result.AsError())
		}
	}
	t.Fatal("guest EIO: all six immediate attempts exhausted before chunk attach")
}

func TestWALStoreChunk_PendingReadWaitIsBounded(t *testing.T) {
	_, _, handler := pendingTakeoverRead(t, 0)
	start := time.Now()
	result := handler.Handle(context.Background(), nvme.IOCommand{Opcode: 0x02, NSID: 1, SLBA: 33, NLB: 1})
	elapsed := time.Since(start)
	if result.SCT != nvme.SCTGeneric || result.SC != nvme.SCNamespaceNotReady || len(result.Data) != 0 {
		t.Fatalf("pending read result: %+v", result)
	}
	if elapsed < 2500*time.Millisecond || elapsed >= 5*time.Second {
		t.Fatalf("pending read wait=%s; want bounded readiness wait below5s", elapsed)
	}
}

func TestWALStoreChunk_PendingReadHonoursEarlierDeadline(t *testing.T) {
	_, backend, _ := pendingTakeoverRead(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	n, err := backend.Read(ctx, 33*4096, make([]byte, 4096))
	if n != 0 || !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline read: n=%d err=%v", n, err)
	}
	if time.Since(start) >= time.Second {
		t.Fatal("read ignored earlier deadline")
	}
}

func TestWALStoreChunk_TakeoverWriteWaitsForAttach(t *testing.T) {
	_, backend, _ := pendingTakeoverRead(t, 2500*time.Millisecond)
	var n int
	var err error
	for attempt := 0; attempt <= 5; attempt++ {
		start := time.Now()
		n, err = backend.Write(context.Background(), 33*4096, block(0x74))
		if elapsed := time.Since(start); elapsed >= 5*time.Second {
			t.Fatalf("write command waited %s", elapsed)
		}
		if err == nil {
			break
		}
		if n != 0 || !errors.Is(err, frontend.ErrBackendBusy) {
			t.Fatalf("pending write lost retryability: n=%d err=%v", n, err)
		}
	}
	if err != nil || n != 4096 {
		t.Fatalf("pending write: n=%d err=%v", n, err)
	}
	got := make([]byte, 4096)
	if n, err := backend.Read(context.Background(), 33*4096, got); err != nil || n != 4096 || !bytes.Equal(got, block(0x74)) {
		t.Fatalf("attached write readback: n=%d err=%v", n, err)
	}
}

func TestWALStoreChunk_PendingWriteHonoursEarlierDeadline(t *testing.T) {
	_, backend, _ := pendingTakeoverRead(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	n, err := backend.Write(ctx, 33*4096, block(0x74))
	if n != 0 || !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending write deadline: n=%d err=%v", n, err)
	}
}

func TestWALStoreChunk_PendingAttachBeyondHostRetryBudget(t *testing.T) {
	_, _, handler := pendingTakeoverRead(t, 0)
	for attempt := 0; attempt <= 5; attempt++ {
		start := time.Now()
		result := handler.Handle(context.Background(), nvme.IOCommand{Opcode: 0x02, NSID: 1, SLBA: 33, NLB: 1})
		if result.SCT != nvme.SCTGeneric || result.SC != nvme.SCNamespaceNotReady || len(result.Data) != 0 {
			t.Fatalf("attempt%d exposed data or terminal status: %+v", attempt, result)
		}
		if elapsed := time.Since(start); elapsed < 2500*time.Millisecond || elapsed >= 5*time.Second {
			t.Fatalf("attempt%d wait=%s", attempt, elapsed)
		}
	}
	// A permanently unavailable backend still exhausts a finite host retry policy;
	// this control promises retryable completions and no bytes, not infinite availability.
}
