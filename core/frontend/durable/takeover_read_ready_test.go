package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// External observers see the committed takeover HEAD while attachment is
// held in the provider-owned worker, without a separate test goroutine.
type heldTakeoverClaim struct {
	layout.StartStore
	claimed           chan struct{}
	release           chan struct{}
	once, releaseOnce sync.Once
	holdFor           time.Duration
	readRegistered    chan struct{}
	resolutionBlocked chan struct{}
	resolutionOnce    sync.Once
}

func (r *heldTakeoverClaim) unblock() { r.releaseOnce.Do(func() { close(r.release) }) }
func (r *heldTakeoverClaim) ReadHead(ctx context.Context) (layout.Head, bool, error) {
	// The permanent-outage case must hold outcome resolution as well as the
	// CAS reply: a committed CAS with readable HEAD can now settle after the
	// per-operation deadline. Unbounded diagnostic reads still see the root.
	if _, bounded := ctx.Deadline(); bounded && r.holdFor <= 0 {
		select {
		case <-r.claimed:
			r.resolutionOnce.Do(func() { close(r.resolutionBlocked) })
			select {
			case <-r.release:
			case <-ctx.Done():
				return layout.Head{}, false, ctx.Err()
			}
		default:
		}
	}
	return r.StartStore.ReadHead(ctx)
}
func (r *heldTakeoverClaim) UpdateHead(ctx context.Context, expected uint64, next layout.Head) error {
	err := r.StartStore.UpdateHead(ctx, expected, next)
	if err != nil || next.Epoch != 4 {
		return err
	}
	r.once.Do(func() { close(r.claimed) })
	var expired <-chan time.Time
	if r.holdFor > 0 {
		select {
		case <-r.readRegistered:
		case <-r.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		timer := time.NewTimer(r.holdFor)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case <-r.release:
		return nil
	case <-expired:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func pendingTakeoverRead(t *testing.T, hold time.Duration) (*durable.DurableProvider, frontend.Backend, *nvme.IOHandler) {
	p, b, h, _ := pendingTakeoverReadWithRelease(t, hold)
	return p, b, h
}

func pendingTakeoverReadWithRelease(t *testing.T, hold time.Duration) (*durable.DurableProvider, frontend.Backend, *nvme.IOHandler, func()) {
	t.Helper()
	fixture := seededLayoutFixture(t)
	store, err := fixture.Catalog.Store(fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	remote := &heldTakeoverClaim{StartStore: store, claimed: make(chan struct{}), release: make(chan struct{}), holdFor: hold, readRegistered: make(chan struct{}), resolutionBlocked: make(chan struct{})}
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks}
	fixture.Configure(&cfg)
	cfg.ChunkLayoutHeadFor = func(string) (layout.StartStore, error) { return remote, nil }
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
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
	// B Start remains inside the held UpdateHead reply. A later diagnostic
	// ReadHead is optional; the committed HEAD below proves the wait is real.
	head := layoutHead(t, fixture)
	if head.Epoch != 4 || head.Holder != "r1" {
		t.Fatalf("takeover HEAD: %+v", head)
	}
	backend, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	var firstWait sync.Once
	durable.SetTestBeforeAttachWait(backend, func() { firstWait.Do(func() { close(remote.readRegistered) }) })
	handler := nvme.NewIOHandler(nvme.HandlerConfig{Backend: backend, BlockSize: 4096, VolumeSize: pcBlocks * 4096, NSID: 1})
	return p, backend, handler, remote.unblock
}

// Model Linux max_retries=5 and CRD=0: at most six attempts, no sleeps.
// A status-only fix exhausts them during attachment and becomes guest EIO.
func TestWALStoreChunk_TakeoverReadBeforeAttachIsRetryable(t *testing.T) {
	_, _, handler := pendingTakeoverRead(t, 1500*time.Millisecond)
	command := nvme.IOCommand{Opcode: 0x02, NSID: 1, SLBA: 33, NLB: 1}
	start := time.Now()
	for attempt := 0; attempt <= 5; attempt++ {
		result := handler.Handle(context.Background(), command)
		if result.AsError() == nil {
			if !bytes.Equal(result.Data, block(0x33)) {
				t.Fatal("guest read returned incorrect HEAD bytes")
			}
			if elapsed := time.Since(start); elapsed >= 5*time.Second {
				t.Fatalf("guest read took %s", elapsed)
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

// Wall-clock integration smoke; exact outcomes are required by the virtual-time
// TestAttachWriteBeforeReadinessBudgetSucceeds and AfterReadinessBudget controls.
func TestWALStoreChunk_TakeoverWriteWaitsForAttach(t *testing.T) {
	const hold = 100 * time.Millisecond
	_, backend, _ := pendingTakeoverRead(t, hold)
	start := time.Now()
	n, err := backend.Write(context.Background(), 33*4096, block(0x74))
	elapsed := time.Since(start)
	want := byte(0x74)
	if err != nil {
		// INV-FE-NVME-READY-STATUS-001: the product's 3s readiness
		// allowance refuses without I/O; NVMe reports retryable 0x82.
		if n != 0 || !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) || elapsed < 3*time.Second {
			t.Fatalf("pending write: n=%d wait=%s err=%v", n, elapsed, err)
		}
		want = 0x33 // A refused write must preserve the original HEAD bytes.
	} else if n != 4096 || elapsed < hold {
		t.Fatalf("write did not wait for attach: n=%d wait=%s hold=%s", n, elapsed, hold)
	}
	t.Logf("attach write: hold=%s wait=%s n=%d err=%v", hold, elapsed, n, err)
	// A loaded worker may still be attaching after the first refusal.
	// Retry only readiness errors; every other error remains a failure.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got := make([]byte, 4096)
	for {
		n, err = backend.Read(ctx, 33*4096, got)
		if !errors.Is(err, frontend.ErrBackendBusy) || ctx.Err() != nil {
			break
		}
	}
	if err != nil || n != 4096 || !bytes.Equal(got, block(want)) {
		t.Fatalf("attached write readback: n=%d err=%v want=%x", n, err, want)
	}
	if want == 0x33 {
		if n, err := backend.Write(ctx, 33*4096, block(0x74)); err != nil || n != 4096 {
			t.Fatalf("retry attached write: n=%d err=%v", n, err)
		}
		if n, err := backend.Read(ctx, 33*4096, got); err != nil || n != 4096 || !bytes.Equal(got, block(0x74)) {
			t.Fatalf("retry write readback: n=%d err=%v", n, err)
		}
	}
	if n, err := backend.Read(ctx, 100*4096, got); err != nil || n != 4096 || !bytes.Equal(got, block(0x64)) {
		t.Fatalf("takeover lost untouched data: n=%d err=%v", n, err)
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
