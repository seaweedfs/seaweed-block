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
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

func seededHydrationLayout(t *testing.T) *testutil.LayoutFixture {
	t.Helper()
	f := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks, ChunkHolder: "old"}
	f.Configure(&cfg)
	source, err := durable.NewDurableProvider(cfg, viewAs("old", 1))
	if err != nil {
		t.Fatal(err)
	}
	s := openRecovered(t, source)
	writeSync(t, s, 33, 0x33)
	writeSync(t, s, 100, 0x64)
	if _, err := source.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	return f
}

func hydrationProvider(t *testing.T, f *testutil.LayoutFixture, view *stubView) *durable.DurableProvider {
	t.Helper()
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks}
	f.Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestDurableHydration_ConfiguredLimitsReachStatus(t *testing.T) {
	for _, configured := range []bool{false, true} {
		cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks}
		testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks).Configure(&cfg)
		wantWorkers, wantDepth := 2, 64
		if configured {
			cfg.ChunkHydrationWorkers, cfg.ChunkHydrationQueueDepth = 3, 17
			wantWorkers, wantDepth = 3, 17
		}
		p, err := durable.NewDurableProvider(cfg, v1View())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		openRecovered(t, p)
		st := p.DurableStatuses()
		if len(st) != 1 || st[0].Chunk == nil {
			t.Fatalf("missing chunk status: %+v", st)
		}
		h := st[0].Chunk.Stats.Hydration
		if h.WorkerLimit != wantWorkers || h.QueueDepth != wantDepth {
			t.Fatalf("configured=%t hydration=%+v", configured, h)
		}
	}
}

type faultyHydrationRemote struct {
	fault   error
	corrupt bool
}

func (r *faultyHydrationRemote) Get(_ context.Context, _ string, b []byte) ([]byte, error) {
	if r.fault != nil {
		return nil, r.fault
	}
	if r.corrupt && len(b) > 0 {
		b = append([]byte(nil), b...)
		b[0] ^= 0xff
	}
	return b, nil
}

func TestDurableHydration_NVMePreservesPhysicalAndHashErrors(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "joined physical", true: "hash"}[corrupt], func(t *testing.T) {
			f := seededHydrationLayout(t)
			r := &faultyHydrationRemote{corrupt: corrupt}
			if !corrupt {
				r.fault = errors.Join(context.DeadlineExceeded, errors.New("physical remote read failure"))
			}
			p := hydrationProvider(t, f, v1View())
			openRecovered(t, p)
			f.Blobs.GetHook = r.Get
			b, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatal(err)
			}
			h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: b, BlockSize: 4096, VolumeSize: pcBlocks * 4096})
			res := h.Handle(context.Background(), nvme.IOCommand{Opcode: 2, NSID: 1, SLBA: 33, NLB: 1})
			if res.SC != nvme.SCInternalError || len(res.Data) != 0 {
				t.Fatalf("terminal hydration failure: %+v", res)
			}
		})
	}
}

type pacedHydrationRemote struct {
	gets   atomic.Int64
	second chan struct{}
	once   sync.Once
}

func (r *pacedHydrationRemote) release() { r.once.Do(func() { close(r.second) }) }
func (r *pacedHydrationRemote) Get(ctx context.Context, _ string, data []byte) ([]byte, error) {
	if r.gets.Add(1) == 1 {
		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else {
		select {
		case <-r.second:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return data, nil
}

func TestDurableHydration_MultipleChunksShareOneDeadline(t *testing.T) {
	f := seededHydrationLayout(t)
	r := &pacedHydrationRemote{second: make(chan struct{})}
	p := hydrationProvider(t, f, v1View())
	t.Cleanup(r.release)
	openRecovered(t, p)
	f.Blobs.GetHook = r.Get
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
	defer cancel()
	start := time.Now()
	buf := make([]byte, (101-33)*4096)
	n, err := b.Read(ctx, 33*4096, buf)
	if !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) || n != (96-33)*4096 {
		t.Fatalf("shared deadline n=%d err=%v gets=%d", n, err, r.gets.Load())
	}
	if elapsed := time.Since(start); elapsed >= 450*time.Millisecond {
		t.Fatalf("read budget renewed: %s", elapsed)
	}
	if !bytes.Equal(buf[:4096], block(0x33)) {
		t.Fatal("first chunk prefix changed")
	}
	r.release()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	last := make([]byte, 4096)
	if n, err := b.Read(ctx2, 100*4096, last); n != 4096 || err != nil || !bytes.Equal(last, block(0x64)) {
		t.Fatalf("shared work after timeout: n=%d err=%v", n, err)
	}
	if r.gets.Load() != 2 {
		t.Fatalf("retry restarted shared fetch: %d", r.gets.Load())
	}
}

type heldHydrationRemote struct {
	gate    chan struct{}
	once    sync.Once
	gets    atomic.Int64
	entered chan struct{}
}

func (r *heldHydrationRemote) release() { r.once.Do(func() { close(r.gate) }) }
func (r *heldHydrationRemote) Get(ctx context.Context, _ string, data []byte) ([]byte, error) {
	r.gets.Add(1)
	if r.entered != nil {
		select {
		case r.entered <- struct{}{}:
		default:
		}
	}
	select {
	case <-r.gate:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestDurableHydration_HostRetriesJoinOwnedFetch(t *testing.T) {
	f := seededHydrationLayout(t)
	r := &heldHydrationRemote{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	p := hydrationProvider(t, f, v1View())
	t.Cleanup(r.release)
	openRecovered(t, p)
	f.Blobs.GetHook = r.Get
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: b, BlockSize: 4096, VolumeSize: pcBlocks * 4096})
	// Start the hold only after the first actual Get. No setup time counts
	// toward the delay that the host's retry sequence must bridge.
	joined := make(chan struct{})
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop); r.release(); <-joined })
	go func() {
		defer close(joined)
		select {
		case <-r.entered:
		case <-stop:
			return
		}
		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			r.release()
		case <-stop:
		}
	}()
	busy := 0
	for attempt := 0; attempt < 6; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		res := h.Handle(ctx, nvme.IOCommand{Opcode: 2, NSID: 1, SLBA: 33, NLB: 1})
		cancel()
		if res.SC == nvme.SCSuccess {
			if busy == 0 || !bytes.Equal(res.Data, block(0x33)) || r.gets.Load() != 1 {
				t.Fatalf("busy=%d Gets=%d bytes=%d", busy, r.gets.Load(), len(res.Data))
			}
			return
		}
		if res.SC != nvme.SCNamespaceNotReady || len(res.Data) != 0 {
			t.Fatalf("attempt %d: %+v", attempt, res)
		}
		busy++
	}
	t.Fatal("six immediate host attempts exhausted despite surviving shared fill")
}

func TestDurableHydration_FenceWinsCallerExpiry(t *testing.T) {
	f := seededHydrationLayout(t)
	r := &heldHydrationRemote{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	view := v1View()
	p := hydrationProvider(t, f, view)
	t.Cleanup(r.release)
	openRecovered(t, p)
	f.Blobs.GetHook = r.Get
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joined := make(chan struct{})
	result := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		r.release()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("fenced hydration waiter not joined")
		}
	})
	go func() {
		defer close(joined)
		_, err := b.Read(ctx, 33*4096, make([]byte, 4096))
		result <- err
	}()
	select {
	case <-r.entered:
	case <-time.After(time.Second):
		t.Fatal("cold read never reached Get")
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 1}))
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, frontend.ErrStalePrimary) || errors.Is(err, frontend.ErrBackendBusy) {
			t.Fatalf("expiry hid fencing: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fenced waiter did not detach")
	}
}

func TestDurableHydration_ReadAndRMWPreserveCallerBudget(t *testing.T) {
	for _, rmw := range []bool{false, true} {
		name := "read"
		if rmw {
			name = "rmw"
		}
		t.Run(name, func(t *testing.T) {
			f := seededHydrationLayout(t)
			r := &heldHydrationRemote{gate: make(chan struct{})}
			p := hydrationProvider(t, f, v1View())
			t.Cleanup(r.release)
			s, err := p.EnsureStorage("v1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatal(err)
			}
			f.Blobs.GetHook = r.Get
			b, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatal(err)
			}
			before := s.NextLSN()
			fired := make(chan struct{})
			timer := time.AfterFunc(500*time.Millisecond, func() { defer close(fired); r.release() })
			t.Cleanup(func() {
				r.release()
				if !timer.Stop() {
					<-fired
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			var n int
			if rmw {
				n, err = b.Write(ctx, 33*4096+1, []byte{0x74})
			} else {
				n, err = b.Read(ctx, 33*4096, make([]byte, 4096))
			}
			if n != 0 || !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("request deadline: n=%d err=%v elapsed=%s", n, err, time.Since(start))
			}
			if time.Since(start) >= 350*time.Millisecond {
				t.Fatal("read/RMW did not share caller budget")
			}
			if s.NextLSN() != before {
				t.Fatal("refused RMW appended data")
			}
		})
	}
}

func TestDurableHydration_RMWFenceWinsCallerExpiry(t *testing.T) {
	f := seededHydrationLayout(t)
	r := &heldHydrationRemote{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	view := v1View()
	p := hydrationProvider(t, f, view)
	t.Cleanup(r.release)
	openRecovered(t, p)
	f.Blobs.GetHook = r.Get
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	store, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	before := store.NextLSN()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	joined := make(chan struct{})
	result := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		r.release()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("fenced hydration waiter not joined")
		}
	})
	go func() {
		defer close(joined)
		n, err := b.Write(ctx, 33*4096+1, []byte{0x74})
		if n != 0 {
			t.Errorf("unexpected RMW prefix %d: %v", n, err)
		}
		result <- err
	}()
	select {
	case <-r.entered:
	case <-time.After(time.Second):
		t.Fatal("cold read never reached Get")
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 1}))
	select {
	case err := <-result:
		if store.NextLSN() != before {
			t.Fatal("fenced RMW appended data")
		}
		if !errors.Is(err, frontend.ErrStalePrimary) || errors.Is(err, frontend.ErrBackendBusy) {
			t.Fatalf("expiry hid fencing: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fenced waiter did not detach")
	}
}

func TestDurableHydration_RMWFenceKeepsCommittedPrefix(t *testing.T) {
	f := seededHydrationLayout(t)
	r := &heldHydrationRemote{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	view := v1View()
	p := hydrationProvider(t, f, view)
	t.Cleanup(r.release)
	openRecovered(t, p)
	f.Blobs.GetHook = r.Get
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	store, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	before := store.NextLSN()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	joined := make(chan struct{})
	result := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		r.release()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("fenced hydration waiter not joined")
		}
	})
	go func() {
		defer close(joined)
		n, err := b.Write(ctx, 32*4096, bytes.Repeat([]byte{0x74}, 4097))
		if n != 4096 {
			t.Errorf("unexpected RMW prefix %d: %v", n, err)
		}
		result <- err
	}()
	select {
	case <-r.entered:
	case <-time.After(time.Second):
		t.Fatal("cold read never reached Get")
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 4, EndpointVersion: 1}))
	select {
	case err := <-result:
		if store.NextLSN() != before+1 {
			t.Fatal("fenced RMW lost its exact committed prefix")
		}
		if !errors.Is(err, frontend.ErrStalePrimary) || errors.Is(err, frontend.ErrBackendBusy) {
			t.Fatalf("expiry hid fencing: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fenced waiter did not detach")
	}
}
