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
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type faultyHydrationRemote struct {
	storage.ChunkRemote
	fault   error
	corrupt bool
}

func (r *faultyHydrationRemote) Get(ctx context.Context, key string) ([]byte, error) {
	if r.fault != nil {
		return nil, r.fault
	}
	b, err := r.ChunkRemote.Get(ctx, key)
	if err == nil && r.corrupt && len(b) > 0 {
		b = append([]byte(nil), b...)
		b[0] ^= 0xff
	}
	return b, err
}

func TestDurableHydration_NVMePreservesPhysicalAndHashErrors(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "joined physical", true: "hash"}[corrupt], func(t *testing.T) {
			r := &faultyHydrationRemote{ChunkRemote: seededRemote(t), corrupt: corrupt}
			if !corrupt {
				r.fault = errors.Join(context.DeadlineExceeded, errors.New("physical remote read failure"))
			}
			p := chunkProvider(t, t.TempDir(), r, v1View(), 0)
			openRecovered(t, p)
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
	storage.ChunkRemote
	gets   atomic.Int64
	second chan struct{}
	once   sync.Once
}

func (r *pacedHydrationRemote) release() { r.once.Do(func() { close(r.second) }) }
func (r *pacedHydrationRemote) Get(ctx context.Context, key string) ([]byte, error) {
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
	return r.ChunkRemote.Get(ctx, key)
}

func TestDurableHydration_MultipleChunksShareOneDeadline(t *testing.T) {
	r := &pacedHydrationRemote{ChunkRemote: seededRemote(t), second: make(chan struct{})}
	p := chunkProvider(t, t.TempDir(), r, v1View(), 0)
	t.Cleanup(r.release)
	openRecovered(t, p)
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
	storage.ChunkRemote
	gate chan struct{}
	once sync.Once
	gets atomic.Int64
}

func (r *heldHydrationRemote) release() { r.once.Do(func() { close(r.gate) }) }
func (r *heldHydrationRemote) Get(ctx context.Context, key string) ([]byte, error) {
	r.gets.Add(1)
	select {
	case <-r.gate:
		return r.ChunkRemote.Get(ctx, key)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestDurableHydration_ReadAndRMWPreserveCallerBudget(t *testing.T) {
	for _, rmw := range []bool{false, true} {
		name := "read"
		if rmw {
			name = "rmw"
		}
		t.Run(name, func(t *testing.T) {
			r := &heldHydrationRemote{ChunkRemote: seededRemote(t), gate: make(chan struct{})}
			p := chunkProvider(t, t.TempDir(), r, v1View(), 0)
			t.Cleanup(r.release)
			s, err := p.EnsureStorage("v1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatal(err)
			}
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
