package durable_test

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
