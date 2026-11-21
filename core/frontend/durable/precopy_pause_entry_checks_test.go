package durable

import (
	"context"
	"errors"
	"testing"
	"time"
)

type refusingRebaser struct{}

func (refusingRebaser) Observe(context.Context, uint32, uint64, []byte) error { return nil }
func (refusingRebaser) Sync(context.Context, uint64) error                    { return nil }
func (refusingRebaser) RebaseShipLSN(uint64) error {
	return errors.New("injected: ship LSN cannot follow the store")
}

// TestPrecopyPauseRefusedOnUnusableBackend pins that a pre-copy pause never
// begins on a backend that is closed, not operational, has a failed observer
// install, or already has a pause installed.
func TestPrecopyPauseRefusedOnUnusableBackend(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spoil func(t *testing.T, b *StorageBackend)
	}{
		{"observer install failed", func(t *testing.T, b *StorageBackend) {
			if err := b.SetWriteObserver(refusingRebaser{}); err == nil {
				t.Fatal("refused observer install reported success")
			}
		}},
		{"closed", func(t *testing.T, b *StorageBackend) { _ = b.Close() }},
		{"not operational", func(t *testing.T, b *StorageBackend) { b.SetOperational(false, "host drain") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := precopyTestBackend(newHeldReadStore())
			tc.spoil(t, b)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resume, err := b.BeginPrecopyPause(ctx)
			if err == nil {
				resume()
				t.Fatal("pre-copy pause began on an unusable backend")
			}
			b.mu.Lock()
			left := b.precopy
			b.mu.Unlock()
			if left != nil {
				t.Fatal("refused pause left a pause installed")
			}
		})
	}

	t.Run("already paused", func(t *testing.T) {
		b := precopyTestBackend(newHeldReadStore())
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resume, err := b.BeginPrecopyPause(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer resume()
		b.mu.Lock()
		first := b.precopy
		b.mu.Unlock()
		second, err := b.BeginPrecopyPause(ctx)
		if err == nil {
			second()
			t.Fatal("second pre-copy pause began while one is installed")
		}
		b.mu.Lock()
		current := b.precopy
		b.mu.Unlock()
		if current != first {
			t.Fatal("refused second pause replaced the installed pause")
		}
		resume()
		if _, err := b.Read(context.Background(), 0, nil); err != nil {
			t.Fatalf("resume of the first pause did not reopen admission: %v", err)
		}
	})
}
