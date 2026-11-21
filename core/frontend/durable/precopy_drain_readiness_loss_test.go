package durable

import (
	"context"
	"testing"
	"time"
)

// TestPrecopyDrainEndsWithErrorOnReadinessLossOrClose pins that a pre-copy
// drain still pending when the backend loses readiness or is closed ends with
// an error and never reports a clean drain.
func TestPrecopyDrainEndsWithErrorOnReadinessLossOrClose(t *testing.T) {
	for _, tc := range []struct {
		name string
		// bound is the drain's caller context, as the pre-copy caller gives it.
		bound time.Duration
		spoil func(b *StorageBackend)
	}{
		{"readiness lost", 3 * time.Second, func(b *StorageBackend) { b.SetOperational(false, "host drain") }},
		// Close does not signal the drain; the waiter ends at its own bound.
		{"closed", 200 * time.Millisecond, func(b *StorageBackend) { _ = b.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newHeldReadStore(0)
			b := precopyTestBackend(store)
			reads := startHeldReads(t, b, store, 0)
			ctx, cancel := context.WithTimeout(context.Background(), tc.bound)
			defer cancel()
			pause := beginPauseAsync(ctx, b)
			installedPause(t, b)

			tc.spoil(b)
			close(store.release[0])
			<-reads
			select {
			case r := <-pause:
				if r.err == nil {
					r.resume()
					t.Fatal("drain reported clean after the backend became unusable")
				}
			case <-time.After(tc.bound + 3*time.Second):
				t.Fatal("drain waiter did not end")
			}
			b.mu.Lock()
			left := b.precopy
			b.mu.Unlock()
			if left != nil {
				t.Fatal("failed drain left its pause installed")
			}
		})
	}
}

// TestPrecopyFinalStopRefusedAfterReadinessLossOrClose pins that the final
// pre-copy stop refuses to commit a freeze when the backend lost readiness or
// was closed after the drain completed.
func TestPrecopyFinalStopRefusedAfterReadinessLossOrClose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spoil func(b *StorageBackend)
	}{
		{"readiness lost", func(b *StorageBackend) { b.SetOperational(false, "host drain") }},
		{"closed", func(b *StorageBackend) { _ = b.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := precopyTestBackend(newHeldReadStore())
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resume, err := b.BeginPrecopyPause(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer resume()
			tc.spoil(b)
			if b.TryStopIO("chunk pre-copy: final detached cut") {
				t.Fatal("final stop committed a freeze on an unusable backend")
			}
		})
	}
}
