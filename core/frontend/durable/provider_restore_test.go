package durable_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// A goroutine started with GoOwned sees its ctx end when Close starts, and
// Close returns only after it has returned (ordering witnessed by sequence
// numbers); after Close nothing is admitted.
func TestDurableProvider_GoOwnedIsCancelledAndJoinedByClose(t *testing.T) {
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 64}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	var seq, settledAt, closedAt atomic.Int64
	started, cancelled, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	startClose := sync.OnceFunc(func() { // one Close, launched by the body or by cleanup
		go func() {
			_ = p.Close()
			closedAt.Store(seq.Add(1))
			close(closed)
		}()
	})
	t.Cleanup(func() { // registered before launch: always cancels, releases and joins
		startClose()
		releaseOnce()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Error("cleanup: Close did not return")
		}
	})
	if err := p.GoOwned(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release // held settlement: Close must wait for this
		settledAt.Store(seq.Add(1))
	}); err != nil {
		t.Fatal(err)
	}
	wait := func(ch <-chan struct{}, d time.Duration, what string) bool {
		t.Helper()
		select {
		case <-ch:
			return true
		case <-time.After(d):
			return false
		}
	}
	if !wait(started, 5*time.Second, "start") {
		t.Fatal("the owned goroutine never started")
	}
	startClose()
	if !wait(cancelled, 5*time.Second, "cancel") {
		t.Fatal("Close did not cancel the owned goroutine's ctx")
	}
	// Adverse schedule for a Close that does not join: give it time to
	// return while the goroutine is still held.
	if wait(closed, 200*time.Millisecond, "early close") {
		t.Fatal("Close returned while the owned goroutine was still held")
	}
	releaseOnce()
	if !wait(closed, 5*time.Second, "close") {
		t.Fatal("Close did not return after the owned goroutine returned")
	}
	if s, c := settledAt.Load(), closedAt.Load(); s == 0 || c <= s {
		t.Fatalf("Close returned (seq %d) before the owned goroutine settled (seq %d)", c, s)
	}
	if err := p.GoOwned(func(context.Context) {}); !errors.Is(err, durable.ErrProviderClosed) {
		t.Fatalf("GoOwned after Close: %v, want ErrProviderClosed", err)
	}
}
