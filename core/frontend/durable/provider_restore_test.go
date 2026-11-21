package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// A goroutine started with GoOwned sees its ctx end when Close starts, and
// Close does not return while it is still settling; after Close nothing is
// admitted.
func TestDurableProvider_GoOwnedIsCancelledAndJoinedByClose(t *testing.T) {
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 64}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	closed := make(chan struct{})
	released := false
	t.Cleanup(func() { // registered before launch: release, then a bounded join of Close
		if !released {
			close(release)
		}
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
	}); err != nil {
		t.Fatal(err)
	}
	wait := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	wait(started, "the owned goroutine to start")
	go func() { _ = p.Close(); close(closed) }()
	wait(cancelled, "Close to cancel the owned goroutine's ctx")
	select {
	case <-closed:
		t.Fatal("Close returned while the owned goroutine was still settling")
	default:
	}
	released = true
	close(release)
	wait(closed, "Close to return after the owned goroutine returned")
	if err := p.GoOwned(func(context.Context) {}); !errors.Is(err, durable.ErrProviderClosed) {
		t.Fatalf("GoOwned after Close: %v, want ErrProviderClosed", err)
	}
}
