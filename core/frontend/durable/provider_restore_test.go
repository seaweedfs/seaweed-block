package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// A goroutine started with GoOwned sees its ctx end when Close starts, and
// Close returns only after it has returned; after Close nothing is admitted.
func TestDurableProvider_GoOwnedIsCancelledAndJoinedByClose(t *testing.T) {
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 64}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	started, finished := make(chan struct{}), make(chan struct{})
	if err := p.GoOwned(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		time.Sleep(20 * time.Millisecond) // still settling when Close is called
		close(finished)
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	closed := make(chan struct{})
	go func() { _ = p.Close(); close(closed) }()
	select {
	case <-closed:
		select {
		case <-finished:
		default:
			t.Fatal("Close returned before the owned goroutine finished")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	if err := p.GoOwned(func(context.Context) {}); !errors.Is(err, durable.ErrProviderClosed) {
		t.Fatalf("GoOwned after Close: %v, want ErrProviderClosed", err)
	}
}
