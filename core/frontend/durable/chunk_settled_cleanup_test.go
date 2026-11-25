package durable

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// After the provider is cancelled (Close), an ordinary call through the
// bound remote ends with the provider, while the cut's Abort still reaches
// the remote: a session left open is ended, never stranded.
func TestSession_ProviderCancellationAbortsTheOpenSession(t *testing.T) {
	inner := storage.NewMemChunkRemote()
	if _, err := inner.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}
	done, cancel := context.WithCancel(context.Background())
	cancel()
	bound := &closeBoundRemote{ChunkRemote: inner, done: done}
	if _, err := bound.Begin(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("ordinary operation escaped Close: %v", err)
	}
	bound.Abort()
	if s := inner.OpenSession(); s != "" {
		t.Fatalf("session %q left open after the provider's Abort", s)
	}
}
