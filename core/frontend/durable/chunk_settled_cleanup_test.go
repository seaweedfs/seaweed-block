package durable

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
)

func TestChunkSettledIntentCleanupAfterProviderCancellation(t *testing.T) {
	inner := storage.NewMemChunkRemote()
	id, err := inner.PutIntent(context.Background(), storage.ChunkIntent{Keys: []string{"unpublished"}})
	if err != nil {
		t.Fatal(err)
	}
	done, cancel := context.WithCancel(context.Background())
	cancel()
	bound := &closeBoundRemote{ChunkRemote: inner, done: done}
	if err = bound.DeleteIntent(context.Background(), id); !errors.Is(err, context.Canceled) {
		t.Fatalf("ordinary operation escaped Close: %v", err)
	}
	if err = bound.DeleteSettledIntent(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	left, err := inner.ListIntents(context.Background())
	if err != nil || len(left) != 0 {
		t.Fatalf("settled roots=%d err=%v", len(left), err)
	}
}
