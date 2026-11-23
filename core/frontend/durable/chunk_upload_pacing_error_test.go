package durable

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A pacing refusal before a chunk put names the host limiter, keeps its cause
// and sends no put (issue 486: it was reported as the chunk remote).
func TestChunkUploadPacingErrorNamesTheLimiter(t *testing.T) {
	inner := storage.NewMemChunkRemote()
	bound := &closeBoundRemote{ChunkRemote: inner, done: context.Background(),
		waitUpload: func(context.Context, int) error { return context.DeadlineExceeded }}
	err := bound.Put(context.Background(), "chunk-1", []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) || !strings.HasPrefix(err.Error(), "hostlimit: upload pacing before put chunk-1: ") {
		t.Fatalf("pacing error: %v", err)
	}
	if _, err := inner.Get(context.Background(), "chunk-1"); err == nil {
		t.Fatal("a chunk was put although pacing refused it")
	}
}
