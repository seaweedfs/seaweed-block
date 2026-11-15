package durable_test

// Bounds of the chunk control surface (second review of #115): timed-out
// detach retries share one drain instead of each leaving a waiter behind,
// and a cut whose volume is closed right after it was resolved fails
// cleanly instead of reading a handle Close removed.

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestWALStoreChunk_TimedOutDetachRetriesShareOneDrain(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	p, view := parkedProvider(t, remote)
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	view.arm()
	t.Cleanup(view.unpark)
	werr := make(chan error, 1)
	go func() { _, err := b.Write(ctx, 4096, block(4)); werr <- err }()
	<-view.entered // an admitted write holds the drain

	timedOut := func() {
		t.Helper()
		dctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		if _, _, err := p.DetachVolume(dctx, "v1"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("detach behind a held write: %v; want the context's deadline", err)
		}
	}
	timedOut()
	base := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		timedOut()
	}
	if n := runtime.NumGoroutine() - base; n > 3 {
		t.Fatalf("20 timed-out detach retries left %d more goroutines; want them to share one drain", n)
	}
	view.unpark()
	acked := <-werr == nil
	r, _, err := p.DetachVolume(ctx, "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach once the write finished: %+v %v", r, err)
	}
	if acked && !bytesEqual(headBlock(t, remote, 1), block(4)) {
		t.Fatal("the held write was acknowledged but is not in the detached HEAD")
	}
}

func TestWALStoreChunk_CutVolumeSurvivesCloseAfterResolve(t *testing.T) {
	p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), v1View(), 0)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	durable.SetTestAfterChunkResolve(p, func() { _ = p.Close() })
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("CutVolume panicked after Close removed the volume it had resolved: %v", r)
			}
		}()
		_, err = p.CutVolume(context.Background(), "v1", 1)
	}()
	if err == nil {
		t.Fatal("a cut of a volume closed during the call succeeded")
	}
}
