package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func hydrationReadContext(s *WALStore, ctx context.Context, lba uint32) ([]byte, error) {
	if r, ok := any(s).(interface {
		ReadContext(context.Context, uint32) ([]byte, error)
	}); ok {
		return r.ReadContext(ctx, lba)
	}
	return s.Read(lba)
}

func TestChunkHydration_DeadlineDetachesWithoutCancellingFill(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	copy(src[8*ctBlockSize:], makeBlock(ctBlockSize, 0x88))
	remote := newFakeChunkRemote()
	remote.seedHead(t, src, 50, 1)
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	mustAttach(t, s, remote, cfg)
	_, release := holdNextGet(t, remote)
	fired := make(chan struct{})
	timer := time.AfterFunc(500*time.Millisecond, func() { defer close(fired); release() })
	t.Cleanup(func() {
		release()
		if !timer.Stop() {
			<-fired
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	got, err := hydrationReadContext(s, ctx, 8)
	elapsed := time.Since(start)
	if len(got) != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline must detach before fill: bytes=%d err=%v elapsed=%s", len(got), err, elapsed)
	}
	if elapsed >= 350*time.Millisecond {
		t.Fatalf("hydration ignored caller budget: %s", elapsed)
	}
	retry, cancelRetry := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRetry()
	got, err = hydrationReadContext(s, retry, 8)
	if err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, 0x88)) {
		t.Fatalf("retry did not get exact bytes: %v", err)
	}
	if n := remote.stats().gets; n != 1 {
		t.Fatalf("retry cancelled/restarted shared fetch: Gets=%d want1", n)
	}
}
