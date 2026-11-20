package durable_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

type stopBoundaryRemote struct {
	storage.ChunkRemote
	entered chan time.Time
	release chan struct{}
}

func (r *stopBoundaryRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if next.State == storage.ChunkHeadDetached {
		r.entered <- time.Now()
		select {
		case <-r.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return r.ChunkRemote.CASHead(ctx, expected, next)
}

func TestPrecopyStopIOReturnTimestampPrecedesFinalPublication(t *testing.T) {
	r := &stopBoundaryRemote{ChunkRemote: storage.NewMemChunkRemote(), entered: make(chan time.Time, 1), release: make(chan struct{})}
	p := precopyProvider(t, r, &parkView{stubView: v1View()})
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(0x67)); err != nil {
		t.Fatal(err)
	}
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	var once sync.Once
	release := func() { once.Do(func() { close(r.release) }) }
	t.Cleanup(func() {
		release()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("detach did not join")
		}
	})
	started := time.Now()
	go func() { _, _, err := p.DetachVolume(ctx, "v1"); done <- err }()
	var beforePublication time.Time
	select {
	case beforePublication = <-r.entered:
	case <-ctx.Done():
		t.Fatal("final publication never entered")
	}
	release()
	select {
	case err := <-done:
		done <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("detach did not finish")
	}
	count := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		_, stamp, ok := strings.Cut(line, "volume=v1 StopIO returned at ")
		if !ok {
			continue
		}
		count++
		at, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || at.Before(started) || at.After(beforePublication) {
			t.Fatalf("StopIO timestamp is not its return boundary: stamp=%q started=%s finalCAS=%s err=%v", stamp, started, beforePublication, err)
		}
	}
	if count != 1 {
		t.Fatalf("StopIO returned timestamp count=%d, want 1", count)
	}
}
