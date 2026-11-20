package durable_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPrecopyFreezeDoesNotWaitForAdmittedIO(t *testing.T) {
	p, view := parkedProvider(t, storage.NewMemChunkRemote())
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	gate, ok := b.(interface{ TryStopIO(string) bool })
	if !ok {
		t.Fatal("backend has no nonblocking tentative freeze")
	}
	view.arm()
	done := make(chan error, 1)
	t.Cleanup(func() {
		view.unpark()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("writer did not join")
		}
	})
	go func() { _, err := b.Write(context.Background(), 4096, block(7)); done <- err }()
	select {
	case <-view.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write never admitted")
	}
	frozen := make(chan bool, 1)
	go func() { frozen <- gate.TryStopIO("pre-copy final cut") }()
	select {
	case stopped := <-frozen:
		if stopped {
			t.Fatal("stopped before admitted write drained")
		}
	case <-time.After(time.Second):
		t.Fatal("freeze blocked on admitted I/O")
	}
	if _, err := b.Write(context.Background(), 8192, block(8)); err != nil {
		t.Fatal("rejected tentative freeze stopped service:", err)
	}
	view.unpark()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("writer stuck")
	}
	if !gate.TryStopIO("pre-copy final cut") {
		t.Fatal("idle frontend did not freeze")
	}
	if _, err := b.Write(context.Background(), 8192, block(9)); err == nil {
		t.Fatal("write served after successful freeze")
	}
}

func precopyProvider(t *testing.T, remote storage.ChunkRemote, view *parkView, hooks ...*detachHooks) *durable.DurableProvider {
	t.Helper()
	cfg := durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkDetachPrecopy: true, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}
	if len(hooks) > 0 {
		cfg.OnChunkDetach = hooks[0].detach
		cfg.OnChunkDetachResumed = hooks[0].resume
	}
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPrecopyProviderPublishesExactDetachedImage(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	p := precopyProvider(t, remote, &parkView{stubView: v1View()})
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Write(context.Background(), 4096, block(0x8a)); err != nil {
		t.Fatal(err)
	}
	r, last, err := p.DetachVolume(context.Background(), "v1")
	if err != nil || !r.Precopy.Frozen || r.Head.State != storage.ChunkHeadDetached || r.H != last {
		t.Fatalf("detach=%+v last=%d err=%v", r, last, err)
	}
	if !bytes.Equal(headBlock(t, remote, 1), block(0x8a)) {
		t.Fatal("detached image lost write")
	}
	if _, err = b.Write(context.Background(), 4096, block(0x9a)); err == nil {
		t.Fatal("old owner serves after final detach")
	}
	again, _, err := p.DetachVolume(context.Background(), "v1")
	if err != nil || again.Head.Gen != r.Head.Gen {
		t.Fatalf("repeated detach: %+v %v", again, err)
	}
}

func TestPrecopyProviderBusyOwnerKeepsServing(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	view := &parkView{stubView: v1View()}
	k := &detachHooks{}
	p := precopyProvider(t, remote, view, k)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(0x8a)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	if err := b.(*durable.StorageBackend).SetWriteObserver(writeObserverFunc{sync: func(ctx context.Context, _ uint64) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	t.Cleanup(func() {
		unpark()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("writer did not join")
		}
	})
	go func() { done <- b.Sync(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write not admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, _, err := p.DetachVolume(ctx, "v1")
	if !errors.Is(err, storage.ErrPrecopyNotConverged) || r.Precopy.Frozen {
		t.Fatalf("report=%+v err=%v", r.Precopy, err)
	}
	if started, resumed := k.calls(); len(started) != 1 || len(resumed) != 1 {
		t.Fatalf("nonconverged hooks: started=%v resumed=%v", started, resumed)
	}
	if h, _ := remote.GetHead(ctx); h.State == storage.ChunkHeadDetached {
		t.Fatal("busy owner detached")
	}
	if _, err = b.Write(ctx, 8192, block(0x9b)); err != nil {
		t.Fatal("nonconverged owner stopped:", err)
	}
	unpark()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-ctx.Done():
		t.Fatal("held write did not finish")
	}
	r, _, err = p.DetachVolume(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if started, resumed := k.calls(); len(started) != 2 || len(resumed) != 1 {
		t.Fatalf("successful retry hooks: started=%v resumed=%v", started, resumed)
	}
	if !bytes.Equal(headBlock(t, remote, 1), block(0x8a)) || !bytes.Equal(headBlock(t, remote, 2), block(0x9b)) {
		t.Fatal("retry lost admitted bytes")
	}
}
