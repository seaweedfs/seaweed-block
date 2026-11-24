package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPlainDetachAfterPrecopyRefusalRequiresIdleFrontend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	hooks := &detachHooks{}
	p := precopyProvider(t, remote, &parkView{stubView: v1View()}, hooks)
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Write(ctx, 4096, block(0x4a)); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	joined := false
	t.Cleanup(func() {
		unblock()
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("admitted sync did not join")
			}
		}
	})
	if err = b.(*durable.StorageBackend).SetWriteObserver(writeObserverFunc{sync: func(ctx context.Context, _ uint64) error {
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
	go func() { done <- b.Sync(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("sync was not admitted")
	}
	r, _, err := p.DetachVolume(ctx, "v1")
	if !errors.Is(err, storage.ErrPrecopyNotConverged) || r.Precopy.Frozen {
		t.Fatalf("expected thawed pre-copy refusal: %+v %v", r, err)
	}
	before, err := remote.GetHead(ctx)
	if err != nil || before.State == storage.ChunkHeadDetached {
		t.Fatalf("refusal detached: %+v %v", before, err)
	}
	if _, _, err = p.DetachVolumePlain(ctx, "v1"); !errors.Is(err, durable.ErrPlainDetachBusy) {
		t.Fatalf("plain accepted admitted I/O: %v", err)
	}
	after, err := remote.GetHead(ctx)
	if err != nil || after.Gen != before.Gen || after.State != before.State {
		t.Fatalf("busy plain mutated HEAD: before=%+v after=%+v err=%v", before, after, err)
	}
	starts, resumes := hooks.calls()
	if len(starts) != 1 || len(resumes) != 1 {
		t.Fatalf("busy plain mutated detach hooks: %v %v", starts, resumes)
	}
	unblock()
	select {
	case err = <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("sync did not finish")
	}
	// A refused plain request left service and authority unchanged.
	if _, err = b.Write(ctx, 8192, block(0x5b)); err != nil {
		t.Fatal("busy plain stopped service:", err)
	}
	r, last, err := p.DetachVolumePlain(ctx, "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached || r.H != last || len(r.Precopy.Rounds) != 0 {
		t.Fatalf("plain did not use ordinary detached cut: %+v last=%d err=%v", r, last, err)
	}
	for _, x := range []struct {
		lba   uint32
		value byte
	}{{1, 0x4a}, {2, 0x5b}} {
		if !bytes.Equal(headBlock(t, remote, x.lba), block(x.value)) {
			t.Fatalf("plain lost acknowledged block %d", x.lba)
		}
	}
	if _, err = b.Write(ctx, 12288, block(0x6c)); err == nil {
		t.Fatal("old frontend serves after plain detach")
	}
	again, last2, err := p.DetachVolumePlain(ctx, "v1")
	if err != nil || again.Head.Gen != r.Head.Gen || last2 != last {
		t.Fatalf("plain retry changed result: %+v last=%d err=%v", again, last2, err)
	}
}

func TestPlainDetachSkipsPrecopyCostAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	p := precopyProvider(t, remote, &parkView{stubView: v1View()})
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Write(ctx, 4096, block(0x7c)); err != nil {
		t.Fatal(err)
	}
	r, _, err := p.DetachVolumePlain(ctx, "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached || len(r.Precopy.Rounds) != 0 || !bytes.Equal(headBlock(t, remote, 1), block(0x7c)) {
		t.Fatalf("plain request used pre-copy admission or lost its point: %+v %v", r, err)
	}
}
