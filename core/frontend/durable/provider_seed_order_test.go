package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type takeoverOrderRemote struct {
	storage.ChunkRemote
	beforeCAS func()
}

func (r *takeoverOrderRemote) CASHead(ctx context.Context, v storage.ChunkHeadVersion, h storage.ChunkHead) (uint64, error) {
	r.beforeCAS()
	return r.ChunkRemote.CASHead(ctx, v, h)
}

func TestRestoreSeedPrecedesTakeoverCAS(t *testing.T) {
	ctx := hangGuard(t)
	remote := storage.NewMemChunkRemote()
	if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "old", RemoteLSN: 7}); err != nil {
		t.Fatal(err)
	}
	var p *durable.DurableProvider
	var checked atomic.Bool
	var calls atomic.Int32
	var nextAtCAS, durableAtCAS, headAtCAS uint64
	wrapped := &takeoverOrderRemote{ChunkRemote: remote, beforeCAS: func() {
		calls.Add(1)
		if !checked.Load() {
			t.Error("takeover preceded VOLUME check")
		}
		s := p.LogicalStorage("v1")
		nextAtCAS = s.NextLSN()
		durableAtCAS, _, headAtCAS = s.Boundaries()
		if _, err := p.Backend("v1").Read(ctx, 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
			t.Errorf("I/O admitted before takeover: %v", err)
		}
	}}
	var err error
	p, err = durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
		ChunkRemoteFor:   func(string) (storage.ChunkRemote, error) { return wrapped, nil },
		ChunkAttachCheck: func(context.Context, string, durable.ChunkGeometry) error { checked.Store(true); return nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || nextAtCAS != 8 || durableAtCAS != 7 || headAtCAS != 7 {
		t.Fatalf("takeover before durable seed: CAS calls=%d nextLSN=%d durable=%d head=%d; want 1/8/7/7", calls.Load(), nextAtCAS, durableAtCAS, headAtCAS)
	}
	be, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := be.Read(ctx, 0, make([]byte, 4096)); err != nil {
		t.Fatal("ready after seed and takeover", err)
	}
	h, err := remote.GetHead(ctx)
	if err != nil || h.Epoch != 3 || h.Holder != "r1" || h.RemoteLSN != 7 {
		t.Fatal(h, err)
	}
	t.Logf("VOLUME check -> seed nextLSN=%d/durable=%d -> takeover -> serve", nextAtCAS, durableAtCAS)
}

// A restored restart under the same assignment must reactivate a detached
// image before exposing I/O, not treat it as an already-settled active claim.
func TestRestoreEqualOwnerDetachedHEADIsReactivated(t *testing.T) {
	ctx := hangGuard(t)
	remote := storage.NewMemChunkRemote()
	makeProvider := func() *durable.DurableProvider {
		p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "node-a/r1",
			ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, v1View())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	source := makeProvider()
	openRecovered(t, source)
	be, err := source.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	want := block(0xA7)
	if _, err = be.Write(ctx, 4096, want); err != nil {
		t.Fatal(err)
	}
	if err = be.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	cut, _, err := source.DetachVolume(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := makeProvider()
	if _, err = restarted.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	h, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Gen <= cut.Head.Gen || h.Epoch != cut.Head.Epoch || h.Holder != cut.Head.Holder || h.State != storage.ChunkHeadActive || h.DetachedBy != nil || h.RemoteLSN != cut.H {
		t.Errorf("ready with stale detached proof: HEAD=%+v detached=%+v", h, cut.Head)
	}
	frontend, err := restarted.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err = frontend.Read(ctx, 4096, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("restart changed exact data")
	}
}
