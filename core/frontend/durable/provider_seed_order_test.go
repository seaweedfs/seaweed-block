package durable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	p, err = durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, ChunkHolder: "r1", StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
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

// A storage restart between the successful plain seal and teardown must not
// erase the proof the planned new host needs. Neither local-root nor fresh
// same-assignment reopen may reactivate it or expose I/O.
func TestRestoreEqualOwnerDetachedHEADStaysSealed(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			ctx := hangGuard(t)
			remote := storage.NewMemChunkRemote()
			root := t.TempDir()
			makeProvider := func(root string, view *stubView, holder string) *durable.DurableProvider {
				p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: holder,
					ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, view)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = p.Close() })
				return p
			}
			source := makeProvider(root, v1View(), "node-a/r1")
			s := openRecovered(t, source)
			want := block(0xA7)
			writeSync(t, s, 1, 0xA7)
			cut, _, err := source.DetachVolumePlain(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if !local {
				root = t.TempDir()
			}
			restarted := makeProvider(root, v1View(), "node-a/r1")
			if _, err := restarted.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerDetached) {
				t.Errorf("sealed incarnation reopened for serving: %v", err)
			}
			h := headHolder(t, remote)
			if h.Gen != cut.Head.Gen || h.Epoch != cut.Head.Epoch || h.Holder != cut.Head.Holder || h.State != storage.ChunkHeadDetached || h.DetachedBy == nil || *h.DetachedBy != *cut.Head.DetachedBy || h.RemoteLSN != cut.H {
				t.Errorf("restart changed sealed proof: HEAD=%+v seal=%+v", h, cut.Head)
			}
			be, err := restarted.Open(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := be.Read(ctx, 4096, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
				t.Errorf("sealed restart exposed I/O: %v", err)
			}
			if !bytes.Equal(headBlock(t, remote, 1), want) {
				t.Fatal("restart changed sealed bytes")
			}
			planned := makeProvider(t.TempDir(), viewAs("r2", 4), "node-b/r2")
			loaded := openRecovered(t, planned)
			if got, err := loaded.Read(1); err != nil || !bytes.Equal(got, want) {
				t.Fatal("planned new holder could not load sealed image", err)
			}
			taken := headHolder(t, remote)
			if taken.State != storage.ChunkHeadActive || taken.Epoch != 4 || taken.Holder != "node-b/r2" || taken.Gen <= cut.Head.Gen || taken.RemoteLSN != cut.H {
				t.Fatalf("planned takeover did not consume preserved seal: %+v", taken)
			}
		})
	}
}
