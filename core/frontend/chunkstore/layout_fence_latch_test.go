package chunkstore

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// moveHeadOnPut is the layout remote with HEAD moved by another owner at
// its first chunk upload: the cut's commit is the site that observes it.
type moveHeadOnPut struct {
	*LayoutRemote
	once sync.Once
	move func()
}

func (r *moveHeadOnPut) Put(ctx context.Context, key string, data []byte) error {
	r.once.Do(r.move)
	return r.LayoutRemote.Put(ctx, key, data)
}

// Every identity-moved signal latches the layer fenced at every call site
// that can observe it, so the one place writes are admitted (Fenced, read by
// the durable backend's ready check) refuses them from then on: an
// ex-holder never acknowledges a write it can no longer publish. A new
// signal or site belongs in this table.
func TestLayoutFenceLatchesAtEverySite(t *testing.T) {
	signals := map[string]func(h layout.Head) layout.Head{
		"newer epoch":        func(h layout.Head) layout.Head { h.Epoch++; h.Holder = "b"; return h },
		"same epoch, holder": func(h layout.Head) layout.Head { h.Holder = "b"; return h },
		"same epoch, claim":  func(h layout.Head) layout.Head { h.ClaimID = "k-other"; return h },
		"volume deleting":    func(h layout.Head) layout.Head { h.State = layout.StateDeleting; return h },
	}
	type site struct {
		name    string
		onPut   bool // HEAD moves at the first upload instead of before the call
		observe func(context.Context, *storage.WALStore, storage.ChunkLayer) error
	}
	sites := []site{
		{"cut", false, func(ctx context.Context, _ *storage.WALStore, l storage.ChunkLayer) error {
			_, err := l.Capture(ctx)
			return err
		}},
		{"commit after upload", true, func(ctx context.Context, _ *storage.WALStore, l storage.ChunkLayer) error {
			_, err := l.Capture(ctx)
			return err
		}},
		{"make remote durable", false, func(ctx context.Context, s *storage.WALStore, l storage.ChunkLayer) error {
			return l.MakeRemoteDurable(ctx, s.NextLSN()-1)
		}},
		{"detach", false, func(ctx context.Context, _ *storage.WALStore, l storage.ChunkLayer) error {
			_, err := l.Detach(ctx)
			return err
		}},
		// The holder's cleaning runs outside the layer; its refusal is
		// classified (LayoutErr) and handed to the layer, as the provider's
		// maintenance loop does.
		{"cleaning", false, nil},
	}
	for sigName, move := range signals {
		for _, st := range sites {
			t.Run(sigName+"/"+st.name, func(t *testing.T) {
				ctx := context.Background()
				lh := newLayoutHost(t)
				moveHead := func() {
					hs, err := lh.cat.Store(lh.v)
					if err != nil {
						t.Error(err)
						return
					}
					h, _, err := hs.ReadHead(ctx)
					if err != nil {
						t.Error(err)
						return
					}
					if err := hs.UpdateHead(ctx, h.Gen, move(h)); err != nil {
						t.Error(err)
					}
				}
				s, err := storage.CreateWALStoreWithOptions(filepath.Join(lh.dir, "s"), lrBlocks, 4096, storage.WALStoreOptions{ChunkBlocks: lrChunkBlocks})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				if _, err := s.Recover(); err != nil {
					t.Fatal(err)
				}
				r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
				if err != nil {
					t.Fatal(err)
				}
				var remote storage.ChunkRemote = r
				if st.onPut {
					remote = &moveHeadOnPut{LayoutRemote: r, move: moveHead}
				}
				l, err := storage.AttachChunkLayer(s, remote, storage.ChunkLayerConfig{ChunkBlocks: lrChunkBlocks, Epoch: lh.me.Epoch, Holder: lh.me.Holder, FreshFromHead: true})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = l.Close() })
				writeSync(t, s, map[uint32][]byte{1: block(1)})
				if !st.onPut {
					moveHead()
				}
				if st.observe == nil { // cleaning
					_, cerr := lh.cat.Clean(ctx, lh.v, lh.me, "clean-x")
					err = LayoutErr(cerr)
					if obs, ok := l.(interface{ ObserveRemoteError(error) error }); ok {
						_ = obs.ObserveRemoteError(err)
					}
				} else {
					err = st.observe(ctx, s, l)
				}
				if err == nil {
					t.Fatalf("%s did not observe the moved HEAD", st.name)
				}
				if !l.Fenced() {
					t.Fatalf("%s observed %v but the layer is not fenced: writes would still be admitted", st.name, err)
				}
				if _, err := l.Capture(ctx); !errors.Is(err, storage.ErrChunkLayerFenced) {
					t.Fatalf("a capture after the latch: %v; want ErrChunkLayerFenced", err)
				}
			})
		}
	}
}
