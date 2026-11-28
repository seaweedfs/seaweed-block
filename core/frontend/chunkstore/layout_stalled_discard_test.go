package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestLayoutStalledSessionYieldAllowsCleaning(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, layer, remote := lh.attach("stalled", true, true)
	defer s.Close()
	data := block(71)
	key := remote.ChunkKey(0, layout.Sum(data))
	if err := remote.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	clean := func() (removed int, err error) {
		err = remote.Clean(ctx, func(ctx context.Context, me layout.HolderID, session string) error {
			st, reader, e := lh.cat.open(lh.v)
			if e != nil {
				return e
			}
			r, e := layout.RemoveUnreachable(ctx, st, reader, st, me, session, layout.Shard(0), []layout.ChunkName{layout.NameChunk(0, data)})
			removed = len(r)
			return e
		})
		return
	}
	if _, err := clean(); !errors.Is(err, layout.ErrBusy) {
		t.Fatalf("cleaning an open session: %v", err)
	}
	stats := layer.Stats()
	if !layer.(storage.ChunkStagedDiscarder).DiscardStagedIfUnchanged(stats.StagedChunks, stats.CutSeq) {
		t.Fatal("idle stalled session was not discarded")
	}
	if n, err := clean(); err != nil || n != 1 {
		t.Fatalf("after yield: removed=%d err=%v; want the unreachable chunk removed", n, err)
	}
}
