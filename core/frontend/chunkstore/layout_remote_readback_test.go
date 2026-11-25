package chunkstore

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// commitThenChangeHEAD places an operator write after the holder's commit but
// before the engine's confirmation read, which is the ordering that used to
// make a healthy holder fence itself.
type commitThenChangeHEAD struct {
	*LayoutRemote
	armed  bool
	change func(context.Context) error
}

func (r *commitThenChangeHEAD) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	gen, err := r.LayoutRemote.CASHead(ctx, expected, next)
	if err == nil && r.armed {
		r.armed = false
		if err := r.change(ctx); err != nil {
			return 0, err
		}
	}
	return gen, err
}

func TestLayoutRemote_OperatorHEADWriteBetweenCommitAndReadback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(context.Context, *layoutHost) error
	}{
		{"delete snapshot", func(ctx context.Context, lh *layoutHost) error {
			_, err := lh.cat.DeleteSnapshot(ctx, lh.v, "old")
			return err
		}},
		{"retire snapshot", func(ctx context.Context, lh *layoutHost) error {
			st, _ := lh.cat.Store(lh.v)
			_, err := layout.RunRetireSnapshot(ctx, st, "old", 10)
			return err
		}},
		{"add dependent", func(ctx context.Context, lh *layoutHost) error {
			st, reader, err := lh.cat.open(lh.v)
			if err != nil {
				return err
			}
			_, err = layout.RunAddDependent(ctx, st, reader, "old", layout.Dependent{Target: "target", TargetID: layout.VolumeID{7}})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lh := newLayoutHost(t)
			s, err := storage.CreateWALStoreWithOptions(lh.dir+"/active", lrBlocks, 4096, storage.WALStoreOptions{ChunkBlocks: lrChunkBlocks})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err := s.Recover(); err != nil {
				t.Fatal(err)
			}
			inner, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
			if err != nil {
				t.Fatal(err)
			}
			r := &commitThenChangeHEAD{LayoutRemote: inner}
			layer, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: lrChunkBlocks, Epoch: lh.me.Epoch, Holder: lh.me.Holder, FreshFromHead: true})
			if err != nil {
				t.Fatal(err)
			}
			writeSync(t, s, map[uint32][]byte{2: block(2)})
			inner.SetSnapshot("old", layout.SnapshotMeta{CreatedNs: 1})
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatal(err)
			}
			writeSync(t, s, map[uint32][]byte{3: block(3)})
			r.change = func(ctx context.Context) error { return tc.change(ctx, lh) }
			r.armed = true
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatalf("cut after operator HEAD write: %v", err)
			}
			writeSync(t, s, map[uint32][]byte{4: block(4)})
			if _, err := layer.Capture(ctx); err != nil {
				t.Fatalf("next cut after operator HEAD write: %v", err)
			}
			lh.readBack("reader", map[uint32][]byte{2: block(2), 3: block(3), 4: block(4)})
		})
	}
}

// The layout records chunk names, not each chunk's birth LSN. A later cut
// that advances HEAD while reusing an older chunk must still permit a no-op
// cut: the same object and holder cannot become a competing publication.
func TestLayoutRemote_ReusedChunkBirthDoesNotFenceNoopCut(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, layer, _ := lh.attach("active", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{1: block(1)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	writeSync(t, s, map[uint32][]byte{9: block(9)})
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Capture(ctx); err != nil {
		t.Fatalf("no-op cut fenced the same holder after reusing a chunk: %v", err)
	}
	lh.readBack("reader", map[uint32][]byte{1: block(1), 9: block(9)})
}
