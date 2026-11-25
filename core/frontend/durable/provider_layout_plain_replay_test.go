package durable_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
	"time"
)

func TestLayoutPlainDetachReplayAfterRestartNeedsNoLegacyFactory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), &layoutProviderBlobs{blobs: map[string][]byte{}}, "/plain-replay")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
	var proofErr error
	var absent, foreign bool
	var proofReads int
	var onProofRead func()
	open := func() (*durable.DurableProvider, *storage.WALStore) {
		p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: dir, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1",
			ChunkLayoutHeadFor: func(string) (layout.StartStore, error) {
				st, err := cat.Store(ref)
				if err != nil {
					return nil, err
				}
				return &layoutReplayFault{StartStore: st, err: &proofErr, absent: &absent, foreign: &foreign, reads: &proofReads, onRead: &onProofRead}, nil
			},
			ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
				return chunkstore.NewLayoutRemote(cat, ref, me)
			},
		}, view)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		s, err := p.EnsureStorage("v1")
		if err != nil {
			t.Fatal(err)
		}
		return p, s.(*storage.WALStore)
	}
	p, ws := open()
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Write(0, bytes.Repeat([]byte{'A'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Sync(); err != nil {
		t.Fatal(err)
	}
	first, last, err := p.DetachVolumePlain(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Head.State != storage.ChunkHeadDetached || first.H != last {
		t.Fatalf("detach proof %+v last=%d", first, last)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, _ := open()
	if _, err := restarted.RecoverVolume(ctx, "v1"); !errors.Is(err, layout.ErrDetached) {
		t.Fatalf("same epoch restart: %v", err)
	}
	beforeReplayReads := proofReads
	replay, again, err := restarted.DetachVolumePlain(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if replay.Head.Gen != first.Head.Gen || replay.H != first.H || again != last {
		t.Fatalf("replay changed detached proof: %+v %d", replay, again)
	}
	if proofReads <= beforeReplayReads {
		t.Fatal("replay did not read the B HEAD")
	}
	for _, mode := range []string{"unreadable", "absent", "foreign"} {
		proofErr = nil
		absent = false
		foreign = false
		switch mode {
		case "unreadable":
			proofErr = errors.New("fixture filer unavailable")
		case "absent":
			absent = true
		case "foreign":
			foreign = true
		}
		if _, _, err := restarted.DetachVolumePlain(ctx, "v1"); err == nil {
			t.Fatalf("%s detached proof returned cached success", mode)
		}
	}
	proofErr, absent, foreign = nil, false, false
	initial := view.Projection()
	onProofRead = func() {
		moved := initial
		moved.EndpointVersion++
		view.set(moved)
	}
	if _, _, err := restarted.DetachVolumePlain(ctx, "v1"); !errors.Is(err, durable.ErrChunkEpochStale) {
		t.Fatalf("replay ignored authority movement during B HEAD read: %v", err)
	}
	onProofRead = nil
	view.set(initial)
	ended, end := context.WithCancel(ctx)
	end()
	if _, _, err := restarted.DetachVolumePlain(ended, "v1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("replay hid caller cancellation: %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := restarted.DetachVolumePlain(ctx, "v1"); err == nil {
		t.Fatal("closed provider replayed a seal")
	}

}

type lostLayoutDetachAnswer struct {
	*chunkstore.LayoutRemote
	lose bool
}

func (r *lostLayoutDetachAnswer) CASHead(ctx context.Context, version storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	gen, err := r.LayoutRemote.CASHead(ctx, version, next)
	if err == nil && r.lose && next.State == storage.ChunkHeadDetached {
		return 0, errors.New("fixture: detached commit reply lost")
	}
	return gen, err
}

func (r *lostLayoutDetachAnswer) AbortOpenSession(ctx context.Context) (string, error) {
	if r.lose {
		return "", errors.New("fixture: settlement read unavailable")
	}
	return r.LayoutRemote.AbortOpenSession(ctx)
}

func TestLayoutPlainDetachLostReplyRestartReplaysProof(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newLayoutCase(t, 64, 4)
	root := t.TempDir()
	first := c.provider(t, root, v1View(), "s/r1", func(cfg *durable.ProviderConfig) {
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			r, err := chunkstore.NewLayoutRemote(c.cat, c.ref, me)
			if err != nil {
				return nil, err
			}
			return &lostLayoutDetachAnswer{LayoutRemote: r, lose: true}, nil
		}
	})
	ws := openRecovered(t, first)
	if _, err := ws.Write(0, bytes.Repeat([]byte{'Z'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.DetachVolumePlain(ctx, "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("committed seal with lost reply was decided without proof: %v", err)
	}
	sealed := c.head(t)
	if sealed.State != layout.StateDetached {
		t.Fatalf("lost reply did not commit: %+v", sealed)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := c.provider(t, root, v1View(), "s/r1", nil)
	if _, err := restarted.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.RecoverVolume(ctx, "v1"); !errors.Is(err, layout.ErrDetached) {
		t.Fatalf("same epoch served after seal: %v", err)
	}
	replay, last, err := restarted.DetachVolumePlain(ctx, "v1")
	if err != nil || replay.Head.Gen != sealed.Gen || replay.H != sealed.Position || last != sealed.Position {
		t.Fatalf("replay did not return the committed B proof: %+v last=%d err=%v", replay, last, err)
	}
	if got := c.head(t); got.Gen != sealed.Gen || got.State != layout.StateDetached {
		t.Fatalf("replay wrote HEAD: %+v", got)
	}
	if !bytes.Equal(c.block(t, 0), bytes.Repeat([]byte{'Z'}, 4096)) {
		t.Fatal("replay lost the acknowledged write")
	}
}

func TestPlainDetachRefusesLegacyHeadProof(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), v1View(), 0)
	store := openRecovered(t, p)
	writeSync(t, store, 0, 'A')
	if _, _, err := p.DetachVolumePlain(ctx, "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("plain detach accepted a legacy HEAD proof: %v", err)
	}
}

type layoutReplayFault struct {
	layout.StartStore
	err             *error
	absent, foreign *bool
	reads           *int
	onRead          *func()
}

func (s *layoutReplayFault) ReadHead(ctx context.Context) (layout.Head, bool, error) {
	*s.reads++
	if *s.onRead != nil {
		(*s.onRead)()
	}
	if *s.err != nil {
		return layout.Head{}, false, *s.err
	}
	if *s.absent {
		return layout.Head{}, false, nil
	}
	h, found, err := s.StartStore.ReadHead(ctx)
	if *s.foreign {
		h.ClaimID = "foreign"
	}
	return h, found, err
}
