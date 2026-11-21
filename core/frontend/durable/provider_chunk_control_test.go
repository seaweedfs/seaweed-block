package durable_test

// The provider's chunk control surface (B9 W2 §3.1, §3.6): CutVolume
// returns the HEAD its cut installed, and DetachVolume stops frontend I/O
// and the periodic cuts, then publishes a detached HEAD once.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestWALStoreChunk_CutVolumeReturnsInstalledHead(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	p := chunkProvider(t, t.TempDir(), remote, v1View(), 0)
	s := openRecovered(t, p)
	claimed := headHolder(t, remote)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	r, err := p.CutVolume(ctx, "v1", 0)
	head, _ := remote.GetHead(ctx)
	// (r.Stats is W3's: its fields read zero until then.)
	if err != nil || r.Noop || head.Gen != claimed.Gen+1 || r.Head.Gen != head.Gen || r.H != s.NextLSN()-1 || r.Head.RemoteLSN != r.H {
		t.Fatalf("cut: %+v %v; remote HEAD gen %d", r, err, head.Gen)
	}
	again, err := p.CutVolume(ctx, "v1", 0)
	if err != nil || !again.Noop || again.Head.Gen != r.Head.Gen || again.H != r.H {
		t.Fatalf("cut with nothing new: %+v %v", again, err)
	}
	if _, err := p.CutVolume(ctx, "v9", 0); !errors.Is(err, durable.ErrVolumeNotOpen) {
		t.Fatalf("cut of a volume that is not open: %v", err)
	}
}

func TestWALStoreChunk_DetachVolumeStopsIOAndPublishesDetached(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	p := chunkProvider(t, t.TempDir(), remote, v1View(), time.Hour) // a periodic loop runs until the detach
	s := openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(ctx, 4096, block(2)); err != nil {
		t.Fatal(err)
	}
	r, last, err := p.DetachVolume(ctx, "v1")
	head, _ := remote.GetHead(ctx)
	if err != nil || r.Head.State != storage.ChunkHeadDetached || head.State != storage.ChunkHeadDetached || r.Head.Gen != head.Gen ||
		r.H != s.NextLSN()-1 || last != r.H {
		t.Fatalf("detach: %+v last %d %v; remote HEAD %+v", r, last, err, head)
	}
	if !bytesEqual(headBlock(t, remote, 1), block(2)) {
		t.Fatal("the detached HEAD lacks the write acknowledged before the detach")
	}
	if _, err := b.Write(ctx, 4096, block(3)); err == nil {
		t.Fatal("a frontend write was accepted after the detach")
	}
	again, last2, err := p.DetachVolume(ctx, "v1")
	if err != nil || again.Head.Gen != r.Head.Gen || again.H != r.H || last2 != last {
		t.Fatalf("repeated detach: %+v last %d %v; want the first result", again, last2, err)
	}
	if _, err := p.CutVolume(ctx, "v1", 0); !errors.Is(err, storage.ErrChunkLayerDetached) {
		t.Fatalf("cut after the detach: %v; want ErrChunkLayerDetached", err)
	}
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }
