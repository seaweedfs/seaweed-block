package durable

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// loopLayer is the part of a chunk layer the cut loop uses.
type loopLayer struct {
	storage.ChunkLayer // unused methods panic
	fenced             atomic.Bool
	cuts               atomic.Int32
}

func (l *loopLayer) Fenced() bool                   { return l.fenced.Load() }
func (l *loopLayer) Stats() storage.ChunkLayerStats { return storage.ChunkLayerStats{} }
func (l *loopLayer) Cut(context.Context) (storage.ChunkCutResult, error) {
	l.cuts.Add(1)
	return storage.ChunkCutResult{}, nil
}

// A fenced layer can never publish again: its loop ends instead of
// retrying refused cuts forever.
func TestChunkLoop_EndsWhenTheLayerIsFenced(t *testing.T) {
	p := &DurableProvider{cfg: ProviderConfig{ChunkCaptureInterval: time.Millisecond}}
	layer := &loopLayer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go p.chunkLoop(ctx, "v1", layer, done, nil, nil)
	deadline := time.Now().Add(5 * time.Second)
	for layer.cuts.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the loop never cut before the fence")
		}
		time.Sleep(10 * time.Millisecond)
	}
	layer.fenced.Store(true)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the cut loop kept running after the layer was fenced")
	}
}
