package durable_test

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"sync/atomic"
	"testing"
	"time"
)

type frozenCountRemote struct {
	storage.ChunkRemote
	puts atomic.Int64
}

func (r *frozenCountRemote) Put(ctx context.Context, key string, p []byte) error {
	r.puts.Add(1)
	return r.ChunkRemote.Put(ctx, key, p)
}
func TestFrozenProviderFlagOffRefusesWithoutUploadThenFinalCommits(t *testing.T) {
	mem := storage.NewMemChunkRemote()
	remote := &frozenCountRemote{ChunkRemote: mem}
	p, _ := parkedProvider(t, remote)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Write(context.Background(), 4096, block(0x77)); err != nil {
		t.Fatal(err)
	}
	before, _ := remote.GetHead(context.Background())
	puts := remote.puts.Load()
	r, _, err := p.DetachVolumeFrozen(context.Background(), "v1", storage.FrozenDetachOptions{StartedAt: time.Now().Add(-6 * time.Second)})
	after, _ := remote.GetHead(context.Background())
	if !errors.Is(err, storage.ErrFrozenTailTooLarge) || r.Precopy.Frozen || puts != remote.puts.Load() || before.Gen != after.Gen {
		t.Fatalf("frozen refusal mutated remote: %+v %v puts=%d/%d", r.Precopy, err, puts, remote.puts.Load())
	}
	if _, err = b.Write(context.Background(), 8192, block(0x88)); err != nil {
		t.Fatalf("refusal stopped serving before thaw: %v", err)
	}
	r, _, err = p.DetachVolumeFrozen(context.Background(), "v1", storage.FrozenDetachOptions{StartedAt: time.Now().Add(-time.Minute), AllowOverBudget: true})
	if err != nil || !r.Precopy.GuestFrozen || !r.Precopy.OverBudget || !r.Precopy.Frozen || r.Head.State != storage.ChunkHeadDetached || len(r.Precopy.Rounds) != 1 || r.Precopy.FreezeElapsed < time.Minute || r.Precopy.DeclaredWorst < time.Minute || !r.Precopy.LatencyGateFailed {
		t.Fatalf("explicit final failed/underreported total freeze: %+v %v", r, err)
	}
	if headBlock(t, mem, 1)[0] != 0x77 || headBlock(t, mem, 2)[0] != 0x88 {
		t.Fatal("final detached point lost writes")
	}
}
