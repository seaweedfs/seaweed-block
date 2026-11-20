package durable_test

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
)

func TestHostCutLimitDoesNotGateGuestIO(t *testing.T) {
	c := hostlimit.DefaultConfig(t.TempDir())
	c.NormalSlots = 1
	c.UrgentSlots = 1
	holder, e := hostlimit.New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer holder.Close()
	x, e := holder.Admit(context.Background(), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer holder.Release(x)
	lim, e := hostlimit.New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer lim.Close()
	r := storage.NewMemChunkRemote()
	p, e := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks, BlockSize: 4096, ChunkBlocks: pcChunkBlocks, ChunkBudget: lim, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return r, nil }}, v1View())
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	s := openRecovered(t, p)
	if _, e = s.Write(0, block(0x72)); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Sync(); e != nil {
		t.Fatal(e)
	}
	if _, e = p.CaptureVolume(context.Background(), "v1"); !errors.Is(e, storage.ErrChunkCaptureRefused) {
		t.Fatalf("full cut pool: %v", e)
	}
	if _, e = s.Write(1, block(0x73)); e != nil {
		t.Fatal("limiter blocked write", e)
	}
	if _, e = s.Sync(); e != nil {
		t.Fatal("limiter blocked Flush", e)
	}
	if b, e := s.Read(0); e != nil || b[0] != 0x72 {
		t.Fatal("limiter blocked read", e)
	}
	h, _ := r.GetHead(context.Background())
	if h.Gen != 0 {
		t.Fatal("refused cut published")
	}
}
