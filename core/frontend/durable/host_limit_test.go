package durable_test

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
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
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks, BlockSize: 4096, ChunkBlocks: pcChunkBlocks, ChunkBudget: lim}
	fixture.Configure(&cfg)
	p, e := durable.NewDurableProvider(cfg, v1View())
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	s := openRecovered(t, p)
	claimed, found, e := fixture.Catalog.ReadHead(context.Background(), fixture.Ref)
	if e != nil || !found {
		t.Fatalf("initial layout HEAD: found=%v err=%v", found, e)
	}
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
	h, _, _ := fixture.Catalog.ReadHead(context.Background(), fixture.Ref)
	if h.Gen != claimed.Gen {
		t.Fatal("refused cut published")
	}
}

func TestHostUnusablePoolExplicitDurabilityUsesUrgent(t *testing.T) {
	c := hostlimit.DefaultConfig(t.TempDir() + "/missing")
	lim, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks, BlockSize: 4096, ChunkBlocks: pcChunkBlocks, ChunkBudget: lim}
	fixture.Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, v1View())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := openRecovered(t, p)
	if _, err = s.Write(0, block(0x6a)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err = p.CaptureVolume(context.Background(), "v1"); !errors.Is(err, hostlimit.ErrBusy) {
		t.Fatalf("background cut bypassed missing pool: %v", err)
	}
	if err = p.MakeVolumeRemoteDurable(context.Background(), "v1", 1); err != nil {
		t.Fatalf("explicit durability was not urgent: %v", err)
	}
	head, found, err := fixture.Catalog.ReadHead(context.Background(), fixture.Ref)
	if err != nil || !found || head.Position < 1 {
		t.Fatalf("explicit durability did not publish: %+v %v", head, err)
	}
	if st := lim.Snapshot().(hostlimit.Stats); st.Fallback != 1 || st.SkippedByClass[hostlimit.Normal] != 1 {
		t.Fatalf("wrong durability/periodic policy: %+v", st)
	}
}
