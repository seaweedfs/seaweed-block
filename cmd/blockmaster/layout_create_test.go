package main

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

type failFirstHead struct {
	chunkstore.Filer
	fail bool
}

func (f *failFirstHead) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, excl bool) error {
	if e.Name == "HEAD" && f.fail {
		f.fail = false
		return errors.New("fixture: lost filer HEAD create")
	}
	return f.Filer.CreateEntry(ctx, dir, e, excl)
}

type noCreateBlobs struct{}

func (noCreateBlobs) Put(context.Context, []byte) (string, error) {
	return "", errors.New("unexpected blob upload")
}
func (noCreateBlobs) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("unexpected blob read")
}

func TestMasterLayoutCreatorWritesUnclaimedHeadAndReplays(t *testing.T) {
	ctx := context.Background()
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), noCreateBlobs{}, "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	spec := lifecycle.VolumeSpec{VolumeID: "v", LayoutID: id.String(), SizeBytes: 1 << 20, BlockSize: 4096, ChunkBytes: 4 << 20, ReplicationFactor: 1}
	create := layoutVolumeCreator(cat)
	var firstGen uint64
	for i := 0; i < 2; i++ {
		if err := create(ctx, spec); err != nil {
			t.Fatalf("create attempt %d: %v", i, err)
		}
		h, found, err := cat.ReadHead(ctx, chunkstore.VolumeRef{Name: "v", ID: id})
		if err != nil || !found {
			t.Fatalf("read HEAD after attempt %d: found=%t err=%v", i, found, err)
		}
		if i == 0 {
			firstGen = h.Gen
		} else if h.Gen != firstGen {
			t.Fatalf("replay changed HEAD generation %d -> %d", firstGen, h.Gen)
		}
	}
	st, err := cat.Store(chunkstore.VolumeRef{Name: "v", ID: id})
	if err != nil {
		t.Fatal(err)
	}
	v, found, err := st.ReadVolumeRecord(ctx)
	if err != nil || !found || v.Geometry != (layout.Geometry{SizeBytes: spec.SizeBytes, BlockSize: spec.BlockSize, ChunkBytes: spec.ChunkBytes}) {
		t.Fatalf("VOLUME=%+v found=%t err=%v", v, found, err)
	}
	h, found, err := st.ReadHead(ctx)
	if err != nil || !found || h.Epoch != 0 || h.Holder != "" || h.ClaimID != "" || h.Gen == 0 {
		t.Fatalf("unclaimed HEAD=%+v found=%t err=%v", h, found, err)
	}
}

func TestMasterLayoutCreatorHealsCrashBetweenVolumeAndHead(t *testing.T) {
	ctx := context.Background()
	filer := &failFirstHead{Filer: chunkstore.NewMemFiler(), fail: true}
	cat, err := chunkstore.NewBCatalog(filer, noCreateBlobs{}, "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v", ID: id}
	spec := lifecycle.VolumeSpec{VolumeID: ref.Name, LayoutID: id.String(), SizeBytes: 1 << 20, BlockSize: 4096, ChunkBytes: 4 << 20, ReplicationFactor: 1}
	create := layoutVolumeCreator(cat)
	if err := create(ctx, spec); err == nil {
		t.Fatal("first HEAD create did not fail")
	}
	st, err := cat.Store(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := st.ReadVolumeRecord(ctx); err != nil || !found {
		t.Fatalf("VOLUME after interrupted create: found=%t err=%v", found, err)
	}
	if _, found, err := st.ReadHead(ctx); err != nil || found {
		t.Fatalf("HEAD after interrupted create: found=%t err=%v", found, err)
	}
	if err := create(ctx, spec); err != nil {
		t.Fatalf("retry did not complete HEAD: %v", err)
	}
	if h, found, err := st.ReadHead(ctx); err != nil || !found || h.Epoch != 0 {
		t.Fatalf("healed HEAD=%+v found=%t err=%v", h, found, err)
	}
}
