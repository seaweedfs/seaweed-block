package chunkstore

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"testing"
)

func TestBCatalogRestoreCopiesVolumeGeometry(t *testing.T) {
	ctx := context.Background()
	c, err := NewBCatalog(newFakeFiler(), newFakeBlobs(), "/geometry")
	if err != nil {
		t.Fatal(err)
	}
	src, target := newRef(t, "source"), newRef(t, "target")
	g := layout.Geometry{SizeBytes: 128 * 4096, BlockSize: 4096, ChunkBytes: 8 * 4096}
	if _, err := c.CreateVolume(ctx, src, g); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Store(src)
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	if _, err := layout.Start(ctx, st, okClaimLease{}, me, layout.LocalRoot{Geometry: g, ClaimID: "k", Fresh: true, Seedable: anySeedable}); err != nil {
		t.Fatal(err)
	}
	const session = "snap"
	if _, err := layout.RunBegin(ctx, st, me, session); err != nil {
		t.Fatal(err)
	}
	root, _, _ := layout.Build(nil)
	rn, raw, _ := layout.NameRoot(root)
	if err := layout.EnsureObject(ctx, st, rn.Path(), raw, session); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunCommit(ctx, st, st, src.Name, me, layout.CommitRequest{Session: session, Snapshot: "s", State: layout.StateLive}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Restore(ctx, src, "s", target); err != nil {
		t.Fatal(err)
	}
	ts, _ := c.Store(target)
	v, found, err := ts.ReadVolumeRecord(ctx)
	if err != nil || !found || v.Geometry != g {
		t.Fatalf("restored geometry %+v found=%v err=%v", v.Geometry, found, err)
	}
	wrong := newRef(t, "wrong")
	bad := g
	bad.ChunkBytes *= 2
	if _, err := c.CreateVolume(ctx, wrong, bad); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Restore(ctx, src, "s", wrong); !errors.Is(err, layout.ErrGeometryMismatch) {
		t.Fatal("restore into different geometry accepted", err)
	}
}
