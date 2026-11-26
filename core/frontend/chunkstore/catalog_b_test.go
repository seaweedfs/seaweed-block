package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

type okDeleteAuthority struct{}

func (okDeleteAuthority) Reserve(context.Context) error     { return nil }
func (okDeleteAuthority) NoLiveLease(context.Context) error { return nil }

// commitSnapshot makes v hold one chunk at index 1 and a snapshot named
// name of it, through the kernel's own steps: Start, Begin, objects, Commit.
func commitSnapshot(t *testing.T, c *BCatalog, v VolumeRef, data []byte, name string) {
	t.Helper()
	ctx := context.Background()
	s, err := c.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	if _, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k", Fresh: true, CanStartFrom: anyStart}); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunBegin(ctx, s, me, "s1"); err != nil {
		t.Fatal(err)
	}
	ch := layout.NameChunk(1, data)
	if err := layout.EnsureObject(ctx, s, ch.Path(), data, "s1"); err != nil {
		t.Fatal(err)
	}
	next := map[uint64]layout.Ref{1: {Owner: v.Name, OwnerID: v.ID, Chunk: ch}}
	root, segs, err := layout.Build(next)
	if err != nil {
		t.Fatal(err)
	}
	for n, b := range segs {
		if err := layout.EnsureObject(ctx, s, n.Path(), b, "s1"); err != nil {
			t.Fatal(err)
		}
	}
	rn, rb, err := layout.NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := layout.EnsureObject(ctx, s, rn.Path(), rb, "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunCommit(ctx, s, s, v.Name, me, layout.CommitRequest{Session: "s1", Next: next, Position: 1, Snapshot: name}); err != nil {
		t.Fatal(err)
	}
}

func newRef(t *testing.T, name string) VolumeRef {
	t.Helper()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	return VolumeRef{Name: name, ID: id}
}

// A restore target reads the source's chunk through its copied root; the
// source's snapshot cannot be deleted while the target depends on it; the
// target's delete releases it, and then the snapshot deletes.
func TestBCatalog_RestoreThenDeleteTheTarget(t *testing.T) {
	ctx := context.Background()
	c, err := NewBCatalog(newFakeFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := newRef(t, "src"), newRef(t, "t")
	if _, err := c.CreateVolume(ctx, src, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	data := []byte("source chunk")
	commitSnapshot(t, c, src, data, "daily")

	th, err := c.Restore(ctx, src, "daily", tgt)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if th.Holder != "" || th.Current == nil {
		t.Fatalf("target HEAD %+v; want unclaimed with the copied root", th)
	}
	ts, tr, err := c.open(tgt)
	if err != nil {
		t.Fatal(err)
	}
	img, err := tr.Open(ctx, *th.Current)
	if err != nil {
		t.Fatal(err)
	}
	got, err := img.Chunk(ctx, 1)
	if err != nil || string(got.Data) != string(data) || got.Ref.OwnerID != src.ID {
		t.Fatalf("target reads chunk 1: %+v %v; want the source's bytes", got, err)
	}
	if u, err := c.Usage(ctx, tgt); err != nil || u.ForeignChunks != 1 || u.OwnChunks != 0 {
		t.Fatalf("target usage %+v %v", u, err)
	}
	if u, err := c.Usage(ctx, src); err != nil || u.Dependents != 1 || u.OwnChunks != 1 {
		t.Fatalf("source usage %+v %v", u, err)
	}
	if _, err := c.Restore(ctx, src, "daily", tgt); err != nil {
		t.Fatalf("restore again: %v; want a replay", err)
	}
	if _, err := c.DeleteSnapshot(ctx, src, "daily"); !errors.Is(err, layout.ErrHasDependents) {
		t.Fatalf("delete a depended-on snapshot: %v; want ErrHasDependents", err)
	}

	if _, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); err != nil {
		t.Fatalf("delete the target: %v", err)
	}
	if h, _, err := ts.ReadHead(ctx); err != nil || h.State != layout.StateDeleted {
		t.Fatalf("target HEAD after delete: %+v %v", h, err)
	}
	if objs, err := c.listObjects(ctx, tgt); err != nil || len(objs) != 0 {
		t.Fatalf("target objects after delete: %v %v", objs, err)
	}
	if _, err := c.DeleteSnapshot(ctx, src, "daily"); err != nil {
		t.Fatalf("delete the released snapshot: %v", err)
	}
	if v, ok, err := ts.ReadVolumeRecord(ctx); err != nil || !ok || v.ID != tgt.ID {
		t.Fatalf("target VOLUME after delete: %+v %t %v; want kept", v, ok, err)
	}
}

// A snapshot of a restore target names the source's chunks, so it cannot
// itself be a restore source (dependency depth stays one).
func TestBCatalog_RestoreOfARestoreTargetIsRefused(t *testing.T) {
	ctx := context.Background()
	c, err := NewBCatalog(newFakeFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt, again := newRef(t, "src"), newRef(t, "t"), newRef(t, "t2")
	if _, err := c.CreateVolume(ctx, src, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	commitSnapshot(t, c, src, []byte("x"), "daily")
	if _, err := c.Restore(ctx, src, "daily", tgt); err != nil {
		t.Fatal(err)
	}
	ts, _ := c.Store(tgt)
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	if _, err := layout.Start(ctx, ts, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k", Fresh: true, CanStartFrom: anyStart}); err != nil {
		t.Fatal(err)
	}
	h, _, _ := ts.ReadHead(ctx)
	if _, err := layout.RunBegin(ctx, ts, me, "s2"); err != nil {
		t.Fatal(err)
	}
	img, _ := layout.NewReader("/blocks", tgt.Dir(), ts.Fetch)
	im, _ := img.Open(ctx, *h.Current)
	ref, _, _ := im.Resolve(ctx, 1)
	prev := map[uint64]layout.Ref{1: ref}
	root, segs, _ := layout.Build(prev)
	for n, b := range segs {
		if err := layout.EnsureObject(ctx, ts, n.Path(), b, "s2"); err != nil {
			t.Fatal(err)
		}
	}
	rn, rb, _ := layout.NameRoot(root)
	if err := layout.EnsureObject(ctx, ts, rn.Path(), rb, "s2"); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunCommit(ctx, ts, ts, tgt.Name, me, layout.CommitRequest{Session: "s2", Prev: prev, Next: prev, Position: 2, Snapshot: "copy"}); err != nil {
		t.Fatalf("snapshot of the target: %v", err)
	}
	if _, err := c.Restore(ctx, tgt, "copy", again); !errors.Is(err, layout.ErrForeignSource) {
		t.Fatalf("restore from a snapshot naming another volume's chunks: %v; want ErrForeignSource", err)
	}
}

// A restore target that was never started still names the source's
// chunks: the operator's release of it is refused, and the source keeps its
// dependent.
func TestBCatalog_ReleaseOfAnUnstartedRestoreTargetIsRefused(t *testing.T) {
	ctx := context.Background()
	c, err := NewBCatalog(newFakeFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := newRef(t, "src"), newRef(t, "t")
	if _, err := c.CreateVolume(ctx, src, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	commitSnapshot(t, c, src, []byte("data"), "daily")
	if _, err := c.Restore(ctx, src, "daily", tgt); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReleaseTarget(ctx, src, tgt); !errors.Is(err, layout.ErrTargetLive) {
		t.Fatalf("release of an unstarted restore target: %v; want ErrTargetLive", err)
	}
	if h, _, _ := c.ReadHead(ctx, src); len(h.Snapshots["daily"].Dependents) != 1 {
		t.Fatalf("source dependents after the refused release: %+v", h.Snapshots["daily"].Dependents)
	}
}
