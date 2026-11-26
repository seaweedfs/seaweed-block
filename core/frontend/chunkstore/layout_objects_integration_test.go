//go:build chunkstore_integration

package chunkstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// On a real filer (stock weed): the removal's two conditional writes and
// the upload's reuse write behave as the layout needs. A delete whose
// mark does not match is refused; a reuse (another session, mark cleared)
// makes the remover's late mark and late delete both fail their
// conditions and the object stays; a mark on the current metadata and its
// own delete remove it.
func TestIntegration_ObjectMarkAndConditionalDelete(t *testing.T) {
	c := startWeed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	root := "/swblock/objs/" + layout.VolumeDir("v1", id)
	cl := c.client(t, root+"/")
	f, err := NewLayoutStore(cl, cl, root, id)
	if err != nil {
		t.Fatal(err)
	}
	rel := layout.NameChunk(5, []byte("orphan")).Path()

	if err := layout.EnsureObject(ctx, f, rel, []byte("orphan"), "s0"); err != nil {
		t.Fatal(err)
	}
	plan, found, err := f.Stat(ctx, rel)
	if err != nil || !found || plan != (layout.ObjectMeta{Session: "s0", Volume: id}) {
		t.Fatalf("created object: %+v %t %v", plan, found, err)
	}
	if err := f.Mark(ctx, rel, plan, "c1"); err != nil {
		t.Fatalf("mark on the plan's metadata: %v", err)
	}
	if err := f.DeleteMarked(ctx, rel, "c9"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("delete with another mark: %v; want refused by the filer", err)
	}
	if _, found, _ := f.Stat(ctx, rel); !found {
		t.Fatal("a refused conditional delete removed the entry")
	}
	// A successor reuses the object: session set, mark cleared.
	if err := layout.EnsureObject(ctx, f, rel, []byte("orphan"), "successor"); err != nil {
		t.Fatal(err)
	}
	if m, _, _ := f.Stat(ctx, rel); m != (layout.ObjectMeta{Session: "successor", Volume: id}) {
		t.Fatalf("after the reuse: %+v; want session successor, unmarked", m)
	}
	if err := f.DeleteMarked(ctx, rel, "c1"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("the old remover's late delete: %v; want refused by the filer", err)
	}
	if err := f.Mark(ctx, rel, plan, "c1"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("the old remover's late mark on its plan's metadata: %v; want refused by the filer", err)
	}
	if _, found, _ := f.Stat(ctx, rel); !found {
		t.Fatal("the reused object was removed")
	}
	// A remover planning on the current metadata removes it.
	now, _, _ := f.Stat(ctx, rel)
	if err := f.Mark(ctx, rel, now, "c2"); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteMarked(ctx, rel, "c2"); err != nil {
		t.Fatalf("delete with its own mark: %v", err)
	}
	if _, found, _ := f.Stat(ctx, rel); found {
		t.Fatal("the marked object was not deleted")
	}
	if err := f.DeleteMarked(ctx, rel, "c2"); err != nil {
		t.Fatalf("delete of a missing entry: %v; want success", err)
	}
}

// On a real filer: the catalog's whole life of a restore. A source with a
// committed snapshot; a restore target that reads the source's chunk
// through its copied root; the snapshot refused deletion while the target
// depends on it; the target's delete empties its pool, keeps its VOLUME
// and a deleted HEAD, and releases the source, whose snapshot then deletes.
func TestIntegration_CatalogRestoreAndDelete(t *testing.T) {
	c := startWeed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const base = "/swblock/cat"
	cl := c.client(t, base+"/")
	cat, err := NewBCatalog(cl, cl, base)
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := newRef(t, "src"), newRef(t, "t")
	if _, err := cat.CreateVolume(ctx, src, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	data := []byte("real source chunk")
	commitSnapshot(t, cat, src, data, "daily")
	th, err := cat.Restore(ctx, src, "daily", tgt)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	_, tr, err := cat.open(tgt)
	if err != nil {
		t.Fatal(err)
	}
	img, err := tr.Open(ctx, *th.Current)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := img.Chunk(ctx, 1); err != nil || string(got.Data) != string(data) {
		t.Fatalf("target reads the source's chunk: %+v %v", got, err)
	}
	if _, err := cat.DeleteSnapshot(ctx, src, "daily"); !errors.Is(err, layout.ErrHasDependents) {
		t.Fatalf("delete a depended-on snapshot: %v", err)
	}
	if _, err := cat.DeleteVolume(ctx, tgt, okDeleteAuthority{}); err != nil {
		t.Fatalf("delete the target: %v", err)
	}
	if objs, err := cat.listObjects(ctx, tgt); err != nil || len(objs) != 0 {
		t.Fatalf("target objects after delete: %v %v", objs, err)
	}
	if h, found, err := cat.ReadHead(ctx, tgt); err != nil || !found || h.State != layout.StateDeleted {
		t.Fatalf("target HEAD after delete: %+v %t %v", h, found, err)
	}
	if _, err := cat.DeleteSnapshot(ctx, src, "daily"); err != nil {
		t.Fatalf("delete the released snapshot: %v", err)
	}
}
