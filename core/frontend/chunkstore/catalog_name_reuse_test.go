package chunkstore

import (
	"context"
	"errors"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// headDropFiler refuses the first create of HEAD in dir: the restore dies
// after its VOLUME and before its HEAD.
type headDropFiler struct {
	Filer
	dir     string
	dropped bool
}

func (f *headDropFiler) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	if !f.dropped && dir == f.dir && e.GetName() == "HEAD" {
		f.dropped = true
		return errors.New("injected: the restore died before its HEAD")
	}
	return f.Filer.CreateEntry(ctx, dir, e, oExcl)
}

// A restore is abandoned between its VOLUME and its HEAD; the operator
// releases it (its directory gets a deleted HEAD first), the source's
// snapshot is deleted, and the name is taken by a new volume. Then the
// abandoned restore's late HEAD create lands. It must be refused: the old
// directory is never reused (the new volume has its own) and the deleted
// HEAD there stops the create, so no live target ever exists without the
// snapshot whose chunks it names, and the new volume is untouched.
func TestBCatalog_AbandonedRestoreNameReusedLateHeadCreateRefused(t *testing.T) {
	ctx := context.Background()
	geometry := layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}
	src, x := newRef(t, "src"), newRef(t, "t")
	f := &headDropFiler{Filer: newFakeFiler(), dir: path.Join("/blocks", x.Dir())}
	c, err := NewBCatalog(f, newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateVolume(ctx, src, geometry); err != nil {
		t.Fatal(err)
	}
	commitSnapshot(t, c, src, []byte("source chunk"), "daily")

	if _, err := c.Restore(ctx, src, "daily", x); err == nil || !f.dropped {
		t.Fatalf("restore: %v (dropped %t); want it to die before its HEAD", err, f.dropped)
	}
	if _, found, err := c.ReadHead(ctx, x); err != nil || found {
		t.Fatalf("abandoned target HEAD found=%t %v; want none", found, err)
	}
	if _, err := c.ReleaseTarget(ctx, src, x); err != nil {
		t.Fatalf("operator release of the abandoned target: %v", err)
	}
	if _, err := c.DeleteSnapshot(ctx, src, "daily"); err != nil {
		t.Fatalf("snapshot delete after the release: %v", err)
	}
	y := VolumeRef{Name: x.Name, ID: newRef(t, "t").ID} // the name is reused
	if _, err := c.CreateVolume(ctx, y, geometry); err != nil {
		t.Fatalf("a new volume under the freed name: %v", err)
	}
	yh, found, err := c.ReadHead(ctx, y)
	if err != nil || !found {
		t.Fatalf("new volume HEAD found=%t %v", found, err)
	}

	xs, err := c.Store(x)
	if err != nil {
		t.Fatal(err)
	}
	if err := xs.CreateHead(ctx, layout.Unclaimed(x.ID, nil)); !errors.Is(err, layout.ErrExists) {
		t.Fatalf("the abandoned restore's late HEAD create: %v; want ErrExists (a live target without its snapshot)", err)
	}
	if xh, found, err := c.ReadHead(ctx, x); err != nil || !found || xh.State != layout.StateDeleted {
		t.Fatalf("abandoned target HEAD %+v found=%t %v; want it deleted", xh, found, err)
	}
	if got, found, err := c.ReadHead(ctx, y); err != nil || !found || got.VolumeID != y.ID || got.Gen != yh.Gen || got.State != yh.State {
		t.Fatalf("new volume HEAD %+v found=%t %v; want its own, unchanged (%+v)", got, found, err, yh)
	}
}
