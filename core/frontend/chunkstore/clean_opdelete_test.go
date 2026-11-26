package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// markHookFiler runs hook before every MARK of a pool object (the removal's
// first write), with the object's name.
type markHookFiler struct {
	*MemFiler
	hook func(ctx context.Context, name string) error
}

func (f *markHookFiler) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	if f.hook != nil && string(e.Extended[layoutKind]) == "object" && len(e.Extended[layoutMark]) > 0 {
		if err := f.hook(ctx, e.Name); err != nil {
			return err
		}
	}
	return f.MemFiler.UpdateEntry(ctx, dir, e, expected)
}

// headNamesMissing reports a name some root in HEAD (current or snapshot)
// names that is absent from the pool: a root names a removed chunk.
func headNamesMissing(ctx context.Context, c *BCatalog, v VolumeRef) error {
	s, r, err := c.open(v)
	if err != nil {
		return err
	}
	h, found, err := s.ReadHead(ctx)
	if err != nil || !found {
		return fmt.Errorf("HEAD: found %v, %v", found, err)
	}
	var missing error
	for _, rn := range layout.HeadRoots(h) {
		for shard := uint64(0); shard < 2; shard++ {
			if err := r.EachRef(ctx, rn, shard, func(_ uint64, ref layout.Ref) bool {
				if _, ok, err := s.Stat(ctx, ref.Chunk.Path()); err != nil || !ok {
					missing = fmt.Errorf("root %v names %s, absent from the pool (%v)", rn, ref.Chunk, err)
					return false
				}
				return true
			}); err != nil {
				return err
			}
		}
	}
	return missing
}

// The operator deletes a snapshot while the holder's cleaning is open, after
// the cleaning took its HEAD view (model SnapshotRemoval.DeleteSnap during a
// cleaning; its safety property: every chunk a root names exists). The
// delete is one HEAD update by nobody's lease and lands in the clean
// activity without ending it. The cleaning
// keeps every chunk its view named (the deleted snapshot's chunk survives
// this pass), never marks a chunk a root still names, and ends normally;
// the next cleaning removes the chunk only the deleted snapshot named.
// Must violate: a cleaning that counts only the current root (the model's
// must-violate variant) marks the snapshot's chunk while the snapshot still
// names it.
func TestBCatalog_OperatorSnapshotDeleteDuringACleaning(t *testing.T) {
	ctx := context.Background()
	f := &markHookFiler{MemFiler: NewMemFiler()}
	c, err := NewBCatalog(f, newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v1")
	g := layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}
	if _, err := c.CreateVolume(ctx, v, g); err != nil {
		t.Fatal(err)
	}
	s, err := c.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{Geometry: g, ClaimID: "k", Fresh: true, CanStartFrom: anyStart}); err != nil {
		t.Fatal(err)
	}
	// s1 names X at index 1; the current root (and s2) moved on to Y.
	x, y := bytes.Repeat([]byte{'X'}, 4096), bytes.Repeat([]byte{'Y'}, 4096)
	cx, cy := layout.NameChunk(1, x), layout.NameChunk(1, y)
	m1 := map[uint64]layout.Ref{1: {Owner: v.Name, OwnerID: v.ID, Chunk: cx}}
	m2 := map[uint64]layout.Ref{1: {Owner: v.Name, OwnerID: v.ID, Chunk: cy}}
	commitImage(t, s, v.Name, "w1", map[uint64]layout.Ref{}, m1, map[uint64][]byte{1: x}, 1, "s1")
	commitImage(t, s, v.Name, "w2", m1, m2, map[uint64][]byte{1: y}, 2, "s2")
	// Z at index 2: an orphan no root names, so the pass marks something.
	z := bytes.Repeat([]byte{'Z'}, 4096)
	cz := layout.NameChunk(2, z)
	if err := layout.EnsureObject(ctx, s, cz.Path(), z, "dead-session"); err != nil {
		t.Fatal(err)
	}
	if err := f.MemFiler.CreateEntry(ctx, path.Join(c.base, v.Dir(), layout.PoolDir), &filerpb.Entry{Name: "0", IsDirectory: true}, false); err != nil {
		t.Fatal(err)
	}

	// At the pass's first MARK (its HEAD view is taken): check that no root
	// names the name being marked, then the operator deletes s1.
	deleted := false
	var hookErr error
	f.hook = func(ctx context.Context, name string) error {
		h, _, err := c.ReadHead(ctx, v)
		if err != nil {
			hookErr = err
			return err
		}
		for _, rn := range layout.HeadRoots(h) {
			_, r, _ := c.open(v)
			_ = r.EachRef(ctx, rn, 0, func(_ uint64, ref layout.Ref) bool {
				if ref.Chunk.Path() != "" && path.Base(ref.Chunk.Path()) == name && hookErr == nil {
					hookErr = fmt.Errorf("the cleaning marks %s while root %v still names it", name, rn)
				}
				return true
			})
		}
		if deleted {
			return nil
		}
		deleted = true
		session := h.Activity.Session
		if h.Activity.Kind != layout.ActClean {
			hookErr = fmt.Errorf("HEAD at the first mark: %+v; want the cleaning open", h.Activity)
			return nil
		}
		after, err := c.DeleteSnapshot(ctx, v, "s1")
		if err != nil {
			hookErr = fmt.Errorf("the operator's delete during a cleaning: %w", err)
			return nil
		}
		if after.Activity != (layout.Activity{Kind: layout.ActClean, Session: session}) {
			hookErr = fmt.Errorf("HEAD after the operator's delete: %+v; want the cleaning still open", after.Activity)
		}
		return nil
	}
	res, _, err := cleanVia(ctx, c, v, cleanHolder)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err != nil || !deleted {
		t.Fatalf("the pass with the delete in it: %+v %v (delete ran: %v)", res, err, deleted)
	}
	if !present(t, s, cx) || !present(t, s, cy) || present(t, s, cz) {
		t.Fatalf("after the pass: X %v, Y %v, Z %v; want X kept (its view named it), Y kept, Z removed",
			present(t, s, cx), present(t, s, cy), present(t, s, cz))
	}
	h, _, err := c.ReadHead(ctx, v)
	if _, s1 := h.Snapshots["s1"]; err != nil || h.Activity.Kind != layout.ActIdle || s1 {
		t.Fatalf("HEAD after the pass: %+v %v; want idle and s1 gone", h.Activity, err)
	}
	if err := headNamesMissing(ctx, c, v); err != nil {
		t.Fatal(err)
	}
	// Convergence: the next cleaning removes the chunk only s1 named.
	f.hook = nil
	res, _, err = cleanVia(ctx, c, v, cleanHolder)
	if err != nil || res.Removed != 1 || present(t, s, cx) || !present(t, s, cy) {
		t.Fatalf("the next pass: %+v %v, X %v, Y %v; want X removed, Y kept", res, err, present(t, s, cx), present(t, s, cy))
	}
	if err := headNamesMissing(ctx, c, v); err != nil {
		t.Fatal(err)
	}
}
