package chunkstore

import (
	"context"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// TestCatalogReplayOfASnapshotBeingDeletedIsNotSuccess: s1 is created by
// request r1, then deleted by a process that dies after marking it deleting
// and before freeing its name (the name record still names r1's attempt).
// A retry of r1, through a restarted catalog, must not answer success for
// the snapshot being deleted, neither from the replay check nor from the
// create: both refuse it as snapshot_deleting, and the snapshot stays
// deleting and unlisted.
func TestCatalogReplayOfASnapshotBeingDeletedIsNotSuccess(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	req := SnapshotRequest{Name: "s1", RequestID: "r1"}
	if _, err := c.CreateSnapshot(ctx, "v1", req, h); err != nil {
		t.Fatal(err)
	}

	nameRecord := path.Join(catBase, "v1", snapNamesDir, "s1")
	crashed := &crashFiler{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}},
		match: func(op, p string) bool { return op == "delete" && p == nameRecord }}
	dying, err := NewCatalog(crashed, catBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dying.DeleteSnapshot(ctx, "v1", "s1"); err == nil {
		t.Fatal("the delete returned success although its process died before freeing the name")
	}
	if _, err := f.LookupEntry(ctx, path.Dir(nameRecord), path.Base(nameRecord)); err != nil {
		t.Fatalf("the name record should still exist after the crash: %v", err)
	}

	r, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase) // the restarted process
	if err != nil {
		t.Fatal(err)
	}
	if info, replayed, err := r.SnapshotReplay(ctx, "v1", req); replayed || CodeOf(err) != CodeSnapshotDeleting {
		t.Errorf("the replay check of a snapshot being deleted: %+v replayed %v, %v; want snapshot_deleting", info, replayed, err)
	}
	if info, err := r.CreateSnapshot(ctx, "v1", req, h); CodeOf(err) != CodeSnapshotDeleting {
		t.Errorf("a retry of the create of a snapshot being deleted: %+v, %v; want snapshot_deleting", info, err)
	}
	if l, _, err := r.ListSnapshots(ctx, "v1", ListOptions{}); err != nil || len(l) != 0 {
		t.Fatalf("the snapshot being deleted is listed: %+v %v", l, err)
	}
	if l, _, err := r.ListSnapshots(ctx, "v1", ListOptions{IncludeDeleting: true}); err != nil || len(l) != 1 || l[0].State != SnapDeleting {
		t.Fatalf("the snapshot must stay deleting: %+v %v", l, err)
	}
}

// peerPublishFiler models a peer (an earlier attempt of the same request)
// that, just before this create writes the attempt entry, publishes that
// same attempt and has it deleted: the create's initial replay check saw the
// attempt unpublished, and its exclusive create then finds the peer's entry,
// now deleting.
type peerPublishFiler struct {
	manifestFilerBlobs
	attemptDir string
	peer       func(ctx context.Context, dir string, e *filerpb.Entry)
	done       bool
}

func (p *peerPublishFiler) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	if !p.done && dir == p.attemptDir {
		p.done = true
		p.peer(ctx, dir, e)
	}
	return p.manifestFilerBlobs.CreateEntry(ctx, dir, e, oExcl)
}

// TestCatalogCreateFindingItsAttemptDeletingIsNotSuccess: r1's first attempt
// at s1 died after its name record. A retry of r1 passes the replay check
// (the attempt is unpublished) and, as it writes the attempt entry, a peer
// with the same request publishes that attempt and deletes it. The retry's
// exclusive create finds the peer's entry, now deleting: it must refuse it
// as snapshot_deleting, not answer success.
func TestCatalogCreateFindingItsAttemptDeletingIsNotSuccess(t *testing.T) {
	f, b, _, h := catalogFixture(t)
	ctx := context.Background()
	req := SnapshotRequest{Name: "s1", RequestID: "r1"}
	nameRecord := path.Join(catBase, "v1", snapNamesDir, "s1")
	crashed := &crashFiler{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}}, after: true,
		match: func(op, p string) bool { return op == "create" && p == nameRecord }}
	dying, err := NewCatalog(crashed, catBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dying.CreateSnapshot(ctx, "v1", req, h); err == nil {
		t.Fatal("the first attempt returned success although its process died after the name record")
	}

	inner := manifestFilerBlobs{f, gcBlobs{b}}
	peer, err := NewCatalog(inner, catBase)
	if err != nil {
		t.Fatal(err)
	}
	pf := &peerPublishFiler{manifestFilerBlobs: inner, attemptDir: path.Join(catBase, "v1", snapsDir, "s1")}
	pf.peer = func(ctx context.Context, dir string, e *filerpb.Entry) {
		if err := inner.CreateEntry(ctx, dir, e, true); err != nil {
			t.Errorf("the peer's publish of the same attempt: %v", err)
			return
		}
		if st, err := peer.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != SnapDeleting {
			t.Errorf("the peer's delete: %q %v", st, err)
		}
	}
	r, err := NewCatalog(pf, catBase)
	if err != nil {
		t.Fatal(err)
	}
	if info, replayed, err := r.SnapshotReplay(ctx, "v1", req); err != nil || replayed {
		t.Fatalf("before the retry the replay check must see the attempt unpublished: %+v %v %v", info, replayed, err)
	}
	info, err := r.CreateSnapshot(ctx, "v1", req, h)
	if !pf.done {
		t.Fatal("the retry never reached the exclusive create of its attempt")
	}
	if CodeOf(err) != CodeSnapshotDeleting {
		t.Fatalf("the retry found its attempt deleting: %+v, %v; want snapshot_deleting", info, err)
	}
}
