package chunkstore

import (
	"context"
	"path"
	"testing"
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
