package chunkstore

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// errCrashed is what a filer call returns once the process that issued it
// is dead.
var errCrashed = errors.New("crashed")

// crashFiler models a blockvolume killed in the middle of a catalog
// operation: at the chosen filer call it dies, before or after that call
// reaches the filer, and from then on no call of it reaches the filer, so
// none of its cleanup runs. Blob calls are not intercepted (snapshot create
// writes entries only).
type crashFiler struct {
	manifestFilerBlobs
	mu    sync.Mutex
	dead  bool
	match func(op, p string) bool // the call at which the process dies
	after bool                    // the call reaches the filer, its reply is lost
}

func (c *crashFiler) gate(op, p string) (pass, die bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead {
		return false, false
	}
	if c.match(op, p) {
		c.dead = true
		return c.after, true
	}
	return true, false
}

func (c *crashFiler) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if pass, _ := c.gate("lookup", path.Join(dir, name)); !pass {
		return nil, errCrashed
	}
	return c.manifestFilerBlobs.LookupEntry(ctx, dir, name)
}

func (c *crashFiler) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	pass, die := c.gate("create", path.Join(dir, e.GetName()))
	if !pass {
		return errCrashed
	}
	err := c.manifestFilerBlobs.CreateEntry(ctx, dir, e, oExcl)
	if die {
		return errCrashed
	}
	return err
}

func (c *crashFiler) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	pass, die := c.gate("update", path.Join(dir, e.GetName()))
	if !pass {
		return errCrashed
	}
	err := c.manifestFilerBlobs.UpdateEntry(ctx, dir, e, expected)
	if die {
		return errCrashed
	}
	return err
}

func (c *crashFiler) ListEntries(ctx context.Context, dir, prefix, startAfter string, limit uint32) ([]*filerpb.Entry, error) {
	if pass, _ := c.gate("list", dir); !pass {
		return nil, errCrashed
	}
	return c.manifestFilerBlobs.ListEntries(ctx, dir, prefix, startAfter, limit)
}

func (c *crashFiler) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	pass, die := c.gate("delete", path.Join(dir, name))
	if !pass {
		return errCrashed
	}
	err := c.manifestFilerBlobs.DeleteEntry(ctx, dir, name, expected)
	if die {
		return errCrashed
	}
	return err
}

// TestCatalogRestartAfterAbandonedSnapshot: a snapshot create of s1 whose
// process dies at each of its filer steps (after the name record, after the
// intent, with the attempt create lost before or after it applied, after the
// create before the intent is dropped), then a catalog restarted over the
// same filer. Whatever the step:
//   - the create never answers success unless its attempt entry was
//     written;
//   - the list shows s1 only if its attempt entry was written, and then as
//     the snapshot of the cut HEAD, never a partial one;
//   - a retry with the same request id completes s1, listed once;
//   - a new snapshot of another name is taken and listed;
//   - when s1 is abandoned unpublished, its name record still holds the name
//     (another request gets snapshot_exists) and blocks volume delete
//     (volume_has_snapshots), and deleting s1 settles it: the name is free
//     and the volume delete completes.
func TestCatalogRestartAfterAbandonedSnapshot(t *testing.T) {
	isName := func(p string) bool { return p == path.Join(catBase, "v1", snapNamesDir, "s1") }
	isIntent := func(p string) bool { return strings.HasPrefix(p, path.Join(catBase, "v1", intentsDir)+"/") }
	isAttempt := func(p string) bool { return isSnapPath(p, "v1", "s1") }
	for _, tc := range []struct {
		name      string
		op        string
		at        func(string) bool
		after     bool
		published bool // the attempt entry was written before the crash
	}{
		{"after the name record", "create", isName, true, false},
		{"after the intent", "create", isIntent, true, false},
		{"attempt create lost before it applied", "create", isAttempt, false, false},
		{"attempt create applied, reply lost", "create", isAttempt, true, true},
		{"before the intent is dropped", "delete", isIntent, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b, _, h := deleteFixture(t)
			ctx := context.Background()
			detachHead(t, f, b, "v1")
			crashed := &crashFiler{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}}, after: tc.after,
				match: func(op, p string) bool { return op == tc.op && tc.at(p) }}
			dying, err := NewCatalog(crashed, catBase)
			if err != nil {
				t.Fatal(err)
			}
			// A crash before the attempt entry is written cannot return success;
			// once it is written the create has succeeded (the intent drop
			// after it is best effort), whatever the caller then hears.
			if _, err := dying.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err == nil && !tc.published {
				t.Fatal("the create returned success although its process died before publishing")
			}
			crashed.mu.Lock()
			died := crashed.dead
			crashed.mu.Unlock()
			if !died {
				t.Fatalf("the create never reached the step %q", tc.name)
			}

			c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase) // the restarted process
			if err != nil {
				t.Fatal(err)
			}
			list := func() []SnapshotInfo {
				t.Helper()
				l, _, err := c.ListSnapshots(ctx, "v1", ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return l
			}
			switch l := list(); {
			case !tc.published && len(l) != 0:
				t.Fatalf("an unpublished snapshot is listed after the restart: %+v", l)
			case tc.published && (len(l) != 1 || l[0].Name != "s1" || l[0].HeadGen != h.Gen || l[0].State != SnapActive):
				t.Fatalf("after the restart the list is %+v; want s1 of HEAD gen %d, active", l, h.Gen)
			}

			if !tc.published {
				if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "other"}, h); CodeOf(err) != CodeSnapshotExists {
					t.Fatalf("another request for the abandoned name: %v; want snapshot_exists (the name record holds it)", err)
				}
				if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeVolumeHasSnapshots {
					t.Fatalf("volume delete with the abandoned name record: %v; want volume_has_snapshots", err)
				}
			}

			info, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h)
			if err != nil || info.Name != "s1" || info.HeadGen != h.Gen {
				t.Fatalf("retry with the same request id after the restart: %+v %v", info, err)
			}
			if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s2", RequestID: "r2"}, h); err != nil {
				t.Fatalf("the next snapshot after the restart: %v", err)
			}
			if l := list(); len(l) != 2 || l[0].Name != "s1" || l[1].Name != "s2" {
				t.Fatalf("after the retry and the next snapshot the list is %+v; want s1, s2 once each", l)
			}

			for _, name := range []string{"s1", "s2"} {
				if _, err := c.DeleteSnapshot(ctx, "v1", name); err != nil {
					t.Fatalf("delete %s: %v", name, err)
				}
			}
			if l := list(); len(l) != 0 {
				t.Fatalf("after deleting both the list is %+v", l)
			}
			if res, err := c.DeleteVolume(ctx, "v1", nil); err != nil || !res.Completed {
				t.Fatalf("volume delete after the snapshots are deleted: %+v %v", res, err)
			}
		})
	}
}

// TestCatalogAbandonedUnpublishedSnapshotIsSettledByDelete: an abandoned,
// never-published s1 (the process died after its name record) is settled by
// deleting it without a retry: the name is then free for another request and
// the volume delete is no longer blocked by it.
func TestCatalogAbandonedUnpublishedSnapshotIsSettledByDelete(t *testing.T) {
	f, b, _, h := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	crashed := &crashFiler{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}}, after: true,
		match: func(op, p string) bool { return op == "create" && p == path.Join(catBase, "v1", snapNamesDir, "s1") }}
	dying, err := NewCatalog(crashed, catBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dying.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err == nil {
		t.Fatal("the create returned success although its process died")
	}
	c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != SnapDeleting {
		t.Fatalf("delete of the abandoned s1: %q %v", st, err)
	}
	if l, _, err := c.ListSnapshots(ctx, "v1", ListOptions{}); err != nil || len(l) != 0 {
		t.Fatalf("list after settling: %+v %v", l, err)
	}
	if res, err := c.DeleteVolume(ctx, "v1", nil); err != nil || !res.Completed {
		t.Fatalf("volume delete after settling the abandoned name: %+v %v", res, err)
	}
}
