package chunkstore

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// Red tests for the 1.0.1 snapshot-name rules F2-F4 (each is a property of
// the snapshot-list model that today's catalog violates, documented for 1.0).
// They fail on purpose until the catalog is changed.

// interposeFiler runs once before the first filer call that match selects,
// then forwards every call to the fake filer.
type interposeFiler struct {
	*fakeFiler
	match func(op, dir, name string) bool
	once  func()
	done  bool
}

func (f *interposeFiler) fire(op, dir, name string) {
	if !f.done && f.match(op, dir, name) {
		f.done = true
		f.once()
	}
}

func (f *interposeFiler) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	f.fire("create", dir, e.GetName())
	return f.fakeFiler.CreateEntry(ctx, dir, e, oExcl)
}

func (f *interposeFiler) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	f.fire("lookup", dir, name)
	return f.fakeFiler.LookupEntry(ctx, dir, name)
}

// TestRedF2SweepDoesNotAbortALiveRetry (model property W7): run 1 of request
// r1 leaves its attempt unsettled (its create's outcome is unknown, its
// intent kept); after run 1's deadline the client retries r1. Run 2 reuses
// run 1's attempt path, and a collector sweep of run 1's expired intent runs
// just before run 2 creates the attempt entry, well inside run 2's own
// deadline. Run 2 must publish its snapshot; today the sweep's tombstone
// aborts it (snapshot_exists: "this request's attempt was aborted").
func TestRedF2SweepDoesNotAbortALiveRetry(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	c.SetClock(func() time.Time { return *now })
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x41)})
	snapshotDir(t, f, "v1", "s1")
	holdFirst(f, path.Join(catBase, "v1", snapsDir, "s1")+"/")
	req := SnapshotRequest{Name: "s1", RequestID: "r1"}
	if _, err := c.CreateSnapshot(ctx, "v1", req, h); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("run 1: %v; want outcome_unknown (its create held)", err)
	}
	*now = now.Add(DefaultOpDeadline + time.Second) // run 1's deadline has passed

	attempts := path.Join(catBase, "v1", snapsDir, "s1")
	ip := &interposeFiler{fakeFiler: f,
		match: func(op, dir, _ string) bool { return op == "create" && dir == attempts },
		once: func() {
			if n, err := g.SweepCopies(ctx); err != nil {
				t.Errorf("sweep: %d %v", n, err)
			}
		}}
	c2, err := NewCatalog(ip, catBase)
	if err != nil {
		t.Fatal(err)
	}
	c2.SetClock(func() time.Time { return *now })
	info, err := c2.CreateSnapshot(ctx, "v1", req, h)
	if !ip.done {
		t.Fatal("run 2 never created its attempt entry")
	}
	if err != nil {
		t.Fatalf("F2: a retry inside its own deadline was refused after a sweep of the earlier run's expired intent: %v", err)
	}
	if info.Name != "s1" || info.State != SnapActive {
		t.Fatalf("F2: run 2 answered %+v; want s1 active", info)
	}
}

// TestRedF3RetryAfterDeleteIsNotANewSnapshot (model property W8): r1
// publishes s1; s1 is deleted (marked deleting, its name freed); the client
// then retries r1 (for example after losing the first answer). Every success
// of one request id must name the same snapshot attempt; today the retry
// cuts again and publishes a new snapshot under the same request id.
func TestRedF3RetryAfterDeleteIsNotANewSnapshot(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	req := SnapshotRequest{Name: "s1", RequestID: "r1"}
	first, err := c.CreateSnapshot(ctx, "v1", req, h)
	if err != nil {
		t.Fatal(err)
	}
	firstPath := snapPath(t, f, "v1", "s1")
	if st, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != SnapDeleting {
		t.Fatalf("delete: %q %v", st, err)
	}
	again, err := c.CreateSnapshot(ctx, "v1", req, h)
	if err == nil {
		againPath := snapPath(t, f, "v1", "s1")
		if againPath != firstPath {
			t.Fatalf("F3: the retry of r1 after its snapshot was deleted published a new snapshot (%s, was %s; first answer %+v, retry %+v)",
				path.Base(againPath), path.Base(firstPath), first, again)
		}
	}
}

// TestRedF4DeletingListingShowsOnlyNamedAttempts (model property W9): an
// IncludeDeleting listing reads s1's name record, then DeleteSnapshot marks
// s1 deleting and frees the name, then the listing reads s1's attempt entry.
// The listing must not show an attempt whose name it no longer holds (the
// name may by then hold another snapshot); today it shows the deleting s1.
func TestRedF4DeletingListingShowsOnlyNamedAttempts(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err != nil {
		t.Fatal(err)
	}
	attempts := path.Join(catBase, "v1", snapsDir, "s1")
	ip := &interposeFiler{fakeFiler: f,
		match: func(op, dir, _ string) bool { return op == "lookup" && dir == attempts },
		once: func() {
			if st, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != SnapDeleting {
				t.Errorf("the delete between the listing's two reads: %q %v", st, err)
			}
		}}
	lister, err := NewCatalog(manifestFilerBlobs{ip, gcBlobs{b}}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	list, _, err := lister.ListSnapshots(ctx, "v1", ListOptions{IncludeDeleting: true})
	if err != nil {
		t.Fatal(err)
	}
	if !ip.done {
		t.Fatal("the listing never read s1's attempt entry")
	}
	var names []string
	for _, s := range list {
		names = append(names, s.Name+"/"+s.State)
	}
	if len(list) != 0 {
		t.Fatalf("F4: the listing shows %s, whose name was freed between its two reads", strings.Join(names, ", "))
	}
}
