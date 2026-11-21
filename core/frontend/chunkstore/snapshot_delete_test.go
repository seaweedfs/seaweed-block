package chunkstore

import (
	"bytes"
	"context"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// INV-SNAP-DELETE-ANY-ORDER-001: forks and restores do not hold their parent.

// A fork stopped past its deadline does not keep its parent: the snapshot is
// deleted, and a retry of the same request reads the parent again and is
// refused; it never publishes from its first read.
func TestSnapshotDelete_ForkRetryAfterDeleteRereadsTheParent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}
	clock := time.Now()
	c.SetClock(func() time.Time { clock = clock.Add(time.Second); return clock }) // every read advances
	c.OpDeadline = time.Nanosecond
	if _, err := c.Fork(ctx, req); CodeOf(err) != CodeDeadlineExceeded {
		t.Fatalf("fork past its deadline: %v, want deadline_exceeded", err)
	}
	c.OpDeadline = 0
	if _, ok, err := c.ReadVolume(ctx, "v2"); !ok || err != nil {
		t.Fatalf("the stopped fork's VOLUME: ok=%v %v", ok, err)
	}
	if got, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || got != SnapDeleting {
		t.Fatalf("delete the parent of an incomplete fork: %q %v, want deleting", got, err)
	}
	_, err := c.Fork(ctx, req)
	if code := CodeOf(err); code != CodeSnapshotNotFound && code != CodeSnapshotDeleting {
		t.Fatalf("retry after the parent was deleted: %v, want snapshot_not_found or snapshot_deleting", err)
	}
	if _, err := f.LookupEntry(ctx, catBase+"/v2", headName); err == nil {
		t.Fatal("the retry published a HEAD from its first read of a deleted parent")
	}
}

// A delete that lands between a fork's read of its parent and its HEAD
// publish succeeds; the fork still publishes inside its deadline, and the
// target keeps every chunk after the source snapshot and volume are gone and
// GC has run past its grace.
func TestSnapshotDelete_DeleteDuringAForkInFlight(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	want := map[uint64][]byte{0: chunkBytes(0x41), 2: chunkBytes(0x42)}
	h := publishSource(t, f, b.fakeBlobs, "v1", want)
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	deleted := false
	f.stallCreate = func(p string) bool { // runs before the fake takes its lock
		if p == catBase+"/v2/"+headName && !deleted {
			deleted = true
			if got, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || got != SnapDeleting {
				t.Errorf("delete while the fork is in flight: %q %v", got, err)
			}
		}
		return false
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); err != nil {
		t.Fatalf("fork whose parent was deleted after its read: %v", err)
	}
	f.stallCreate = nil
	if !deleted {
		t.Fatal("the delete never ran inside the fork")
	}
	detachHead(t, f, b.fakeBlobs, "v1")
	if _, err := c.DeleteVolume(ctx, "v1", nil); err != nil {
		t.Fatalf("delete the source volume: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := g.Run(ctx); err != nil {
			t.Fatalf("gc: %v", err)
		}
		*now = now.Add(2 * time.Hour)
	}
	r2, _ := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	h2, err := r2.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for idx, ref := range h2.Chunks {
		if got, err := r2.Get(ctx, ref.Key); err != nil || !bytes.Equal(got, want[idx]) {
			t.Fatalf("fork chunk %d lost: %v", idx, err)
		}
	}
}

// An unknown publish outcome keeps the fork's intent, and that intent roots
// the parent's keys with no time bound: the snapshot deleted, the source
// volume deleted, GC run well past its grace, the keys survive while the
// intent exists.
func TestSnapshotDelete_UnknownPublishKeepsTheKeysWhileItsIntentExists(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	want := map[uint64][]byte{0: chunkBytes(0x51), 1: chunkBytes(0x52)}
	h := publishSource(t, f, b.fakeBlobs, "v1", want)
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	f.failCreate = func(p string) error {
		if p == catBase+"/v2/"+headName {
			return errTransport
		}
		return nil
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork with an unknown HEAD create: %v", err)
	}
	f.failCreate = nil
	if got, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || got != SnapDeleting {
		t.Fatalf("delete the parent: %q %v", got, err)
	}
	detachHead(t, f, b.fakeBlobs, "v1")
	if _, err := c.DeleteVolume(ctx, "v1", nil); err != nil {
		t.Fatalf("delete the source volume: %v", err)
	}
	intents := func() int {
		list, err := f.ListEntries(ctx, path.Join(catBase, "v2", intentsDir), "", "", 0)
		if err != nil {
			t.Fatal(err)
		}
		return len(list)
	}
	if intents() == 0 {
		t.Fatal("the unknown publish left no intent")
	}
	for i := 0; i < 4; i++ {
		if _, err := g.Run(ctx); err != nil {
			t.Fatalf("gc: %v", err)
		}
		*now = now.Add(2 * time.Hour) // far past the grace
	}
	if intents() == 0 {
		t.Fatal("GC settled the fork intent without the target being resolved")
	}
	r1, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	for idx, ref := range h.Chunks {
		if got, err := r1.Get(ctx, ref.Key); err != nil || !bytes.Equal(got, want[idx]) {
			t.Fatalf("parent chunk %d reclaimed while the fork intent exists: %v", idx, err)
		}
	}
}
