package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
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
	c.SetClock(func() time.Time { return clock })
	f.beforeCreate = func(p string) {
		if p == catBase+"/v2/VOLUME" {
			clock = clock.Add(DefaultOpDeadline)
		}
	}
	if _, err := c.Fork(ctx, req); CodeOf(err) != CodeDeadlineExceeded {
		t.Fatalf("fork past its deadline: %v, want deadline_exceeded", err)
	}
	f.beforeCreate = nil
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
	requireChunks(t, ctx, r2, h2, want, "fork")
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
		// The fork's HEAD is absent (its publish is unknown): GC acts on
		// nothing while it is (INV-GC-HEAD-ABSENT-001).
		if res, err := g.Run(ctx); err == nil || len(res.RefusedVolumes) != 1 || res.Reclaimed != 0 {
			t.Fatalf("gc with the fork's HEAD absent: %+v %v; want the pass refused", res, err)
		}
		*now = now.Add(2 * time.Hour) // far past the grace
	}
	if intents() == 0 {
		t.Fatal("GC settled the fork intent without the target being resolved")
	}
	r1, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	requireChunks(t, ctx, r1, h, want, "parent (fork intent still present)")
}

// requireChunks fails unless h names exactly the indexes of want and each one
// reads back want's bytes (a missing index cannot pass by iterating h alone).
func requireChunks(t *testing.T, ctx context.Context, r *ChunkRemote, h storage.ChunkHead, want map[uint64][]byte, what string) {
	t.Helper()
	if err := chunksMismatch(ctx, r, h, want); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func chunksMismatch(ctx context.Context, r *ChunkRemote, h storage.ChunkHead, want map[uint64][]byte) error {
	if len(h.Chunks) != len(want) {
		return fmt.Errorf("HEAD names %d chunks, want %d", len(h.Chunks), len(want))
	}
	for idx, data := range want {
		ref, ok := h.Chunks[idx]
		if !ok {
			return fmt.Errorf("HEAD lost chunk %d", idx)
		}
		if got, err := r.Get(ctx, ref.Key); err != nil || !bytes.Equal(got, data) {
			return fmt.Errorf("chunk %d unreadable or changed: %v", idx, err)
		}
	}
	return nil
}

// The check refuses a HEAD that lost an index, even when every chunk it
// still names reads back correctly.
func TestSnapshotDelete_ChunkCheckCatchesAMissingIndex(t *testing.T) {
	f, b, _, h := catalogFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	want := map[uint64][]byte{}
	missing := uint64(0)
	for idx, ref := range h.Chunks {
		data, err := r.Get(ctx, ref.Key)
		if err != nil {
			t.Fatal(err)
		}
		want[idx] = data
		if idx >= missing {
			missing = idx + 1
		}
	}
	if err := chunksMismatch(ctx, r, h, want); err != nil {
		t.Fatalf("the matching HEAD failed the check: %v", err)
	}
	want[missing] = chunkBytes(0x62) // an index the HEAD does not name
	if err := chunksMismatch(ctx, r, h, want); err == nil {
		t.Fatal("a HEAD missing an expected chunk passed the check")
	}
}
