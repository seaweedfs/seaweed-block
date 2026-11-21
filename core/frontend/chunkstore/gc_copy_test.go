package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

func snapshotDir(t *testing.T, f *fakeFiler, vol, name string) {
	t.Helper()
	if e := f.CreateEntry(context.Background(), path.Join(catBase, vol, snapsDir), &filerpb.Entry{Name: name, IsDirectory: true}, true); e != nil {
		t.Fatal(e)
	}
}

func TestGC_CopySweepClaimsSnapshotAttemptBeforeRelease(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	c.SetClock(func() time.Time { return *now })
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x31)})
	snapshotDir(t, f, "v1", "s1")
	holdFirst(f, path.Join(catBase, "v1", snapsDir, "s1")+"/")
	if _, e := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "dead"}, h); CodeOf(e) != CodeOutcomeUnknown {
		t.Fatalf("held snapshot: %v", e)
	}
	oldPath := snapPath(t, f, "v1", "s1")
	if count, e := g.SweepCopies(ctx); e != nil || count != 0 {
		t.Fatalf("early sweep: %d %v", count, e)
	}
	*now = now.Add(DefaultOpDeadline + time.Second)
	if count, e := g.SweepCopies(ctx); e != nil || count != 1 {
		t.Fatalf("dead copy intent not settled by tombstone claim: count=%d err=%v", count, e)
	}
	if ins := intentsOf(t, f, "v1"); len(ins) != 0 {
		t.Fatalf("intent retained after target claimed: %+v", ins)
	}
	e, err := f.LookupEntry(ctx, path.Dir(oldPath), path.Base(oldPath))
	if err != nil || string(e.Extended[extSnapState]) != SnapAborted {
		t.Fatalf("permanent claim missing: %v", err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "new"}, h); err != nil {
		t.Fatal(err)
	}
	if p := snapPath(t, f, "v1", "s1"); p == oldPath {
		t.Fatal("reused permanent attempt path")
	}
	if applied := f.releaseDeferredCreates(); applied != 0 {
		t.Fatalf("late old snapshot insert landed after name reuse: %d", applied)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "newfork", RequestID: "fork"}); err != nil {
		t.Fatal(err)
	}
	// The new name resolves to its new published attempt, never old tombstone.
	info, err := c.readSnap(ctx, "v1", "s1")
	if err != nil || info.Head().Gen != h.Gen {
		t.Fatalf("new snapshot: %+v %v", info, err)
	}
}

func TestGC_CopySweepClaimsForkHEADNotExistingVolume(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	c.SetClock(func() time.Time { return *now })
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x41)})
	snapshotDir(t, f, "v1", "s1")
	if _, e := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "s"}, h); e != nil {
		t.Fatal(e)
	}
	if e := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); e != nil {
		t.Fatal(e)
	}
	holdFirst(f, path.Join(catBase, "v2", headName))
	if _, e := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "dead"}); CodeOf(e) != CodeOutcomeUnknown {
		t.Fatalf("held fork: %v", e)
	}
	if _, ok, e := c.ReadVolume(ctx, "v2"); e != nil || !ok {
		t.Fatalf("fixture lacks existing VOLUME: %v", e)
	}
	if _, ok, e := c.ReadHead(ctx, "v2"); e != nil || ok {
		t.Fatalf("held fork already published: %v %v", ok, e)
	}
	*now = now.Add(DefaultOpDeadline + time.Second)
	count, e := g.SweepCopies(ctx)
	if e != nil || count != 1 {
		t.Fatalf("fork sweep: %d %v", count, e)
	}
	head, ok, e := c.ReadHead(ctx, "v2")
	if e != nil || !ok || head.State != storage.ChunkHeadAborted {
		t.Fatalf("final HEAD not claimed: %+v %v", head, e)
	}
	if n := f.releaseDeferredCreates(); n != 0 {
		t.Fatalf("late fork HEAD published over claim: %d", n)
	}
	if _, e := c.EnsureVolume(ctx, "v2", catGeo); CodeOf(e) != CodeForkIncomplete {
		t.Fatalf("aborted target attached: %v", e)
	}
	if _, e := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "new"}); CodeOf(e) != CodeTargetExists {
		t.Fatalf("volume id reused: %v", e)
	}
}

type retainCopyIntent struct{ Filer }

func (f retainCopyIntent) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	if path.Base(dir) == intentsDir {
		return errors.New("cleanup response unavailable")
	}
	return f.Filer.DeleteEntry(ctx, dir, name, expected)
}

func TestGC_PublishedUnnamedAttemptRootsUntilDeletingGrace(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	c.SetClock(func() time.Time { return *now })
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x51)})
	snapshotDir(t, f, "v1", "s1")
	c.f = retainCopyIntent{f}
	if _, e := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "published"}, h); e != nil {
		t.Fatal(e)
	}
	c.f = f
	oldPath := snapPath(t, f, "v1", "s1")
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if _, e := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: h.RemoteLSN}); e != nil {
		t.Fatal(e)
	}
	*now = now.Add(DefaultOpDeadline + time.Second)
	if count, e := g.SweepCopies(ctx); e != nil || count != 1 {
		t.Fatalf("published copy claim: %d %v", count, e)
	}
	e, err := f.LookupEntry(ctx, path.Dir(oldPath), path.Base(oldPath))
	if err != nil || string(e.Extended[extSnapState]) != SnapActive {
		t.Fatalf("published snapshot was overwritten: %v", err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	// The name is gone, but its immutable attempt remains a root throughout grace.
	if list, _, e := c.ListSnapshots(ctx, "v1", ListOptions{IncludeDeleting: true}); e != nil || len(list) != 0 {
		t.Fatalf("name still listed: %+v %v", list, e)
	}
	for i := 0; i < 2; i++ {
		if result, e := g.Run(ctx); e != nil || result.Reclaimed != 0 {
			t.Fatalf("unnamed deleting attempt lost grace: %+v %v", result, e)
		}
		*now = now.Add(time.Minute)
	}
	for _, ref := range h.Chunks {
		if got, e := r.Get(ctx, ref.Key); e != nil || !bytes.Equal(got, chunkBytes(0x51)) {
			t.Fatalf("snapshot bytes lost: %v", e)
		}
	}
	*now = now.Add(2 * time.Hour)
	if result, e := g.Run(ctx); e != nil || result.Reclaimed != 0 {
		t.Fatalf("first unrooted mark: %+v %v", result, e)
	}
	*now = now.Add(time.Hour + g.cfg.ClockSkew + time.Nanosecond)
	if result, e := g.Run(ctx); e != nil || result.Reclaimed != 2 {
		t.Fatalf("deleted attempt retained chunks forever: %+v %v", result, e)
	}
	if _, e := f.LookupEntry(ctx, path.Dir(oldPath), path.Base(oldPath)); e != nil {
		t.Fatal("permanent attempt entry removed", e)
	}
}

func TestGC_CopyClaimUnknownRetainsIntent(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	c.SetClock(func() time.Time { return *now })
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x61)})
	snapshotDir(t, f, "v1", "s1")
	holdFirst(f, path.Join(catBase, "v1", snapsDir, "s1")+"/")
	if _, e := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "dead"}, h); CodeOf(e) != CodeOutcomeUnknown {
		t.Fatal(e)
	}
	*now = now.Add(DefaultOpDeadline + time.Second)
	f.failCreate = func(string) error { return errors.New("claim outcome unknown") }
	if n, e := g.SweepCopies(ctx); e == nil || n != 0 {
		t.Fatalf("unknown claim accepted: %d %v", n, e)
	}
	if ins := intentsOf(t, f, "v1"); len(ins) != 1 {
		t.Fatalf("unknown claim released root: %+v", ins)
	}
	f.failCreate = nil
	if n, e := g.SweepCopies(ctx); e != nil || n != 1 {
		t.Fatalf("claim recovery: %d %v", n, e)
	}
}
