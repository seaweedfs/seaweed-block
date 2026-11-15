package chunkstore

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
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
	_ = storage.ChunkHead{}
}
