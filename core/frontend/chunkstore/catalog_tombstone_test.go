package chunkstore

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A sweep settles a dead fork by claiming the target's HEAD, the fork's
// publishing entry, with an aborted HEAD tombstone (W5 ruling, 2026-11-15
// 17:30Z): the late HEAD insert fails, a retry is refused, and the volume
// never attaches.
func TestFork_SweepTombstonesADeadFork(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	holdFirst(f, catBase+"/v2/HEAD")
	req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}
	if _, err := c.Fork(ctx, req); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork with a held HEAD create: %v", err)
	}
	if claimed, err := c.AbortFork(ctx, "v2"); err != nil || !claimed {
		t.Fatalf("sweep over the dead fork: claimed=%v %v", claimed, err)
	}
	if n := f.releaseDeferredCreates(); n != 0 {
		t.Fatalf("the dead fork's late HEAD landed over the tombstone (%d applied)", n)
	}
	if again, err := c.AbortFork(ctx, "v2"); err != nil || !again {
		t.Fatalf("a repeated sweep: claimed=%v %v; want the tombstone found", again, err)
	}
	if hd, ok, err := c.ReadHead(ctx, "v2"); err != nil || !ok || hd.State != storage.ChunkHeadAborted || len(hd.Chunks) != 0 {
		t.Fatalf("target HEAD after the sweep: %+v ok=%v %v", hd, ok, err)
	}
	if _, err := c.Fork(ctx, req); CodeOf(err) != CodeTargetExists {
		t.Fatalf("retry of the aborted fork: %v; want target_exists", err)
	}
	if _, err := c.EnsureVolume(ctx, "v2", catGeo); CodeOf(err) != CodeForkIncomplete {
		t.Fatalf("attach of the aborted fork: %v; want fork_incomplete", err)
	}
}

// The tombstone loses to a published fork: the fork's keys are reachable
// through its HEAD, and its replay still works.
func TestFork_TombstoneLosesToPublishedFork(t *testing.T) {
	_, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}
	if _, err := c.Fork(ctx, req); err != nil {
		t.Fatal(err)
	}
	if claimed, err := c.AbortFork(ctx, "v2"); err != nil || claimed {
		t.Fatalf("sweep over a published fork: claimed=%v %v; want published", claimed, err)
	}
	if res, err := c.Fork(ctx, req); err != nil || !res.Replayed {
		t.Fatalf("replay after a refused sweep: %+v %v", res, err)
	}
}
