package chunkstore

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Volume delete (claude05's item, 2026-11-16 02:39Z).

// detachHead publishes vol's HEAD as a clean detach by its holder (what a
// volume server's Detach writes).
func detachHead(t *testing.T, f *fakeFiler, b *fakeBlobs, vol string) {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, vol))
	if err != nil {
		t.Fatal(err)
	}
	h, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := h
	next.State = storage.ChunkHeadDetached
	next.DetachedBy = &storage.ChunkDetachedBy{Holder: h.Holder, Epoch: h.Epoch, Gen: h.Gen + 1}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next); err != nil {
		t.Fatal(err)
	}
}

func deleteFixture(t *testing.T) (*fakeFiler, *fakeBlobs, *Catalog, storage.ChunkHead) {
	t.Helper()
	f, b, c, h := catalogFixture(t) // v1 HEAD held by h1 (attached)
	ctx := context.Background()
	for _, vol := range []string{"v1", "w2", "w3"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"s1", "s2"} {
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: name, RequestID: "r-" + name}, h); err != nil {
			t.Fatal(err)
		}
	}
	return f, b, c, h
}

// An attached volume (HEAD held), or one the blockmaster reports in use,
// is refused with volume_in_use and nothing is written.
func TestCatalog_DeleteVolumeRefusesAVolumeInUse(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	before := len(f.allWritten())
	if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeVolumeInUse {
		t.Fatalf("delete of an attached volume: %v; want volume_in_use", err)
	}
	detachHead(t, f, b, "v1")
	before = len(f.allWritten())
	busy := func(context.Context, string) (bool, string, error) { return true, "assigned to r1", nil }
	_, err := c.DeleteVolume(ctx, "v1", busy)
	var ce *CatalogError
	if !errors.As(err, &ce) || ce.Code != CodeVolumeInUse || ce.Retryable || !strings.Contains(ce.Detail(), "assigned to r1") {
		t.Fatalf("delete of an assigned volume: %v; want volume_in_use naming the reason", err)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("a refused delete wrote %d entries", got-before)
	}
	if v, _, _ := c.ReadVolume(ctx, "v1"); v.State != "" {
		t.Fatalf("refused delete changed VOLUME state to %q", v.State)
	}
}

// A volume with a snapshot a live fork claims is refused before anything
// is written (snapshot_claimed).
func TestCatalog_DeleteVolumeRefusesAClaimedSnapshot(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	holdFirst(f, catBase+"/w3/HEAD")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w3", RequestID: "f3"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork w3: %v", err)
	}
	before := len(f.allWritten())
	if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeSnapshotClaimed {
		t.Fatalf("delete with a claimed snapshot: %v; want snapshot_claimed", err)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("a refused delete wrote %d entries", got-before)
	}
}

// The acceptance flow on the fake: fork w2 from s1, delete w2, then s1 can
// be deleted; the deleted id is never attached, recreated or forked to
// again; an incomplete fork w3 is deleted too and its claim released; a
// rerun replays.
func TestCatalog_DeleteVolumeTombstonesAndReleases(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "w2", RequestID: "f2"}); err != nil {
		t.Fatalf("fork w2: %v", err)
	}
	if _, err := c.CreateSnapshot(ctx, "w2", SnapshotRequest{Name: "w2snap", RequestID: "rw"}, mustHead(t, c, "w2")); err != nil {
		t.Fatal(err)
	}
	res, err := c.DeleteVolume(ctx, "w2", nil)
	if err != nil || res.Replayed || strings.Join(res.SnapshotsDeleted, ",") != "w2snap" {
		t.Fatalf("delete w2: %+v %v", res, err)
	}
	h, has, err := c.ReadHead(ctx, "w2")
	if err != nil || !has || h.State != storage.ChunkHeadDeleted || len(h.Chunks) != 0 {
		t.Fatalf("w2 HEAD: %+v has=%t %v; want a deleted tombstone with no chunks", h, has, err)
	}
	if v, _, _ := c.ReadVolume(ctx, "w2"); v.State != VolumeDeleted {
		t.Fatalf("w2 VOLUME state %q", v.State)
	}
	if _, ok, _ := c.resolveSnap(ctx, "w2", "w2snap"); ok {
		t.Fatal("w2's snapshot name is still held")
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatalf("delete s1 after its fork was deleted: %v", err)
	}
	// The id is never reused.
	if _, err := c.EnsureVolume(ctx, "w2", catGeo); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("attach/create of deleted w2: %v; want volume_deleted", err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w2", RequestID: "again"}); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("fork to deleted w2: %v; want volume_deleted", err)
	}
	// A rerun replays.
	if res, err := c.DeleteVolume(ctx, "w2", nil); err != nil || !res.Replayed {
		t.Fatalf("rerun: %+v %v; want a replay", res, err)
	}

	// An incomplete fork (VOLUME, no HEAD) is deleted and its claim
	// released, so its parent snapshot can be deleted.
	holdFirst(f, catBase+"/w3/HEAD")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "w3", RequestID: "f3"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork w3: %v", err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s2"); CodeOf(err) != CodeSnapshotClaimed {
		t.Fatalf("delete s2 under the incomplete fork: %v; want snapshot_claimed", err)
	}
	res, err = c.DeleteVolume(ctx, "w3", nil)
	if err != nil || !res.ClaimReleased {
		t.Fatalf("delete w3: %+v %v; want its claim released", res, err)
	}
	if got := claimsOf(t, c, "s2"); len(got) != 0 {
		t.Fatalf("s2 claims after w3 was deleted: %v", got)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s2"); err != nil {
		t.Fatalf("delete s2 after w3 was deleted: %v", err)
	}
	_ = b
}

// A volume server that attaches between the delete's check and its HEAD
// tombstone makes the tombstone's condition fail; the delete re-checks and
// refuses (volume_in_use), and VOLUME is untouched.
func TestCatalog_DeleteVolumeLosesToAConcurrentAttach(t *testing.T) {
	f, b, c, _ := deleteFixture(t)
	ctx := context.Background()
	detachHead(t, f, b, "v1")
	headPath := path.Join(catBase, "v1", headName)
	lookups := 0
	f.afterLookup = func(p string) {
		if p != headPath {
			return
		}
		lookups++
		if lookups != 2 { // 1: the delete's check; 2: the tombstone's read
			return
		}
		cur, _ := decodeHead(p, f.entries[p])
		att := cur
		att.Gen, att.Epoch, att.Holder, att.State, att.DetachedBy = cur.Gen+1, cur.Epoch+1, "h2", storage.ChunkHeadActive, nil
		e, err := headEntry(att)
		if err != nil {
			t.Fatal(err)
		}
		f.entries[p] = e // h2 attaches (takeover CAS)
	}
	if _, err := c.DeleteVolume(ctx, "v1", nil); CodeOf(err) != CodeVolumeInUse {
		t.Fatalf("delete racing an attach: %v; want volume_in_use", err)
	}
	f.afterLookup = nil
	h, _, _ := c.ReadHead(ctx, "v1")
	if h.Holder != "h2" || h.State == storage.ChunkHeadDeleted {
		t.Fatalf("HEAD after the lost delete: %+v; want h2's", h)
	}
	if v, _, _ := c.ReadVolume(ctx, "v1"); v.State != "" {
		t.Fatalf("VOLUME state %q after the refused delete", v.State)
	}
}

func mustHead(t *testing.T, c *Catalog, vol string) storage.ChunkHead {
	t.Helper()
	h, has, err := c.ReadHead(context.Background(), vol)
	if err != nil || !has {
		t.Fatalf("HEAD of %s: has=%t %v", vol, has, err)
	}
	return h
}
