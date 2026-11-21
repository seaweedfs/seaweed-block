package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// restoreFixture: v1 with snapshot s1, restored into v2.
func restoreFixture(t *testing.T) (*fakeFiler, *fakeBlobs, *Catalog, storage.ChunkHead) {
	t.Helper()
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Restore(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "rs1"}); err != nil {
		t.Fatal(err)
	}
	return f, b, c, h
}

// ownAll re-uploads every qualified chunk of vol under vol's own keys and
// publishes a HEAD with no qualified ref (what the restore's batch cuts do).
func ownAll(t *testing.T, f *fakeFiler, b *fakeBlobs, vol string) storage.ChunkHead {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, catBase+"/"+vol)
	if err != nil {
		t.Fatal(err)
	}
	h, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The batch cut's result in one step: every chunk under a fresh own key
	// (chunks/e2/...), a new manifest, one HEAD CAS, intents dropped.
	next := storage.ChunkHead{Epoch: 1, Holder: "h2", RemoteLSN: h.RemoteLSN, Chunks: map[uint64]storage.ChunkRef{}}
	for idx, ref := range h.Chunks {
		data, err := r.Get(ctx, ref.Key)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("chunks/e2/c%d/k", idx)
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		next.Chunks[idx] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	}
	next, intents, err := r.PrepareManifest(ctx, h, next, uint64(catGeo.BlockSize)*uint64(catGeo.ChunkBlocks))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next); err != nil {
		t.Fatal(err)
	}
	for _, id := range intents {
		if err := r.DeleteIntent(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// allQualified reports whether every chunk ref and segment key of h names another volume.
func allQualified(h storage.ChunkHead) bool {
	for _, ref := range h.Chunks {
		if !strings.HasPrefix(ref.Key, "@") {
			return false
		}
	}
	for _, seg := range h.Segments {
		if !strings.HasPrefix(seg.Key, "@") {
			return false
		}
	}
	return len(h.Chunks) > 0
}

func TestRestore_RecordsOriginForkRecordAndQualifiedHead(t *testing.T) {
	f, b, c, h := restoreFixture(t)
	ctx := context.Background()
	v, ok, err := c.ReadVolume(ctx, "v2")
	if err != nil || !ok || v.Origin != OriginRestore || v.Fork == nil || v.Fork.Source != "v1" || v.Parent == nil {
		t.Fatalf("restored VOLUME %+v ok=%v err=%v: want Origin restore with a fork record and Parent", v, ok, err)
	}
	r2, _ := NewChunkRemote(f, b, catBase+"/v2")
	h2, _ := r2.GetHead(ctx)
	if !allQualified(h2) || len(h2.Chunks) != len(h.Chunks) {
		t.Fatalf("restored HEAD %v: want all %d chunk refs (and segments) qualified", h2.Chunks, len(h.Chunks))
	}
	if _, err := c.EnsureVolume(ctx, "v2", v.Geometry); err != nil {
		t.Fatalf("a restored volume attaches like a fork: %v", err)
	}
}

func TestRestore_ReplayAndOriginMismatchRefused(t *testing.T) {
	_, _, c, _ := restoreFixture(t)
	ctx := context.Background()
	if r, err := c.Restore(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "rs1"}); err != nil || !r.Replayed {
		t.Fatalf("restore replay: %+v %v", r, err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "rs1"}); CodeOf(err) != CodeTargetExists {
		t.Fatalf("a fork onto a restored volume with the same request id must be refused: %v", err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "f1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Restore(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "f1"}); CodeOf(err) != CodeTargetExists {
		t.Fatalf("a restore onto a fork volume with the same request id must be refused: %v", err)
	}
}

func TestRestore_MarkIndependentOnlyWithoutQualifiedRefs(t *testing.T) {
	f, b, c, _ := restoreFixture(t)
	ctx := context.Background()
	if _, err := c.MarkIndependent(ctx, "v2"); CodeOf(err) != CodeRestoreIncomplete {
		t.Fatalf("marker while HEAD holds qualified refs: %v, want %s", err, CodeRestoreIncomplete)
	}
	if _, ok, err := c.ReadIndependence(ctx, "v2"); err != nil || ok {
		t.Fatalf("no marker may exist after a refused mark: ok=%v err=%v", ok, err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "f1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MarkIndependent(ctx, "v3"); CodeOf(err) != CodeNotRestore {
		t.Fatalf("marker on a fork: %v, want %s", err, CodeNotRestore)
	}
	h := ownAll(t, f, b, "v2")
	if ForeignRefs(h, "v2") != 0 {
		t.Fatalf("fixture: %d foreign refs left", ForeignRefs(h, "v2"))
	}
	m, err := c.MarkIndependent(ctx, "v2")
	if err != nil || m.HeadGen != h.Gen || m.VolumeID != "v2" {
		t.Fatalf("marker: %+v %v (HEAD gen %d)", m, err, h.Gen)
	}
	if again, err := c.MarkIndependent(ctx, "v2"); err != nil || again != m {
		t.Fatalf("marker is write-once (a replay returns it): %+v %v", again, err)
	}
	if got, ok, err := c.ReadIndependence(ctx, "v2"); err != nil || !ok || got != m {
		t.Fatalf("read marker: %+v %v %v", got, ok, err)
	}
}

// Deleting the source mid-restore is a normal state for every reader of T's fork record.
func TestRestore_SourceGoneIsNormal(t *testing.T) {
	f, b, c, _ := restoreFixture(t)
	ctx := context.Background()
	r2, _ := NewChunkRemote(f, b, catBase+"/v2")
	h2, _ := r2.GetHead(ctx)
	before := map[uint64][]byte{}
	for idx, ref := range h2.Chunks {
		data, err := r2.Get(ctx, ref.Key)
		if err != nil {
			t.Fatal(err)
		}
		before[idx] = data
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatalf("after T has its HEAD the source snapshot may go: %v", err)
	}
	detachHead(t, f, b, "v1")
	if _, err := c.DeleteVolume(ctx, "v1", nil); err != nil {
		t.Fatalf("delete the source volume: %v", err)
	}
	v, _, _ := c.ReadVolume(ctx, "v2")
	if _, err := c.EnsureVolume(ctx, "v2", v.Geometry); err != nil {
		t.Fatalf("attach of a restore whose source is gone must be normal: %v", err)
	}
	if r, err := c.Restore(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "rs1"}); err != nil || !r.Replayed {
		t.Fatalf("replay of a restore whose source is gone: %+v %v", r, err)
	}
	for idx, ref := range h2.Chunks {
		data, err := r2.Get(ctx, ref.Key)
		if err != nil || sha256.Sum256(data) != sha256.Sum256(before[idx]) || sha256.Sum256(data) != ref.SHA256 {
			t.Fatalf("chunk %d unreadable or changed after the source was deleted: %v", idx, err)
		}
	}
	h := ownAll(t, f, b, "v2")
	if _, err := c.MarkIndependent(ctx, "v2"); err != nil {
		t.Fatalf("the restore finishes with its source gone: %v (HEAD gen %d)", err, h.Gen)
	}
}

// Load-bearing (the claim ends at the first HEAD): after the restore's HEAD exists, the
// source snapshot and the source volume may both go; GC must keep every chunk T's HEAD names (qualified
// refs resolve to the owner's bindings) until T's own batch cuts replace them.
func TestRestore_GCRootsQualifiedRefsWithSnapshotAndSourceGone(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x31), 3: chunkBytes(0x33)})
	if _, e := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "req"}, h); e != nil {
		t.Fatal(e)
	}
	if e := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Restore(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "rs"}); e != nil {
		t.Fatal(e)
	}
	if _, e := c.DeleteSnapshot(ctx, "v1", "s1"); e != nil {
		t.Fatalf("retention/delete may retire S once T has a HEAD: %v", e)
	}
	detachHead(t, f, b.fakeBlobs, "v1")
	if _, e := c.DeleteVolume(ctx, "v1", nil); e != nil {
		t.Fatalf("delete the source volume: %v", e)
	}
	r2, _ := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	h2, e := r2.GetHead(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if _, e := g.Run(ctx); e != nil {
			t.Fatalf("gc: %v", e)
		}
		*now = now.Add(2 * time.Hour)
	}
	want := map[uint64][]byte{0: chunkBytes(0x31), 3: chunkBytes(0x33)}
	for idx, ref := range h2.Chunks {
		got, e := r2.Get(ctx, ref.Key)
		if e != nil || !bytes.Equal(got, want[idx]) {
			t.Fatalf("restored chunk %d lost after S and V were deleted and GC ran: %v", idx, e)
		}
	}
}

// A restore whose HEAD is absent or an aborted tombstone never attaches, like a fork.
func TestRestore_WithoutLiveHeadNeverAttaches(t *testing.T) {
	f, _, c, _ := catalogFixture(t)
	ctx := context.Background()
	v := VolumeInfo{Format: volumeFormat, VolumeID: "v9", Geometry: catGeo, Origin: OriginRestore, CreatedNs: 1,
		Parent: &Parent{VolumeID: "v1", Snapshot: "s1", Gen: 1}, Fork: &ForkRecord{RequestID: "rq", Source: "v1", Snapshot: "s1"}}
	e, err := volumeEntry(v, "rq")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CreateEntry(ctx, catBase+"/v9", e, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v9", catGeo); CodeOf(err) != CodeForkIncomplete {
		t.Fatalf("restore without a HEAD attached: %v, want %s", err, CodeForkIncomplete)
	}
	if _, err := c.AbortFork(ctx, "v9"); err != nil {
		t.Fatalf("abort the unpublished restore (sweep): %v", err)
	}
	if _, err := c.EnsureVolume(ctx, "v9", catGeo); CodeOf(err) != CodeForkIncomplete {
		t.Fatalf("restore with an aborted HEAD attached: %v, want %s", err, CodeForkIncomplete)
	}
	if _, err := c.MarkIndependent(ctx, "v9"); CodeOf(err) != CodeRestoreIncomplete {
		t.Fatalf("marker on an aborted restore: %v, want %s", err, CodeRestoreIncomplete)
	}
}

// Snapshots of a restore are refused until it is independent.
func TestRestore_SnapshotsRefusedUntilIndependent(t *testing.T) {
	f, b, c, _ := restoreFixture(t)
	ctx := context.Background()
	r2, _ := NewChunkRemote(f, b, catBase+"/v2")
	h2, _ := r2.GetHead(ctx)
	if _, err := c.CreateSnapshot(ctx, "v2", SnapshotRequest{Name: "early", RequestID: "e"}, h2); CodeOf(err) != CodeRestoreIncomplete {
		t.Fatalf("snapshot of a restore in progress: %v, want %s", err, CodeRestoreIncomplete)
	}
	if _, ok, err := c.SnapshotReplay(ctx, "v2", SnapshotRequest{Name: "early", RequestID: "e"}); err != nil || ok {
		t.Fatalf("a refused snapshot must leave nothing: ok=%v err=%v", ok, err)
	}
	h := ownAll(t, f, b, "v2")
	if _, err := c.CreateSnapshot(ctx, "v2", SnapshotRequest{Name: "late", RequestID: "l"}, h); CodeOf(err) != CodeRestoreIncomplete {
		t.Fatalf("own chunks but no marker yet: %v, want %s", err, CodeRestoreIncomplete)
	}
	if _, err := c.MarkIndependent(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	s, err := c.CreateSnapshot(ctx, "v2", SnapshotRequest{Name: "late", RequestID: "l"}, h)
	if err != nil {
		t.Fatalf("snapshot after the marker: %v", err)
	}
	if ForeignRefs(s.Head(), "v2") != 0 {
		t.Fatalf("a snapshot of an independent restore names %d foreign refs", ForeignRefs(s.Head(), "v2"))
	}
}

// hangingDeleteFiler blocks DeleteEntry until its ctx ends (or the test
// releases it) and records whether that ctx carried a deadline.
type hangingDeleteFiler struct {
	Filer
	hadDeadline chan bool
	release     chan struct{}
}

func (h hangingDeleteFiler) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	_, ok := ctx.Deadline()
	h.hadDeadline <- ok
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.release:
		return errors.New("released by the test")
	}
}

// dropIntent outlives a cancelled caller, but one unresponsive delete must still be bounded.
func TestCatalog_DropIntentIsBoundedAfterTheCallerIsCancelled(t *testing.T) {
	old := dropIntentTimeout
	dropIntentTimeout = 50 * time.Millisecond
	t.Cleanup(func() { dropIntentTimeout = old })
	h := hangingDeleteFiler{Filer: newFakeFiler(), hadDeadline: make(chan bool, 1), release: make(chan struct{})}
	c := &Catalog{f: h}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	t.Cleanup(func() { // an unbounded delete (the failing case) is released and joined
		close(h.release)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("cleanup: dropIntent did not return")
		}
	})
	go func() { c.dropIntent(ctx, "/vols/v1", "op-1"); close(done) }()
	select {
	case ok := <-h.hadDeadline:
		if !ok {
			t.Fatal("the intent delete ran without any deadline")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dropIntent never reached the filer")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dropIntent did not return: the delete is unbounded")
	}
}

// A cut image saved before the marker still names the source's chunks: the
// marker must not let it become a snapshot.
func TestRestore_SnapshotOfAStalePreIndependenceImageRefused(t *testing.T) {
	f, b, c, _ := restoreFixture(t)
	ctx := context.Background()
	r2, _ := NewChunkRemote(f, b, catBase+"/v2")
	stale, _ := r2.GetHead(ctx)
	h := ownAll(t, f, b, "v2")
	if _, err := c.MarkIndependent(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	req := SnapshotRequest{Name: "stale", RequestID: "s"}
	if _, err := c.CreateSnapshot(ctx, "v2", req, stale); CodeOf(err) != CodeRestoreIncomplete {
		t.Fatalf("stale foreign image after the marker: %v, want %s", err, CodeRestoreIncomplete)
	}
	if _, ok, err := c.SnapshotReplay(ctx, "v2", req); err != nil || ok {
		t.Fatalf("a refused snapshot must leave nothing: ok=%v err=%v", ok, err)
	}
	if list, _, err := c.ListSnapshots(ctx, "v2", ListOptions{}); err != nil || len(list) != 0 {
		t.Fatalf("snapshots after the refusal: %v %v", list, err)
	}
	if _, err := c.CreateSnapshot(ctx, "v2", SnapshotRequest{Name: "own", RequestID: "o"}, h); err != nil {
		t.Fatalf("the own image must still be accepted: %v", err)
	}
}

// Retention disposition with an aborted first HEAD on a restore target: the
// fork claim alone no longer holds the parent (liveClaims counts any HEAD,
// aborted included), but the fork-record scan does (forkParentsNeeded treats
// an aborted HEAD as no HEAD), so the parent snapshot is kept as fork_parent
// until the aborted target is deleted. That conservative hold is the rule.
func TestRestore_AbortedFirstHeadKeepsTheParentUntilTheTargetIsDeleted(t *testing.T) {
	f, c, _, _ := retentionFixture(t, 1)
	ctx := context.Background()
	if err := c.SetRetentionPolicy(ctx, "v1", RetentionPolicy{}); err != nil { // keep nothing by policy
		t.Fatal(err)
	}
	for _, vol := range []string{"v1", "v2"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	holdFirst(f, catBase+"/v2/HEAD")
	if _, err := c.Restore(ctx, ForkRequest{Source: "v1", Snapshot: "s00", Target: "v2", RequestID: "rs"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("restore with a held HEAD create: %v", err)
	}
	if claimed, err := c.AbortFork(ctx, "v2"); err != nil || !claimed {
		t.Fatalf("abort the unpublished restore: claimed=%v %v", claimed, err)
	}
	if live, err := c.liveClaims(ctx, "v1", "s00", []string{forkClaim("v2", "rs")}); err != nil || len(live) != 0 {
		t.Fatalf("claim on an aborted target: live=%v %v, want not live (any HEAD ends the claim)", live, err)
	}
	res, err := c.Retain(ctx, "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	if k := kept(res.Decisions); len(k) != 1 || k["s00"] == nil || k["s00"][0] != keepForkParent || len(res.Deleted) != 0 {
		t.Fatalf("aborted restore: kept %v deleted %v; want s00 kept as fork_parent", k, res.Deleted)
	}
	if s, err := c.allSnapshots(ctx, "v1"); err != nil || len(s) != 1 {
		t.Fatalf("parent after retention: %v %v", s, err)
	}
	if _, err := c.DeleteVolume(ctx, "v2", nil); err != nil {
		t.Fatalf("delete the aborted restore: %v", err)
	}
	res, err = c.Retain(ctx, "v1", false)
	if err != nil || len(res.Deleted) != 1 || res.Deleted[0] != "s00" {
		t.Fatalf("after the target is gone: deleted %v (%v), want s00", res.Deleted, err)
	}
}
