package chunkstore

import (
	"context"
	"path"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// Schedules where the filer holds an attempt's create and runs it after the
// attempt has returned (second review of #111, P1). The rule (W5): an
// attempt deletes only its own intent, and only once its own create is
// confirmed or can never apply; never another attempt's, even under the same
// request, since that attempt's create may still land.

// holdFirst makes the filer hold the first create of p (p ending in "/":
// the first create under it).
func holdFirst(f *fakeFiler, p string) {
	held := false
	f.deferCreate = func(q string, _ *filerpb.Entry) bool {
		if held || !(q == p || strings.HasSuffix(p, "/") && strings.HasPrefix(q, p)) {
			return false
		}
		held = true
		return true
	}
}

// holdFirstBehindAnother holds the first create of p and, in the same
// moment, lets another attempt of the same request write p (a copy of the
// held entry stamped with another attempt id).
func holdFirstBehindAnother(f *fakeFiler, p string) {
	held := false
	f.deferCreate = func(q string, e *filerpb.Entry) bool {
		if q != p || held {
			return false
		}
		held = true
		other := proto.Clone(e).(*filerpb.Entry)
		other.Extended["swb.attempt"] = []byte("0-00000000000000ff")
		f.entries[q] = other
		return true
	}
}

// gcRemove removes an entry, as GC does after a delete.
func gcRemove(f *fakeFiler, p string) {
	f.mu.Lock()
	delete(f.entries, p)
	f.mu.Unlock()
}

// assertKeysProtected fails unless every key is named by an intent of vol.
func assertKeysProtected(t *testing.T, f *fakeFiler, vol string, keys []string) {
	t.Helper()
	named := map[string]bool{}
	for _, in := range intentsOf(t, f, vol) {
		for _, k := range in.Keys {
			named[k] = true
		}
	}
	for _, k := range keys {
		if !named[k] {
			t.Fatalf("key %s of the late entry is named by no intent of %s: GC may delete it", k, vol)
		}
	}
}

func lateSnapshotKeys(t *testing.T, f *fakeFiler, vol, name string) []string {
	t.Helper()
	dir := path.Join(catBase, vol, snapsDir)
	e, err := f.LookupEntry(context.Background(), dir, name)
	if err != nil {
		t.Fatalf("the held create did not land: %v", err)
	}
	info, _, err := decodeSnap(path.Join(dir, name), e, e.GetContent())
	if err != nil {
		t.Fatal(err)
	}
	return sortedKeysOf(info.Head(), "")
}

func lateHeadKeys(t *testing.T, f *fakeFiler, b *fakeBlobs, vol string) []string {
	t.Helper()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, vol))
	h, err := r.GetHead(context.Background())
	if err != nil {
		t.Fatalf("the held HEAD create did not land: %v", err)
	}
	return sortedKeysOf(h, "")
}

// Attempt A's snapshot create is held. Retry B of the same request takes
// over A's attempt path (the name record names it) and publishes there,
// deleting only B's intent. A's held insert can never land: the path is
// taken, and attempt paths are never removed, not even by DeleteSnapshot.
// A sweep then finds A's attempt published.
func TestCatalog_HeldSnapshotCreateKeepsItsIntent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	holdFirst(f, catBase+"/v1/snaps/s1/")
	req := SnapshotRequest{Name: "s1", RequestID: "r"}
	if _, err := c.CreateSnapshot(ctx, "v1", req, h); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("attempt A, create held: %v, want outcome_unknown", err)
	}
	a := intentsOf(t, f, "v1")
	if len(a) != 1 {
		t.Fatalf("attempt A left %d intents, want its own", len(a))
	}
	attempt := path.Base(snapPath(t, f, "v1", "s1"))
	if a[0].Holder != "snapshot:v1/s1/"+attempt {
		t.Fatalf("intent holder %q, want it to name the attempt %s", a[0].Holder, attempt)
	}
	if info, err := c.CreateSnapshot(ctx, "v1", req, h); err != nil || info.State != SnapActive {
		t.Fatalf("retry B: %+v %v", info, err)
	}
	if in := intentsOf(t, f, "v1"); len(in) != 1 || in[0].ID != a[0].ID {
		t.Fatalf("after B: intents %+v, want only A's %s", in, a[0].ID)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	if n := f.releaseDeferredCreates(); n != 0 {
		t.Fatalf("A's held insert landed (%d applied); its attempt path is held for good", n)
	}
	if claimed, err := c.AbortSnapshot(ctx, "v1", "s1", attempt); err != nil || claimed {
		t.Fatalf("sweep over A's published attempt: claimed=%v %v; want published", claimed, err)
	}
}

// A sweep settles a dead attempt by claiming its path with an aborted
// tombstone: the late insert fails, the name reads as aborted until it is
// deleted, and then the name can be reused.
func TestCatalog_SweepTombstonesADeadSnapshotAttempt(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	holdFirst(f, catBase+"/v1/snaps/s1/")
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("attempt with a held create: %v", err)
	}
	attempt := path.Base(snapPath(t, f, "v1", "s1"))
	if claimed, err := c.AbortSnapshot(ctx, "v1", "s1", attempt); err != nil || !claimed {
		t.Fatalf("sweep over the dead attempt: claimed=%v %v", claimed, err)
	}
	if n := f.releaseDeferredCreates(); n != 0 {
		t.Fatalf("the dead attempt's late insert landed over the tombstone (%d applied)", n)
	}
	if again, err := c.AbortSnapshot(ctx, "v1", "s1", attempt); err != nil || !again {
		t.Fatalf("a repeated sweep: claimed=%v %v; want the tombstone found", again, err)
	}
	for _, include := range []bool{false, true} {
		if list, _, err := c.ListSnapshots(ctx, "v1", ListOptions{IncludeDeleting: include}); err != nil || len(list) != 0 {
			t.Fatalf("list (includeDeleting=%v) shows the aborted attempt: %+v %v", include, list, err)
		}
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeSnapshotNotFound {
		t.Fatalf("fork from the aborted attempt: %v; want snapshot_not_found", err)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); CodeOf(err) != CodeSnapshotExists {
		t.Fatalf("retry of the aborted request: %v; want snapshot_exists until the name is deleted", err)
	}
	if st, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != SnapDeleting {
		t.Fatalf("delete of the aborted name: %q %v", st, err)
	}
	info, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r2"}, h)
	if err != nil || info.State != SnapActive {
		t.Fatalf("snapshot after the name was freed: %+v %v", info, err)
	}
	if p := snapPath(t, f, "v1", "s1"); path.Base(p) == attempt {
		t.Fatal("the new snapshot reused the aborted attempt's path")
	}
}

// Another request holding the name refuses before any intent is written.
func TestCatalog_NameHeldByAnotherRequestWritesNoIntent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	// Another request takes the name between this request's check and its
	// name record create.
	f.failCreate = func(p string) error {
		if p != catBase+"/v1/"+snapNamesDir+"/s1" {
			return nil
		}
		other, _ := nameRecordEntry(nameRecord{Format: snapFormat, VolumeID: "v1", Name: "s1", RequestID: "other", Attempt: "1-00000000000000aa"})
		f.entries[p] = other
		return ErrExists
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); CodeOf(err) != CodeSnapshotExists {
		t.Fatalf("name held by another request: %v, want snapshot_exists", err)
	}
	if in := intentsOf(t, f, "v1"); len(in) != 0 {
		t.Fatalf("a refused name left %d intents", len(in))
	}
}

// Fork: attempt A's HEAD create is held; retry B completes the fork and
// deletes only B's intent; a replay C deletes none. The target's HEAD is
// later removed; A's held create lands with its qualified keys protected.
func TestFork_HeldHeadCreateKeepsItsIntent(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	p := catBase + "/v2/HEAD"
	holdFirst(f, p)
	req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}
	if _, err := c.Fork(ctx, req); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("attempt A, HEAD create held: %v, want outcome_unknown", err)
	}
	a := intentsOf(t, f, "v2")
	if len(a) != 1 {
		t.Fatalf("attempt A left %d intents, want its own", len(a))
	}
	if res, err := c.Fork(ctx, req); err != nil || res.Replayed {
		t.Fatalf("retry B: %+v %v", res, err)
	}
	if res, err := c.Fork(ctx, req); err != nil || !res.Replayed {
		t.Fatalf("replay C: %+v %v", res, err)
	}
	if in := intentsOf(t, f, "v2"); len(in) != 1 || in[0].ID != a[0].ID {
		t.Fatalf("after B and C: intents %+v, want only A's %s (A's HEAD create is still held)", in, a[0].ID)
	}
	gcRemove(f, p)
	if n := f.releaseDeferredCreates(); n != 1 {
		t.Fatalf("A's held HEAD create applied %d times, want 1", n)
	}
	assertKeysProtected(t, f, "v2", lateHeadKeys(t, f, b, "v2"))
}

// Fork: attempt A's HEAD create is held while another attempt writes HEAD.
// A reports the completed fork and keeps its own intent.
func TestFork_HeldHeadCreateBehindAnotherAttempt(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	p := catBase + "/v2/HEAD"
	holdFirstBehindAnother(f, p)
	res, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"})
	if in := intentsOf(t, f, "v2"); len(in) != 1 {
		t.Fatalf("attempt A (err %v) left %d intents; its held create needs its own", err, len(in))
	}
	if err != nil || !res.Replayed || res.HeadGen != 1 {
		t.Fatalf("attempt A behind another attempt: %+v %v, want the completed fork", res, err)
	}
	gcRemove(f, p)
	if n := f.releaseDeferredCreates(); n != 1 {
		t.Fatalf("A's held HEAD create applied %d times, want 1", n)
	}
	assertKeysProtected(t, f, "v2", lateHeadKeys(t, f, b, "v2"))
}

// The filer's "already exists" answer to a fork's HEAD create: HEAD is there
// (only this request writes it), so the fork is complete and the attempt
// deletes its own intent.
func TestFork_RefusedHeadCreateDropsItsIntent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}
	if _, err := c.Fork(ctx, req); err != nil {
		t.Fatal(err)
	}
	// HEAD is written by another attempt after this attempt finds it absent
	// and before this attempt's create.
	p := catBase + "/v2/HEAD"
	head := f.entries[p]
	gcRemove(f, p)
	f.beforeCreate = func(q string) {
		if path.Dir(q) == catBase+"/v2/intents" {
			f.entries[p] = head
		}
	}
	res, err := c.Fork(ctx, req)
	if err != nil || !res.Replayed || res.HeadGen != 1 {
		t.Fatalf("refused HEAD create: %+v %v, want the completed fork", res, err)
	}
	if in := intentsOf(t, f, "v2"); len(in) != 0 {
		t.Fatalf("a refused HEAD create left %d intents", len(in))
	}
}
