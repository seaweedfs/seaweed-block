package chunkstore

import (
	"context"
	"path"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// Schedules where the filer holds an attempt's create and runs it after the
// attempt has returned (second review of #111, P1). The rule (W5): an
// attempt deletes only its own intent, and only once its own create is
// confirmed or can never apply; never another attempt's, even under the same
// request, since that attempt's create may still land.

// holdFirst makes the filer hold the first create of p.
func holdFirst(f *fakeFiler, p string) {
	held := false
	f.deferCreate = func(q string, _ *filerpb.Entry) bool {
		if q != p || held {
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
	info, _, err := decodeSnap(path.Join(dir, name), e)
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

// Attempt A's snapshot create is held; retry B of the same request publishes
// the snapshot and deletes only B's intent. The snapshot is deleted and GC
// removes it; then A's held create lands. Its keys are still protected by
// A's intent.
func TestCatalog_HeldSnapshotCreateKeepsItsIntent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	p := catBase + "/v1/snaps/s1"
	holdFirst(f, p)
	req := SnapshotRequest{Name: "s1", RequestID: "r"}
	if _, err := c.CreateSnapshot(ctx, "v1", req, h); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("attempt A, create held: %v, want outcome_unknown", err)
	}
	a := intentsOf(t, f, "v1")
	if len(a) != 1 {
		t.Fatalf("attempt A left %d intents, want its own", len(a))
	}
	if info, err := c.CreateSnapshot(ctx, "v1", req, h); err != nil || info.Replayed {
		t.Fatalf("retry B: %+v %v", info, err)
	}
	if in := intentsOf(t, f, "v1"); len(in) != 1 || in[0].ID != a[0].ID {
		t.Fatalf("after B: intents %+v, want only A's %s (A's create is still held)", in, a[0].ID)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	gcRemove(f, p)
	if n := f.releaseDeferredCreates(); n != 1 {
		t.Fatalf("A's held create applied %d times, want 1", n)
	}
	assertKeysProtected(t, f, "v1", lateSnapshotKeys(t, f, "v1", "s1"))
}

// Attempt A's create is held while another attempt of the same request
// writes the snapshot. A reports the snapshot but keeps its own intent; when
// the snapshot is later removed, A's create lands with its keys protected.
func TestCatalog_HeldSnapshotCreateBehindAnotherAttempt(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	p := catBase + "/v1/snaps/s1"
	holdFirstBehindAnother(f, p)
	info, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h)
	if in := intentsOf(t, f, "v1"); len(in) != 1 {
		t.Fatalf("attempt A (err %v) left %d intents; its held create needs its own", err, len(in))
	}
	if err != nil || !info.Replayed || info.HeadGen != h.Gen {
		t.Fatalf("attempt A behind another attempt: %+v %v, want the replayed snapshot", info, err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	gcRemove(f, p)
	if n := f.releaseDeferredCreates(); n != 1 {
		t.Fatalf("A's held create applied %d times, want 1", n)
	}
	assertKeysProtected(t, f, "v1", lateSnapshotKeys(t, f, "v1", "s1"))
}

// The filer's own "already exists" answer refuses the create for good: the
// attempt deletes its own intent and leaks none.
func TestCatalog_RefusedSnapshotCreateDropsItsIntent(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "other"}, h); err != nil {
		t.Fatal(err)
	}
	// The name is taken after this request's read and before its create.
	taken := f.entries[catBase+"/v1/snaps/s1"]
	gcRemove(f, catBase+"/v1/snaps/s1")
	f.beforeCreate = func(p string) {
		if path.Dir(p) == catBase+"/v1/intents" {
			f.entries[catBase+"/v1/snaps/s1"] = taken
		}
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); CodeOf(err) != CodeSnapshotExists {
		t.Fatalf("refused create: %v, want snapshot_exists", err)
	}
	if in := intentsOf(t, f, "v1"); len(in) != 0 {
		t.Fatalf("a refused create left %d intents", len(in))
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
