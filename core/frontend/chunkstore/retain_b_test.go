package chunkstore

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/retain"
)

// snapshotWithMeta names v's current image again as snapshot name with
// the given retention facts (a snapshot transaction with no new chunks).
func snapshotWithMeta(t *testing.T, c *BCatalog, v VolumeRef, name string, meta layout.SnapshotMeta, session string, position uint64) {
	t.Helper()
	ctx := context.Background()
	s, _, err := c.open(v)
	if err != nil {
		t.Fatal(err)
	}
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	h, _, _ := s.ReadHead(ctx)
	r, _ := layout.NewReader(c.base, v.Dir(), s.Fetch)
	img, err := r.Open(ctx, *h.Current)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, _ := img.Resolve(ctx, 1)
	m := map[uint64]layout.Ref{1: ref}
	if _, err := layout.RunBegin(ctx, s, me, session); err != nil {
		t.Fatal(err)
	}
	root, segs, _ := layout.Build(m)
	rn, rb, _ := layout.NameRoot(root)
	objs := map[string][]byte{rn.Path(): rb}
	for n, b := range segs {
		objs[n.Path()] = b
	}
	for rel, b := range objs {
		if err := layout.EnsureObject(ctx, s, rel, b, session); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := layout.RunCommit(ctx, s, s, v.Name, me, layout.CommitRequest{Session: session, Prev: m, Next: m, Position: position, Snapshot: name, Meta: meta}); err != nil {
		t.Fatal(err)
	}
}

func names(ns []string) string {
	out := append([]string(nil), ns...)
	sort.Strings(out)
	return strings.Join(out, ",")
}

// Retention on the layout keeps the old behaviour: the policy from the
// volume's RETENTION entry (default when absent) over HEAD's snapshot
// facts; pinned and unexpired snapshots kept; the scheduled class ranked
// by its own keep label; a snapshot a restore target depends on refused by
// the delete and reported; --dry-run writes nothing.
func TestBCatalog_RetainKeepsTheOldBehaviour(t *testing.T) {
	ctx := context.Background()
	c, err := NewBCatalog(newFakeFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v")
	if _, err := c.CreateVolume(ctx, v, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	commitSnapshot(t, c, v, []byte("data"), "")
	now := time.Unix(10_000_000, 0)
	at := func(h int) int64 { return now.Add(-time.Duration(h) * time.Hour).UnixNano() }
	facts := []struct {
		name string
		meta layout.SnapshotMeta
	}{
		{"a1", layout.SnapshotMeta{CreatedNs: at(1), Class: layout.ClassSemantic}},
		{"a2", layout.SnapshotMeta{CreatedNs: at(2), Class: layout.ClassSemantic}},
		{"a3", layout.SnapshotMeta{CreatedNs: at(3), Class: layout.ClassSemantic}},
		{"a4", layout.SnapshotMeta{CreatedNs: at(4), Class: layout.ClassSemantic}},
		{"pin", layout.SnapshotMeta{CreatedNs: at(30), Class: layout.ClassPinned}},
		{"hold", layout.SnapshotMeta{CreatedNs: at(31), ExpiresNs: now.Add(time.Hour).UnixNano()}},
		{"lapsed", layout.SnapshotMeta{CreatedNs: at(32), ExpiresNs: now.Add(-time.Hour).UnixNano()}},
		{"s1", layout.SnapshotMeta{CreatedNs: at(5), Class: layout.ClassScheduled, Labels: map[string]string{retain.LabelScheduleKeep: "1"}}},
		{"s2", layout.SnapshotMeta{CreatedNs: at(6), Class: layout.ClassScheduled}},
	}
	for i, f := range facts {
		snapshotWithMeta(t, c, v, f.name, f.meta, "m"+f.name, uint64(i+2))
	}
	if p, err := c.ReadRetention(ctx, v); err != nil || p != retain.DefaultPolicy() {
		t.Fatalf("policy never set: %+v %v; want the default", p, err)
	}
	if err := c.SetRetention(ctx, v, retain.Policy{KeepNewest: 2}); err != nil {
		t.Fatal(err)
	}
	// a4 is restored from: the plan expires it, the delete refuses it.
	if _, err := c.Restore(ctx, v, "a4", newRef(t, "t")); err != nil {
		t.Fatal(err)
	}
	dry, err := c.Retain(ctx, v, now, true)
	if err != nil {
		t.Fatal(err)
	}
	want := "a3,a4,lapsed,s2"
	if names(dry.Deleted) != want {
		t.Fatalf("dry run would delete %s; want %s (decisions %+v)", names(dry.Deleted), want, dry.Decisions)
	}
	if h, _, _ := c.ReadHead(ctx, v); len(h.Snapshots) != len(facts) {
		t.Fatalf("a dry run deleted: %d snapshots left", len(h.Snapshots))
	}
	got, err := c.Retain(ctx, v, now, false)
	if err != nil {
		t.Fatal(err)
	}
	if names(got.Deleted) != "a3,lapsed,s2" || names(got.Refused) != "a4" {
		t.Fatalf("retain deleted %s refused %s; want a3,lapsed,s2 and a4", names(got.Deleted), names(got.Refused))
	}
	h, _, _ := c.ReadHead(ctx, v)
	var left []string
	for n := range h.Snapshots {
		left = append(left, n)
	}
	if names(left) != "a1,a2,a4,hold,pin,s1" {
		t.Fatalf("left %s", names(left))
	}
}

// The policy write is conditional on the generation read: of two writers
// that read the same generation, the second is refused.
func TestBCatalog_RetentionPolicyIsConditional(t *testing.T) {
	ctx := context.Background()
	c, err := NewBCatalog(newFakeFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v")
	if _, err := c.CreateVolume(ctx, v, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	s, _ := c.Store(v)
	_, gen, err := s.ReadRetention(ctx)
	if err != nil || gen != 0 {
		t.Fatalf("unset policy: gen %d %v", gen, err)
	}
	if err := s.WriteRetention(ctx, gen, retain.Policy{KeepNewest: 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteRetention(ctx, gen, retain.Policy{KeepNewest: 4}); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("a second writer at the same generation: %v; want ErrPrecondition", err)
	}
	if p, gen2, err := s.ReadRetention(ctx); err != nil || p.KeepNewest != 3 || gen2 != 1 {
		t.Fatalf("policy %+v gen %d %v", p, gen2, err)
	}
	if err := s.WriteRetention(ctx, 1, retain.Policy{KeepNewest: -1}); !errors.Is(err, retain.ErrInvalidPolicy) {
		t.Fatalf("negative policy: %v", err)
	}
	if err := c.SetRetention(ctx, newRef(t, "never"), retain.DefaultPolicy()); !errors.Is(err, layout.ErrHeadAbsent) {
		t.Fatalf("policy of a volume never created: %v", err)
	}
}
