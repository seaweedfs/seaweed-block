package chunkstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Snapshot retention and space accounting: policy edges, dry run, forks,
// accounting.

var retBase = time.Unix(1_000_000, 0)

func snapAt(name string, at time.Time, class string, expiresAt time.Time, labels map[string]string) SnapshotInfo {
	s := SnapshotInfo{Name: name, CreatedNs: at.UnixNano(), Class: class, Labels: labels}
	if !expiresAt.IsZero() {
		s.ExpiresAtNs = expiresAt.UnixNano()
	}
	return s
}

func kept(ds []RetentionDecision) map[string][]string {
	out := map[string][]string{}
	for _, d := range ds {
		if d.Keep {
			out[d.Name] = d.Reasons
		}
	}
	return out
}

func noYoung() RetentionPolicy { return RetentionPolicy{KeepNewest: 10} }

// Newest N unpinned are kept; equal creation times rank by name.
func TestPlanRetention_NewestNWithNameTieBreak(t *testing.T) {
	now := retBase.Add(48 * time.Hour)
	var snaps []SnapshotInfo
	for i := 0; i < 15; i++ {
		snaps = append(snaps, snapAt(fmt.Sprintf("s%02d", i), retBase.Add(time.Duration(i)*time.Minute), SnapshotClassSemantic, time.Time{}, nil))
	}
	k := kept(planRetention(noYoung(), snaps, now))
	if len(k) != 10 {
		t.Fatalf("kept %d, want 10: %v", len(k), k)
	}
	for i := 5; i < 15; i++ {
		if _, ok := k[fmt.Sprintf("s%02d", i)]; !ok {
			t.Fatalf("newest s%02d not kept: %v", i, k)
		}
	}
	tie := []SnapshotInfo{snapAt("b", retBase, "", time.Time{}, nil), snapAt("a", retBase, "", time.Time{}, nil)}
	if k := kept(planRetention(RetentionPolicy{KeepNewest: 1}, tie, now)); len(k) != 1 || k["a"] == nil {
		t.Fatalf("tie: kept %v, want only a (name order)", k)
	}
}

// Pinned: never expired by count or age; past its ExpiresAtNs it is
// unpinned again. Semantic and labeled snapshots are not exempt.
func TestPlanRetention_PinnedSemanticAndLabels(t *testing.T) {
	now := retBase.Add(48 * time.Hour)
	snaps := []SnapshotInfo{
		snapAt("pinned-old", retBase, SnapshotClassPinned, time.Time{}, nil),
		snapAt("pinned-by-expiry", retBase, "", now.Add(time.Hour), nil),
		snapAt("pinned-expired", retBase, SnapshotClassPinned, now.Add(-time.Second), nil),
		snapAt("semantic-old", retBase, SnapshotClassSemantic, time.Time{}, nil),
		snapAt("labeled-old", retBase, "", time.Time{}, map[string]string{"agent.step": "npm-ci"}),
	}
	k := kept(planRetention(RetentionPolicy{}, snaps, now))
	if len(k) != 2 || k["pinned-old"] == nil || k["pinned-by-expiry"] == nil {
		t.Fatalf("kept %v; want exactly pinned-old and pinned-by-expiry", k)
	}
}

// Younger than T is kept beyond N; a creation time in the future (clock
// skew) counts as young.
func TestPlanRetention_YoungAndClockSkew(t *testing.T) {
	now := retBase.Add(48 * time.Hour)
	p := RetentionPolicy{KeepNewest: 0, KeepYoungerNs: int64(time.Hour)}
	snaps := []SnapshotInfo{
		snapAt("young", now.Add(-30*time.Minute), SnapshotClassSemantic, time.Time{}, nil),
		snapAt("future", now.Add(10*time.Minute), SnapshotClassSemantic, time.Time{}, nil),
		snapAt("old", now.Add(-2*time.Hour), SnapshotClassSemantic, time.Time{}, nil),
	}
	k := kept(planRetention(p, snaps, now))
	if len(k) != 2 || k["young"] == nil || k["future"] == nil {
		t.Fatalf("kept %v; want young and future", k)
	}
}

// Log-spaced keeps: the newest snapshot of each of the last H hours.
func TestPlanRetention_HourlyBuckets(t *testing.T) {
	now := retBase.Add(48*time.Hour + 59*time.Minute) // late in an hour
	var snaps []SnapshotInfo
	for i := 0; i < 12; i++ { // every 30 min over the last 6 hours
		snaps = append(snaps, snapAt(fmt.Sprintf("s%02d", i), now.Add(-time.Duration(i)*30*time.Minute), SnapshotClassSemantic, time.Time{}, nil))
	}
	k := kept(planRetention(RetentionPolicy{KeepHourly: 3}, snaps, now))
	if len(k) != 3 {
		t.Fatalf("kept %d, want 3 (one per hour for the last 3 hours): %v", len(k), k)
	}
	for _, reasons := range k {
		if len(reasons) != 1 || reasons[0] != keepHourly {
			t.Fatalf("reasons %v; want hourly", reasons)
		}
	}
}

// An agent that checkpoints every step stays bounded: one semantic
// snapshot a minute for a day keeps at most N + T/step + hourly buckets.
func TestPlanRetention_AgentCheckpointingEveryStepStaysBounded(t *testing.T) {
	p := RetentionPolicy{KeepNewest: DefaultKeepNewest, KeepYoungerNs: int64(DefaultKeepYounger), KeepHourly: 24}
	start := retBase
	var snaps []SnapshotInfo
	for i := 0; i < 24*60; i++ {
		snaps = append(snaps, snapAt(fmt.Sprintf("step-%05d", i), start.Add(time.Duration(i)*time.Minute), SnapshotClassSemantic, time.Time{}, map[string]string{"agent.step": fmt.Sprint(i)}))
	}
	now := start.Add(24 * time.Hour)
	bound := p.KeepNewest + int(DefaultKeepYounger/time.Minute) + p.KeepHourly
	if k := kept(planRetention(p, snaps, now)); len(k) > bound {
		t.Fatalf("kept %d of %d step checkpoints; want at most %d", len(k), len(snaps), bound)
	}
	// The bound holds however long the agent runs.
	for i := 24 * 60; i < 7*24*60; i++ {
		snaps = append(snaps, snapAt(fmt.Sprintf("step-%05d", i), start.Add(time.Duration(i)*time.Minute), SnapshotClassSemantic, time.Time{}, nil))
	}
	if k := kept(planRetention(p, snaps, start.Add(7*24*time.Hour))); len(k) > bound {
		t.Fatalf("after a week: kept %d; want at most %d", len(k), bound)
	}
}

// retentionFixture makes n semantic snapshots of v1, one a minute, two
// days before the catalog clock's now.
func retentionFixture(t *testing.T, n int) (*fakeFiler, *Catalog, storage.ChunkHead, *time.Time) {
	t.Helper()
	f, _, c, h := catalogFixture(t)
	clock := retBase
	c.SetClock(func() time.Time { return clock })
	for i := 0; i < n; i++ {
		clock = retBase.Add(time.Duration(i) * time.Minute)
		if _, err := c.CreateSnapshot(context.Background(), "v1", SnapshotRequest{Name: fmt.Sprintf("s%02d", i), RequestID: fmt.Sprintf("r%02d", i), Class: SnapshotClassSemantic}, h); err != nil {
			t.Fatal(err)
		}
	}
	clock = retBase.Add(48 * time.Hour)
	return f, c, h, &clock
}

func snapNames(t *testing.T, c *Catalog) map[string]bool {
	t.Helper()
	snaps, err := c.allSnapshots(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, s := range snaps {
		out[s.Name] = true
	}
	return out
}

// A dry run lists exactly what would go and writes nothing.
func TestCatalog_RetainDryRunWritesNothing(t *testing.T) {
	f, c, _, _ := retentionFixture(t, 15)
	before := len(f.allWritten())
	res, err := c.Retain(context.Background(), "v1", true)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("dry run wrote %d entries", got-before)
	}
	var expired []string
	for _, d := range res.Decisions {
		if !d.Keep {
			expired = append(expired, d.Name)
		}
	}
	if res.Expired != 5 || res.Kept != 10 || len(res.Deleted) != 0 || strings.Join(expired, ",") != "s04,s03,s02,s01,s00" {
		t.Fatalf("dry run: expired=%d kept=%d deleted=%v names=%v; want the 5 oldest listed, none deleted", res.Expired, res.Kept, res.Deleted, expired)
	}
	if len(snapNames(t, c)) != 15 {
		t.Fatal("dry run removed snapshots")
	}
}

// Retain deletes the expired snapshots, keeps N plus the pinned one, and
// frees the expired names.
func TestCatalog_RetainKeepsNewestNAndPinned(t *testing.T) {
	_, c, h, clock := retentionFixture(t, 15)
	ctx := context.Background()
	saved := *clock
	*clock = retBase.Add(-time.Hour) // the oldest snapshot of all, but pinned
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "golden", RequestID: "rg", Class: SnapshotClassPinned}, h); err != nil {
		t.Fatal(err)
	}
	*clock = saved
	res, err := c.Retain(ctx, "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	names := snapNames(t, c)
	if len(res.Deleted) != 5 || len(names) != 11 || !names["golden"] || names["s00"] || !names["s14"] {
		t.Fatalf("after retain: deleted=%v remaining=%v; want 5 deleted, s05..s14 and golden left", res.Deleted, names)
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s00", RequestID: "reuse"}, h); err != nil {
		t.Fatalf("expired name not freed: %v", err)
	}
}

// The policy lives on the VOLUME entry: default until set; set stores it
// without touching the volume document; negatives are refused.
func TestCatalog_RetentionPolicyStoredOnVolume(t *testing.T) {
	_, _, c, _ := catalogFixture(t)
	ctx := context.Background()
	p, set, err := c.ReadRetentionPolicy(ctx, "v1")
	if err != nil || set || p != DefaultRetentionPolicy() {
		t.Fatalf("unset policy: %+v set=%t err=%v; want the default", p, set, err)
	}
	want := RetentionPolicy{KeepNewest: 3, KeepYoungerNs: int64(time.Minute), KeepDaily: 7}
	if err := c.SetRetentionPolicy(ctx, "v1", want); err != nil {
		t.Fatal(err)
	}
	if p, set, err := c.ReadRetentionPolicy(ctx, "v1"); err != nil || !set || p != want {
		t.Fatalf("stored policy: %+v set=%t err=%v", p, set, err)
	}
	if v, ok, err := c.ReadVolume(ctx, "v1"); err != nil || !ok || v.Geometry != catGeo {
		t.Fatalf("volume after setting a policy: %+v %t %v", v, ok, err)
	}
	if err := c.SetRetentionPolicy(ctx, "v1", RetentionPolicy{KeepNewest: -1}); CodeOf(err) != CodeInvalidPolicy {
		t.Fatalf("negative policy: %v; want invalid_policy", err)
	}
}

// Two setters racing on one revision: the second update's precondition
// fails.
func TestCatalog_RetentionPolicyConcurrentSetIsConditional(t *testing.T) {
	f, _, c, _ := catalogFixture(t)
	ctx := context.Background()
	f.failUpdate = func(p string) error {
		if p == path.Join(catBase, "v1", volumeName) {
			f.failUpdate = nil
			// another setter lands first
			e := f.entries[p]
			e.Extended[extRetentionRev] = []byte("7")
		}
		return nil
	}
	if err := c.SetRetentionPolicy(ctx, "v1", RetentionPolicy{KeepNewest: 1}); err == nil {
		t.Fatal("a set that lost the race succeeded")
	}
}

// The class is validated and stored with the snapshot.
func TestCatalog_SnapshotClass(t *testing.T) {
	_, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "x", RequestID: "rx", Class: "forever"}, h); CodeOf(err) != CodeInvalidClass {
		t.Fatalf("unknown class: %v; want invalid_class", err)
	}
	s, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "p", RequestID: "rp", Class: SnapshotClassPinned}, h)
	if err != nil || s.Class != SnapshotClassPinned || !s.Pinned() {
		t.Fatalf("pinned snapshot: %+v %v", s, err)
	}
	snaps, err := c.allSnapshots(ctx, "v1")
	if err != nil || len(snaps) != 1 || snaps[0].Class != SnapshotClassPinned {
		t.Fatalf("listed class: %+v %v", snaps, err)
	}
}

// publishTagged installs a HEAD for vol with chunks under keys tagged tag.
func publishTagged(t *testing.T, f *fakeFiler, b *fakeBlobs, vol, tag string, chunks map[uint64][]byte) storage.ChunkHead {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, vol))
	if err != nil {
		t.Fatal(err)
	}
	h := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: 43, Chunks: map[uint64]storage.ChunkRef{}}
	for idx, data := range chunks {
		key := fmt.Sprintf("chunks/%s/c%d/k", tag, idx)
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		h.Chunks[idx] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	}
	cur, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h, ids, err := r.PrepareManifest(ctx, cur, h, uint64(catGeo.BlockSize)*uint64(catGeo.ChunkBlocks))
	if err != nil {
		t.Fatal(err)
	}
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, h)
	if err != nil {
		t.Fatal(err)
	}
	h.Gen = gen
	for _, id := range ids {
		if err := r.DeleteIntent(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

// Accounting from manifests and listings: live bytes from HEAD; a
// snapshot's exclusive bytes are what only it references; stored bytes
// count every binding, exact for referenced keys.
func TestCatalog_UsageFromManifests(t *testing.T) {
	f, b, c, h := catalogFixture(t) // HEAD: chunks 0 and 3
	ctx := context.Background()
	full := uint64(catGeo.ChunkBlocks) * uint64(catGeo.BlockSize)
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	manifestBytes := func(head storage.ChunkHead) uint64 {
		var total uint64
		for _, ref := range head.Segments {
			body, err := r.Get(ctx, ref.Key)
			if err != nil {
				t.Fatal(err)
			}
			total += uint64(len(body))
		}
		return total
	}
	expectedManifestBytes := manifestBytes(h)
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err != nil {
		t.Fatal(err)
	}
	u, err := c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if u.LiveChunks != 2 || u.LiveBytes != 2*full || len(u.Snapshots) != 1 || u.Snapshots[0].ExclusiveBytes != 0 {
		t.Fatalf("shared chunks: %+v", u)
	}
	if u.StoredBindings != 3 || u.ManifestBindings != 1 || expectedManifestBytes == 0 || u.ManifestBytes != expectedManifestBytes || u.StoredBytes != 2*full+expectedManifestBytes || u.StoredUpperBound {
		t.Fatalf("stored: %+v; want 2 bindings, exact", u)
	}

	// HEAD moves on: chunk 0 is rewritten, so s1 alone holds the old one.
	newHead := publishTagged(t, f, b, "v1", "e2", map[uint64][]byte{0: chunkBytes(0x20), 3: chunkBytes(0x13)})
	expectedManifestBytes += manifestBytes(newHead)
	u, err = c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if u.Snapshots[0].ExclusiveBytes != 2*full {
		// both of s1's chunks are now exclusive: the new HEAD uses e2 keys
		t.Fatalf("after HEAD moved: s1 exclusive %d, want %d", u.Snapshots[0].ExclusiveBytes, 2*full)
	}
	if u.StoredBindings != 6 || u.ManifestBindings != 2 || u.ManifestBytes != expectedManifestBytes || u.StoredBytes != 4*full+expectedManifestBytes || u.StoredUpperBound {
		t.Fatalf("stored after HEAD moved: %+v; want 4 bindings, exact", u)
	}

	// s1 deleted: its keys are unreferenced but still stored (until GC),
	// counted at the full chunk size as an upper bound.
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	u, err = c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(u.Snapshots) != 0 || u.StoredBindings != 6 || !u.StoredUpperBound {
		t.Fatalf("after delete: %+v; want no snapshots, 4 bindings, upper bound", u)
	}
}

// chunkSize clips the last chunk to NumBlocks.
func TestChunkSizeClipsLastChunk(t *testing.T) {
	g := Geometry{NumBlocks: 20, BlockSize: 4096, ChunkBlocks: 8}
	for idx, want := range map[uint64]uint64{0: 8 * 4096, 1: 8 * 4096, 2: 4 * 4096, 3: 0} {
		if got := chunkSize(g, idx); got != want {
			t.Errorf("chunk %d: %d, want %d", idx, got, want)
		}
	}
}

// Scheduled snapshots form their own class: retention keeps the newest K
// of them, K from the newest one's LabelScheduleKeep, and they neither
// count against KeepNewest nor get the age keeps; other snapshots are
// planned as before.
func TestPlanRetention_ScheduledClassKeepsItsOwnNewest(t *testing.T) {
	now := retBase.Add(30 * 24 * time.Hour)
	keep3 := map[string]string{LabelScheduleKeep: "3"}
	var snaps []SnapshotInfo
	for d := 0; d < 6; d++ { // one scheduled snapshot a day, the newest first at now-1h
		snaps = append(snaps, snapAt(fmt.Sprintf("sched-%d", d), now.Add(-time.Hour-time.Duration(d)*24*time.Hour), SnapshotClassScheduled, time.Time{}, keep3))
	}
	snaps = append(snaps,
		snapAt("manual-new", now.Add(-2*time.Hour), "", time.Time{}, nil),
		snapAt("manual-old", now.Add(-20*24*time.Hour), "", time.Time{}, nil))
	k := kept(planRetention(RetentionPolicy{KeepNewest: 2, KeepYoungerNs: int64(30 * 24 * time.Hour)}, snaps, now))
	for _, name := range []string{"sched-0", "sched-1", "sched-2", "manual-new", "manual-old"} {
		if k[name] == nil {
			t.Fatalf("kept %v; %s missing", k, name)
		}
	}
	if len(k) != 5 {
		t.Fatalf("kept %v; want the 3 newest scheduled and both manual (KeepNewest 2 is theirs alone)", k)
	}
	// Without a valid label the default count applies.
	snaps[0].Labels = nil
	if k := kept(planRetention(RetentionPolicy{}, snaps[:6], now)); len(k) != 6 {
		t.Fatalf("kept %v with no label on the newest; want min(6, DefaultScheduleKeep)", k)
	}
}
