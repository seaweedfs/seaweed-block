package chunkstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// Controls for the review of #142: attempt-bound deletion, fork holds,
// rolling windows, cross-volume holders and partial results.

// Log-spaced keeps use rolling windows anchored at now, not calendar
// periods: at 10:30 with KeepHourly=1, 10:20 is kept and 09:50 (another
// clock hour, but inside the same rolling hour) is not.
func TestPlanRetention_RollingWindows(t *testing.T) {
	day0 := time.Unix(86400*1000, 0) // midnight, so clock hours and days are visible
	at := func(d time.Duration) time.Time { return day0.Add(d) }
	nm := func(a time.Time) string { return a.UTC().Format("01-02T15:04") }
	plan := func(p RetentionPolicy, now time.Time, ages ...time.Time) map[string][]string {
		var snaps []SnapshotInfo
		for _, a := range ages {
			snaps = append(snaps, snapAt(nm(a), a, SnapshotClassSemantic, time.Time{}, nil))
		}
		return kept(planRetention(p, snaps, nil, now))
	}
	now := at(10*time.Hour + 30*time.Minute)
	if k := plan(RetentionPolicy{KeepHourly: 1}, now, at(10*time.Hour+20*time.Minute), at(9*time.Hour+50*time.Minute)); len(k) != 1 || k[nm(at(10*time.Hour+20*time.Minute))] == nil {
		t.Fatalf("hourly at 10:30: kept %v; want only 10:20", k)
	}
	// Windows k=0,1: [09:30,10:30) and [08:30,09:30). 09:00 is the newest of window 1.
	if k := plan(RetentionPolicy{KeepHourly: 2}, now, at(10*time.Hour+20*time.Minute), at(9*time.Hour+50*time.Minute), at(9*time.Hour), at(8*time.Hour+40*time.Minute)); len(k) != 2 || k[nm(at(10*time.Hour+20*time.Minute))] == nil || k[nm(at(9*time.Hour))] == nil {
		t.Fatalf("hourly=2 at 10:30: kept %v; want 10:20 and 09:00", k)
	}
	// Daily across a non-aligned midnight: at 01:00, 00:30 and 23:00 the
	// day before are both in the last rolling day; only the newest stays.
	now = at(24*time.Hour + time.Hour)
	if k := plan(RetentionPolicy{KeepDaily: 1}, now, at(24*time.Hour+30*time.Minute), at(23*time.Hour)); len(k) != 1 || k[nm(at(24*time.Hour+30*time.Minute))] == nil {
		t.Fatalf("daily at 01:00: kept %v; want only 00:30", k)
	}
	// Weekly: 8 days back is week window 1, 6 days back window 0.
	if k := plan(RetentionPolicy{KeepWeekly: 1}, now, now.Add(-6*24*time.Hour), now.Add(-8*24*time.Hour)); len(k) != 1 {
		t.Fatalf("weekly=1: kept %v; want only the 6-day-old one", k)
	}
	if k := plan(RetentionPolicy{KeepWeekly: 2}, now, now.Add(-6*24*time.Hour), now.Add(-8*24*time.Hour), now.Add(-13*24*time.Hour)); len(k) != 2 {
		t.Fatalf("weekly=2: kept %v; want 6 and 8 days old", k)
	}
}

// Deletion is bound to the planned attempt: a same-name pinned replacement
// made between planning and deletion survives, attempt and name intact.
func TestCatalog_RetainSkipsASameNameReplacement(t *testing.T) {
	_, c, h, _ := retentionFixture(t, 15) // s00..s04 expire
	ctx := context.Background()
	var newAttempt string
	c.retainHook = func(stage, _ string) {
		if stage != hookPlanned {
			return
		}
		if _, err := c.DeleteSnapshot(ctx, "v1", "s02"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s02", RequestID: "pinned-b", Class: SnapshotClassPinned}, h); err != nil {
			t.Fatal(err)
		}
		b, err := c.readSnap(ctx, "v1", "s02")
		if err != nil {
			t.Fatal(err)
		}
		newAttempt = b.attempt
	}
	res, err := c.Retain(ctx, "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.readSnap(ctx, "v1", "s02")
	if err != nil {
		t.Fatalf("the pinned replacement is gone: %v", err)
	}
	if b.attempt != newAttempt || b.State != SnapActive || b.Class != SnapshotClassPinned {
		t.Fatalf("replacement changed: %+v (attempt %s, want %s)", b, b.attempt, newAttempt)
	}
	if strings.Join(res.Replaced, ",") != "s02" || strings.Join(res.Deleted, ",") != "s04,s03,s01,s00" {
		t.Fatalf("deleted=%v replaced=%v; want s02 replaced, the other four deleted", res.Deleted, res.Replaced)
	}
}

func forkFixture(t *testing.T) (*fakeFiler, *Catalog) {
	t.Helper()
	f, c, _, _ := retentionFixture(t, 3)
	ctx := context.Background()
	if err := c.SetRetentionPolicy(ctx, "v1", RetentionPolicy{}); err != nil { // keep nothing by policy
		t.Fatal(err)
	}
	for _, vol := range []string{"v1", "v2"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	return f, c
}

// A fork that starts after the retention scan (its VOLUME published, its
// HEAD not yet) holds the parent: the mark is reverted, the parent kept,
// and the fork completes on retry.
func TestCatalog_RetainKeepsAParentForkedAfterTheScan(t *testing.T) {
	f, c := forkFixture(t)
	ctx := context.Background()
	c.retainHook = func(stage, _ string) {
		if stage != hookPlanned {
			return
		}
		holdFirst(f, catBase+"/v2/HEAD")
		if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s01", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeOutcomeUnknown {
			t.Fatalf("fork with a held HEAD create: %v", err)
		}
	}
	res, err := c.Retain(ctx, "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(res.ForkHeld, ",") != "s01" || strings.Join(res.Deleted, ",") != "s02,s00" {
		t.Fatalf("deleted=%v forkHeld=%v; want s01 kept for the fork", res.Deleted, res.ForkHeld)
	}
	if s, err := c.readSnap(ctx, "v1", "s01"); err != nil || s.State != SnapActive {
		t.Fatalf("parent after retain: %+v %v; want active", s, err)
	}
	c.retainHook = nil
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s01", Target: "v2", RequestID: "f"}); err != nil {
		t.Fatalf("fork retry: %v", err)
	}
	// Completed: the hold is gone and the parent is no longer needed.
	res, err = c.Retain(ctx, "v1", false)
	if err != nil || strings.Join(res.Deleted, ",") != "s01" {
		t.Fatalf("after the fork completed: deleted=%v %v; want s01", res.Deleted, err)
	}
}

// A fork that takes its hold after retention checked the holds finds the
// attempt deleting and refuses before installing anything.
func TestCatalog_ForkAfterTheHoldCheckRefuses(t *testing.T) {
	f, c := forkFixture(t)
	ctx := context.Background()
	ran := false
	c.retainHook = func(stage, name string) {
		if stage != hookUnheld || name != "s01" {
			return
		}
		ran = true
		if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s01", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeSnapshotDeleting {
			t.Fatalf("fork of a marked parent: %v; want snapshot_deleting", err)
		}
	}
	res, err := c.Retain(ctx, "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	if !ran || strings.Join(res.Deleted, ",") != "s02,s01,s00" {
		t.Fatalf("ran=%v deleted=%v; want all three", ran, res.Deleted)
	}
	if _, ok, err := c.ReadVolume(ctx, "v2"); ok || err != nil {
		t.Fatalf("a refused fork published its VOLUME (%v)", err)
	}
	if holds, _ := f.ListEntries(ctx, forkHoldDir(catBase+"/v1", "s01", res.Decisions[1].Attempt), "", "", 0); len(holds) != 0 {
		t.Fatalf("a refused fork left %d holds", len(holds))
	}
}

// A hold whose fork died before publishing its VOLUME counts only within
// the operation deadline.
func TestCatalog_StaleForkHoldExpires(t *testing.T) {
	f, c := forkFixture(t)
	ctx := context.Background()
	s, err := c.readSnap(ctx, "v1", "s01")
	if err != nil {
		t.Fatal(err)
	}
	now := c.now()
	hold := func(target string, at time.Time) {
		e := &filerpb.Entry{Name: target, Attributes: &filerpb.FuseAttributes{Crtime: at.Unix(), Mtime: at.Unix()}}
		if err := f.CreateEntry(ctx, forkHoldDir(catBase+"/v1", "s01", s.attempt), e, true); err != nil {
			t.Fatal(err)
		}
	}
	hold("v8", now.Add(-time.Second)) // young: the fork may still publish
	res, err := c.Retain(ctx, "v1", false)
	if err != nil || strings.Join(res.ForkHeld, ",") != "s01" {
		t.Fatalf("young hold: forkHeld=%v %v; want s01", res.ForkHeld, err)
	}
	if err := f.DeleteEntry(ctx, forkHoldDir(catBase+"/v1", "s01", s.attempt), "v8", nil); err != nil {
		t.Fatal(err)
	}
	hold("v9", now.Add(-2*c.deadline())) // old, no VOLUME: the fork died
	res, err = c.Retain(ctx, "v1", false)
	if err != nil || strings.Join(res.Deleted, ",") != "s01" {
		t.Fatalf("stale hold: deleted=%v %v; want s01", res.Deleted, err)
	}
}

// A failure part way reports the confirmed deletions, the failing
// snapshot (uncertain: the mark's outcome is unknown) and what was not
// attempted; a lost response leaves the snapshot deleting and the next
// pass finishes it.
func TestCatalog_RetainReportsPartialProgress(t *testing.T) {
	f, c, _, _ := retentionFixture(t, 15) // s04,s03,s02,s01,s00 expire, in that order
	ctx := context.Background()
	f.failUpdate = func(p string) error {
		if strings.Contains(p, "/snaps/s03/") {
			return errors.New("filer unavailable")
		}
		return nil
	}
	res, err := c.Retain(ctx, "v1", false)
	if err == nil || res.Failed == nil {
		t.Fatalf("retain with a failing mark: %+v %v; want an error with the partial result", res, err)
	}
	if strings.Join(res.Deleted, ",") != "s04" || res.Failed.Name != "s03" || !res.Failed.Uncertain || strings.Join(res.NotAttempted, ",") != "s02,s01,s00" {
		t.Fatalf("partial: deleted=%v failed=%+v notAttempted=%v", res.Deleted, res.Failed, res.NotAttempted)
	}

	f.failUpdate = nil
	f.failUpdateAfterApply = func(p string) error {
		if strings.Contains(p, "/snaps/s03/") {
			return errors.New("response lost")
		}
		return nil
	}
	res, err = c.Retain(ctx, "v1", false)
	if err == nil || res.Failed == nil || res.Failed.Name != "s03" || !res.Failed.Uncertain || len(res.Deleted) != 0 {
		t.Fatalf("lost response: %+v %v", res, err)
	}
	f.failUpdateAfterApply = nil
	res, err = c.Retain(ctx, "v1", false)
	if err != nil || strings.Join(res.Deleted, ",") != "s02,s01,s00,s03" {
		t.Fatalf("rerun: deleted=%v %v; want the rest and the half-deleted s03", res.Deleted, err)
	}
	if _, ok, err := c.resolveSnap(ctx, "v1", "s03"); ok || err != nil {
		t.Fatalf("s03's name still held after the rerun (%v)", err)
	}
}

// Exclusive bytes count other volumes' holders: after a completed fork, a
// snapshot whose chunks the fork's HEAD references ("@v1/key") owns none.
func TestCatalog_UsageCountsForkHolders(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	for _, vol := range []string{"v1", "v2"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); err != nil {
		t.Fatal(err)
	}
	publishTagged(t, f, b, "v1", "e2", map[uint64][]byte{0: chunkBytes(0x20), 3: chunkBytes(0x13)})
	u, err := c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(u.Snapshots) != 1 || u.Snapshots[0].ExclusiveBytes != 0 {
		t.Fatalf("s1 after v1's HEAD moved, v2 forked from it: %+v; want 0 exclusive (v2 holds both chunks)", u.Snapshots)
	}
}
