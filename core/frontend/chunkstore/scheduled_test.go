package chunkstore

import (
	"context"
	"testing"
	"time"
)

// A scheduled snapshot's slot is read from its name, to the second, and
// only for class "scheduled" with that name.
func TestScheduledSlot_ReadsOnlyAScheduledName(t *testing.T) {
	slot := time.Date(2026, 11, 24, 3, 4, 5, 600_000_000, time.UTC)
	name := ScheduledSnapshotName(slot)
	if name != "sched-20261124T030405Z" {
		t.Fatalf("name %q", name)
	}
	if got, ok := ScheduledSlot(SnapshotInfo{Name: name, Class: SnapshotClassScheduled}); !ok || !got.Equal(slot.Truncate(time.Second)) {
		t.Fatalf("slot of %s: %v %v; want %v", name, got, ok, slot.Truncate(time.Second))
	}
	for _, s := range []SnapshotInfo{
		{Name: name, Class: SnapshotClassSemantic},
		{Name: "sched-yesterday", Class: SnapshotClassScheduled},
		{Name: "nightly", Class: SnapshotClassScheduled},
	} {
		if _, ok := ScheduledSlot(s); ok {
			t.Fatalf("%+v read as a scheduled slot", s)
		}
	}
}

// The inverted pair: an old holder's cut, committed before a takeover,
// publishes the LATER slot at an OLDER generation, while the new holder
// publishes the earlier slot at a newer generation. "Newer" is the
// generation, so keep 1 keeps the newer generation; the later slot holds
// the schedule's coverage and is kept too, so the schedule does not retake
// it. Creation times (publisher clocks) order nothing.
func TestPlanRetention_ScheduledInvertedPairKeepsNewerGenerationAndCoverage(t *testing.T) {
	now := retBase.Add(10 * 24 * time.Hour)
	day1, day2 := now.Add(-48*time.Hour), now.Add(-24*time.Hour)
	keep1 := map[string]string{LabelScheduleKeep: "1"}
	older := SnapshotInfo{Name: ScheduledSnapshotName(day2), Class: SnapshotClassScheduled, HeadGen: 5, CreatedNs: day2.Add(2 * time.Hour).UnixNano(), Labels: keep1}
	newer := SnapshotInfo{Name: ScheduledSnapshotName(day1), Class: SnapshotClassScheduled, HeadGen: 7, CreatedNs: day1.UnixNano(), Labels: keep1}
	third := SnapshotInfo{Name: ScheduledSnapshotName(day1.Add(-24 * time.Hour)), Class: SnapshotClassScheduled, HeadGen: 3, CreatedNs: day2.Add(3 * time.Hour).UnixNano(), Labels: keep1}
	k := kept(planRetention(RetentionPolicy{}, []SnapshotInfo{older, newer, third}, now))
	if len(k) != 2 || k[newer.Name] == nil || k[older.Name] == nil {
		t.Fatalf("kept %v; want %s (newest generation) and %s (coverage), not %s", k, newer.Name, older.Name, third.Name)
	}
	if !contains(k[older.Name], keepCovered) || contains(k[older.Name], keepScheduled) {
		t.Fatalf("%s kept for %v; want the coverage reason only", older.Name, k[older.Name])
	}
}

// For every class, "newer" is the generation, not the creation time: a
// manual snapshot of a later cut is the newest even when its publisher's
// clock was behind.
func TestPlanRetention_NewestIsTheHigherGeneration(t *testing.T) {
	now := retBase.Add(time.Hour)
	a := SnapshotInfo{Name: "a", HeadGen: 9, CreatedNs: retBase.UnixNano()}
	b := SnapshotInfo{Name: "b", HeadGen: 4, CreatedNs: retBase.Add(30 * time.Minute).UnixNano()}
	k := kept(planRetention(RetentionPolicy{KeepNewest: 1}, []SnapshotInfo{b, a}, now))
	if len(k) != 1 || k["a"] == nil {
		t.Fatalf("kept %v; want a (generation 9) over b (generation 4, created later)", k)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// The newest scheduled slot counts only published snapshots: a newest
// marked deleting (its name not yet freed) does not count, nor does it once
// deleted; the one before it is then the newest. None at all is zero.
func TestCatalog_NewestScheduledSlotIgnoresADeletedNewest(t *testing.T) {
	_, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if got, err := c.NewestScheduledSlot(ctx, "v1"); err != nil || !got.IsZero() {
		t.Fatalf("no snapshots: %v %v; want zero", got, err)
	}
	day1 := time.Date(2026, 11, 22, 3, 4, 5, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	for _, slot := range []time.Time{day1, day2} {
		name := ScheduledSnapshotName(slot)
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: name, RequestID: name, Class: SnapshotClassScheduled}, h); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "sched-manual", RequestID: "m", Class: SnapshotClassScheduled}, h); err != nil {
		t.Fatal(err)
	}
	if got, err := c.NewestScheduledSlot(ctx, "v1"); err != nil || !got.Equal(day2) {
		t.Fatalf("newest: %v %v; want %v", got, err, day2)
	}
	var marked time.Time
	var markedErr error
	seen := false
	c.retainHook = func(stage, name string) {
		if stage == hookMarked && name == ScheduledSnapshotName(day2) {
			seen = true
			marked, markedErr = c.NewestScheduledSlot(ctx, "v1")
		}
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", ScheduledSnapshotName(day2)); err != nil {
		t.Fatal(err)
	}
	c.retainHook = nil
	if !seen || markedErr != nil || !marked.Equal(day1) {
		t.Fatalf("while the newest is marked deleting (hook reached %v): %v %v; want %v", seen, marked, markedErr, day1)
	}
	if got, err := c.NewestScheduledSlot(ctx, "v1"); err != nil || !got.Equal(day1) {
		t.Fatalf("after deleting the newest: %v %v; want %v", got, err, day1)
	}
}
