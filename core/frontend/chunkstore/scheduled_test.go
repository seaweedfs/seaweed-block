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

// Retention ranks the scheduled class by slot, not by creation time: with
// keep 1, the newest slot survives even when an older slot was published
// later (a cut that committed late in its slot, or by an earlier holder).
func TestPlanRetention_ScheduledNewestBySlotNeverExpires(t *testing.T) {
	now := retBase.Add(10 * 24 * time.Hour)
	day1, day2 := now.Add(-48*time.Hour), now.Add(-24*time.Hour)
	keep1 := map[string]string{LabelScheduleKeep: "1"}
	snaps := []SnapshotInfo{
		snapAt(ScheduledSnapshotName(day2), day2.Add(time.Minute), SnapshotClassScheduled, time.Time{}, keep1),
		snapAt(ScheduledSnapshotName(day1), day2.Add(time.Hour), SnapshotClassScheduled, time.Time{}, keep1), // published after day2's
	}
	k := kept(planRetention(RetentionPolicy{}, snaps, now))
	if len(k) != 1 || k[ScheduledSnapshotName(day2)] == nil {
		t.Fatalf("kept %v; want only the newest slot %s", k, ScheduledSnapshotName(day2))
	}
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
