package chunkstore

import (
	"context"
	"strings"
	"time"
)

// A scheduled snapshot is named, and requested, for the slot it was due
// for: "sched-" and the slot's start, UTC to the second. Every attempt for
// one slot (a retry, a restart, another holder) uses that one name, so
// they converge on one snapshot. The slot in the name is the scheduled
// snapshot's time: the schedule decides from the newest one, and retention
// ranks the class by it.
const (
	scheduledNamePrefix = "sched-"
	scheduledNameLayout = "20060102T150405Z"
)

// ScheduledSnapshotName is the name, and request id, of the scheduled
// snapshot for the slot starting at slot.
func ScheduledSnapshotName(slot time.Time) string {
	return scheduledNamePrefix + slot.UTC().Format(scheduledNameLayout)
}

// ScheduledSlot returns the slot s was taken for: the time in its name, for
// a snapshot of class "scheduled" named by ScheduledSnapshotName. ok is
// false for any other snapshot.
func ScheduledSlot(s SnapshotInfo) (time.Time, bool) {
	if s.Class != SnapshotClassScheduled || !strings.HasPrefix(s.Name, scheduledNamePrefix) {
		return time.Time{}, false
	}
	t, err := time.Parse(scheduledNameLayout, strings.TrimPrefix(s.Name, scheduledNamePrefix))
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// NewestScheduledSlot returns the slot of vol's newest published scheduled
// snapshot; zero when it has none. A snapshot being deleted, or one not
// published, does not count.
func (c *Catalog) NewestScheduledSlot(ctx context.Context, vol string) (time.Time, error) {
	snaps, err := c.allSnapshots(ctx, vol)
	if err != nil {
		return time.Time{}, err
	}
	var newest time.Time
	for _, s := range snaps {
		if t, ok := ScheduledSlot(s); ok && t.After(newest) {
			newest = t
		}
	}
	return newest, nil
}
