// Package retain chooses which snapshots a retention policy deletes. It
// only selects names: the delete is the layout's snapshot transition for
// the automatic caller, which refuses a pinned snapshot or one a restore
// target depends on. Times are wall clock and decide policy only.
package retain

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// The retention classes and the schedule label.
const (
	ClassSemantic  = "semantic"
	ClassPinned    = "pinned"
	ClassScheduled = "scheduled"

	// LabelScheduleKeep, on a scheduled snapshot, is how many scheduled
	// snapshots its schedule keeps (a decimal count).
	LabelScheduleKeep = "swb.schedule.keep"
	// DefaultScheduleKeep is the count kept when the newest scheduled
	// snapshot carries no valid LabelScheduleKeep.
	DefaultScheduleKeep = 7

	DefaultKeepNewest  = 10
	DefaultKeepYounger = time.Hour
)

// ErrInvalidPolicy refuses a negative count or duration.
var ErrInvalidPolicy = errors.New("retain: invalid policy")

// Policy is a volume's snapshot retention policy.
type Policy struct {
	KeepNewest    int   `json:"keepNewest"`    // keep the newest N unpinned snapshots
	KeepYoungerNs int64 `json:"keepYoungerNs"` // keep every snapshot younger than this (ns)
	KeepHourly    int   `json:"keepHourly"`    // keep the newest snapshot of each of the last N hours
	KeepDaily     int   `json:"keepDaily"`     // keep the newest snapshot of each of the last N days
	KeepWeekly    int   `json:"keepWeekly"`    // keep the newest snapshot of each of the last N weeks
}

// DefaultPolicy is the policy of a volume that never set one.
func DefaultPolicy() Policy {
	return Policy{KeepNewest: DefaultKeepNewest, KeepYoungerNs: int64(DefaultKeepYounger)}
}

// Validate refuses negative counts and durations.
func (p Policy) Validate() error {
	if p.KeepNewest < 0 || p.KeepYoungerNs < 0 || p.KeepHourly < 0 || p.KeepDaily < 0 || p.KeepWeekly < 0 {
		return fmt.Errorf("%w: %+v: counts and durations must not be negative", ErrInvalidPolicy, p)
	}
	return nil
}

// Snapshot is one named snapshot as retention sees it.
type Snapshot struct {
	Name      string
	CreatedNs int64
	Class     string
	ExpiresNs int64
	Labels    map[string]string
}

// Pinned reports whether the snapshot is an explicit keep.
func (s Snapshot) Pinned() bool {
	return s.Class == ClassPinned || s.ExpiresNs > 0
}

// Decision is one snapshot's fate under a policy.
type Decision struct {
	Name      string   `json:"name"`
	CreatedNs int64    `json:"createdNs"`
	Class     string   `json:"class"`
	Pinned    bool     `json:"pinned"`
	Keep      bool     `json:"keep"`
	Reasons   []string `json:"reasons"` // why it is kept; "expired" when it goes
}

// The reasons a snapshot is kept.
const (
	keepNewest    = "newest"
	keepPinned    = "pinned"
	keepYoung     = "younger_than_policy"
	keepHourly    = "hourly"
	keepDaily     = "daily"
	keepWeekly    = "weekly"
	keepScheduled = "scheduled_newest"
	reasonExpired = "expired"
)

// Plan decides, from one listing, which snapshots policy keeps at now.
// Unpinned snapshots are ranked newest first; equal creation times rank by
// name, so the plan is deterministic.
func Plan(p Policy, snaps []Snapshot, now time.Time) []Decision {
	ordered := append([]Snapshot(nil), snaps...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].CreatedNs != ordered[j].CreatedNs {
			return ordered[i].CreatedNs > ordered[j].CreatedNs
		}
		return ordered[i].Name < ordered[j].Name
	})
	nowNs := now.UnixNano()
	out := make([]Decision, len(ordered))
	unpinned, scheduled := 0, 0
	scheduleKeep := -1 // read from the newest scheduled snapshot
	windows := map[string]bool{}
	for i, s := range ordered {
		pinned := s.Pinned() && (s.ExpiresNs == 0 || nowNs < s.ExpiresNs)
		d := Decision{Name: s.Name, CreatedNs: s.CreatedNs, Class: s.Class, Pinned: pinned}
		if s.Class == ClassScheduled {
			// Every scheduled snapshot, pinned or not, is planned here and
			// ranked among its class; a pinned one is also kept by its pin.
			if scheduleKeep < 0 {
				scheduleKeep = scheduleKeepOf(s)
			}
			if pinned {
				d.Reasons = append(d.Reasons, keepPinned)
			}
			if scheduled < scheduleKeep {
				d.Reasons = append(d.Reasons, keepScheduled)
			}
			scheduled++
			d.Keep = len(d.Reasons) > 0
			if !d.Keep {
				d.Reasons = []string{reasonExpired}
			}
			out[i] = d
			continue
		}
		if pinned {
			d.Reasons = append(d.Reasons, keepPinned)
		} else {
			if unpinned < p.KeepNewest {
				d.Reasons = append(d.Reasons, keepNewest)
			}
			unpinned++
		}
		age := time.Duration(nowNs - s.CreatedNs)
		if age < time.Duration(p.KeepYoungerNs) {
			d.Reasons = append(d.Reasons, keepYoung)
		}
		for _, lk := range []struct {
			reason string
			period time.Duration
			count  int
		}{
			{keepHourly, time.Hour, p.KeepHourly},
			{keepDaily, 24 * time.Hour, p.KeepDaily},
			{keepWeekly, 7 * 24 * time.Hour, p.KeepWeekly},
		} {
			if lk.count <= 0 || age < 0 {
				continue
			}
			k := int64(age / lk.period) // the rolling window holding this age
			if k >= int64(lk.count) {
				continue
			}
			key := fmt.Sprintf("%s/%d", lk.reason, k)
			if !windows[key] { // ordered newest first: the first seen is the window's newest
				windows[key] = true
				d.Reasons = append(d.Reasons, lk.reason)
			}
		}
		d.Keep = len(d.Reasons) > 0
		if !d.Keep {
			d.Reasons = []string{reasonExpired}
		}
		out[i] = d
	}
	return out
}

// scheduleKeepOf is the count a scheduled snapshot's schedule keeps.
func scheduleKeepOf(s Snapshot) int {
	if n, err := strconv.Atoi(s.Labels[LabelScheduleKeep]); err == nil && n > 0 {
		return n
	}
	return DefaultScheduleKeep
}
