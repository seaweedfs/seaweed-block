package retain

import (
	"fmt"
	"testing"
	"time"
)

var retBase = time.Unix(1_000_000, 0)

func snapAt(name string, at time.Time, class string, expiresAt time.Time, labels map[string]string) Snapshot {
	s := Snapshot{Name: name, CreatedNs: at.UnixNano(), Class: class, Labels: labels}
	if !expiresAt.IsZero() {
		s.ExpiresNs = expiresAt.UnixNano()
	}
	return s
}

func kept(ds []Decision) map[string][]string {
	out := map[string][]string{}
	for _, d := range ds {
		if d.Keep {
			out[d.Name] = d.Reasons
		}
	}
	return out
}

func noYoung() Policy { return Policy{KeepNewest: 10} }

// Newest N unpinned are kept; equal creation times rank by name.
func TestPlanRetention_NewestNWithNameTieBreak(t *testing.T) {
	now := retBase.Add(48 * time.Hour)
	var snaps []Snapshot
	for i := 0; i < 15; i++ {
		snaps = append(snaps, snapAt(fmt.Sprintf("s%02d", i), retBase.Add(time.Duration(i)*time.Minute), ClassSemantic, time.Time{}, nil))
	}
	k := kept(Plan(noYoung(), snaps, now))
	if len(k) != 10 {
		t.Fatalf("kept %d, want 10: %v", len(k), k)
	}
	for i := 5; i < 15; i++ {
		if _, ok := k[fmt.Sprintf("s%02d", i)]; !ok {
			t.Fatalf("newest s%02d not kept: %v", i, k)
		}
	}
	tie := []Snapshot{snapAt("b", retBase, "", time.Time{}, nil), snapAt("a", retBase, "", time.Time{}, nil)}
	if k := kept(Plan(Policy{KeepNewest: 1}, tie, now)); len(k) != 1 || k["a"] == nil {
		t.Fatalf("tie: kept %v, want only a (name order)", k)
	}
}

// Pinned: never expired by count or age; past its ExpiresNs it is
// unpinned again. Semantic and labeled snapshots are not exempt.
func TestPlanRetention_PinnedSemanticAndLabels(t *testing.T) {
	now := retBase.Add(48 * time.Hour)
	snaps := []Snapshot{
		snapAt("pinned-old", retBase, ClassPinned, time.Time{}, nil),
		snapAt("pinned-by-expiry", retBase, "", now.Add(time.Hour), nil),
		snapAt("pinned-expired", retBase, ClassPinned, now.Add(-time.Second), nil),
		snapAt("semantic-old", retBase, ClassSemantic, time.Time{}, nil),
		snapAt("labeled-old", retBase, "", time.Time{}, map[string]string{"agent.step": "npm-ci"}),
	}
	k := kept(Plan(Policy{}, snaps, now))
	if len(k) != 2 || k["pinned-old"] == nil || k["pinned-by-expiry"] == nil {
		t.Fatalf("kept %v; want exactly pinned-old and pinned-by-expiry", k)
	}
}

// Younger than T is kept beyond N; a creation time in the future (clock
// skew) counts as young.
func TestPlanRetention_YoungAndClockSkew(t *testing.T) {
	now := retBase.Add(48 * time.Hour)
	p := Policy{KeepNewest: 0, KeepYoungerNs: int64(time.Hour)}
	snaps := []Snapshot{
		snapAt("young", now.Add(-30*time.Minute), ClassSemantic, time.Time{}, nil),
		snapAt("future", now.Add(10*time.Minute), ClassSemantic, time.Time{}, nil),
		snapAt("old", now.Add(-2*time.Hour), ClassSemantic, time.Time{}, nil),
	}
	k := kept(Plan(p, snaps, now))
	if len(k) != 2 || k["young"] == nil || k["future"] == nil {
		t.Fatalf("kept %v; want young and future", k)
	}
}

// Log-spaced keeps: the newest snapshot of each of the last H hours.
func TestPlanRetention_HourlyBuckets(t *testing.T) {
	now := retBase.Add(48*time.Hour + 59*time.Minute) // late in an hour
	var snaps []Snapshot
	for i := 0; i < 12; i++ { // every 30 min over the last 6 hours
		snaps = append(snaps, snapAt(fmt.Sprintf("s%02d", i), now.Add(-time.Duration(i)*30*time.Minute), ClassSemantic, time.Time{}, nil))
	}
	k := kept(Plan(Policy{KeepHourly: 3}, snaps, now))
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
	p := Policy{KeepNewest: DefaultKeepNewest, KeepYoungerNs: int64(DefaultKeepYounger), KeepHourly: 24}
	start := retBase
	var snaps []Snapshot
	for i := 0; i < 24*60; i++ {
		snaps = append(snaps, snapAt(fmt.Sprintf("step-%05d", i), start.Add(time.Duration(i)*time.Minute), ClassSemantic, time.Time{}, map[string]string{"agent.step": fmt.Sprint(i)}))
	}
	now := start.Add(24 * time.Hour)
	bound := p.KeepNewest + int(DefaultKeepYounger/time.Minute) + p.KeepHourly
	if k := kept(Plan(p, snaps, now)); len(k) > bound {
		t.Fatalf("kept %d of %d step checkpoints; want at most %d", len(k), len(snaps), bound)
	}
	// The bound holds however long the agent runs.
	for i := 24 * 60; i < 7*24*60; i++ {
		snaps = append(snaps, snapAt(fmt.Sprintf("step-%05d", i), start.Add(time.Duration(i)*time.Minute), ClassSemantic, time.Time{}, nil))
	}
	if k := kept(Plan(p, snaps, start.Add(7*24*time.Hour))); len(k) > bound {
		t.Fatalf("after a week: kept %d; want at most %d", len(k), bound)
	}
}

// Scheduled snapshots form their own class: retention keeps the newest K
// of them, K from the newest one's LabelScheduleKeep, and they neither
// count against KeepNewest nor get the age keeps; other snapshots are
// planned as before.
func TestPlanRetention_ScheduledClassKeepsItsOwnNewest(t *testing.T) {
	now := retBase.Add(30 * 24 * time.Hour)
	keep3 := map[string]string{LabelScheduleKeep: "3"}
	var snaps []Snapshot
	for d := 0; d < 6; d++ { // one scheduled snapshot a day, the newest first at now-1h
		snaps = append(snaps, snapAt(fmt.Sprintf("sched-%d", d), now.Add(-time.Hour-time.Duration(d)*24*time.Hour), ClassScheduled, time.Time{}, keep3))
	}
	snaps = append(snaps,
		snapAt("manual-new", now.Add(-2*time.Hour), "", time.Time{}, nil),
		snapAt("manual-old", now.Add(-20*24*time.Hour), "", time.Time{}, nil))
	k := kept(Plan(Policy{KeepNewest: 2, KeepYoungerNs: int64(30 * 24 * time.Hour)}, snaps, now))
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
	if k := kept(Plan(Policy{}, snaps[:6], now)); len(k) != 6 {
		t.Fatalf("kept %v with no label on the newest; want min(6, DefaultScheduleKeep)", k)
	}
}

// A pinned scheduled snapshot stays in its class: it is kept by its pin,
// ranks among the scheduled ones (its label sets K when it is the newest),
// and takes no log-spaced window from a manual snapshot.
func TestPlanRetention_PinnedScheduledStaysInItsClass(t *testing.T) {
	now := retBase.Add(30 * 24 * time.Hour)
	newest := snapAt("sched-pinned", now.Add(-10*time.Minute), ClassScheduled, now.Add(24*time.Hour), map[string]string{LabelScheduleKeep: "2"})
	snaps := []Snapshot{
		newest,
		snapAt("sched-1", now.Add(-24*time.Hour), ClassScheduled, time.Time{}, map[string]string{LabelScheduleKeep: "9"}),
		snapAt("sched-2", now.Add(-48*time.Hour), ClassScheduled, time.Time{}, map[string]string{LabelScheduleKeep: "9"}),
		snapAt("manual", now.Add(-20*time.Minute), "", time.Time{}, nil), // same hourly window as the pinned scheduled one
	}
	k := kept(Plan(Policy{KeepHourly: 1}, snaps, now))
	if len(k) != 3 || k["sched-pinned"] == nil || k["sched-1"] == nil || k["manual"] == nil {
		t.Fatalf("kept %v; want sched-pinned (pin, K=2 from its label), sched-1 (second newest scheduled) and manual (its hourly window)", k)
	}
}
