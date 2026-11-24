package durable_test

// Scheduled snapshots decided from stored facts: each test fails today and
// states the rule it checks. The rules: a scheduled snapshot's time is the
// slot it was due for, on a fixed per-volume grid on Unix time
// (slotStart(t) = phase + floor((t-phase)/P)*P, phase = hostlimit.Phase(vol +
// ":snapshot", P)); it is due when no published scheduled snapshot names the
// current slot; overdue is measured from the newest one. Nothing about the
// schedule's progress is kept in memory across a restart.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// schedFacts is the stored side of the schedule: the published scheduled
// snapshots of volume v1, by name. The callback publishes under the name
// the scheduled-snapshot path uses (the attempt time, UTC to the second).
type schedFacts struct {
	mu        sync.Mutex
	published map[string]time.Time // name -> the time the callback was given
	calls     int
	// unknownOnce: the first call publishes, then reports outcome_unknown.
	unknownOnce bool
	// block: calls wait here until the test ends (a run in progress).
	block chan struct{}
}

func newSchedFacts() *schedFacts { return &schedFacts{published: map[string]time.Time{}} }

func schedName(at time.Time) string { return "sched-" + at.UTC().Format("20060102T150405Z") }

func (f *schedFacts) take(ctx context.Context, _ string, at time.Time, cut func(context.Context) (storage.ChunkHead, error)) error {
	f.mu.Lock()
	f.calls++
	first, block := f.calls == 1, f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return ctx.Err()
	}
	if _, err := cut(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	f.published[schedName(at)] = at
	f.mu.Unlock()
	if first && f.unknownOnce {
		return &durable.SnapshotFailure{Code: "outcome_unknown"}
	}
	return nil
}

// seed records a published scheduled snapshot of time at.
func (f *schedFacts) seed(at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published[schedName(at)] = at
}

// newest is the time of the newest published scheduled snapshot (zero:
// none): the fact the schedule decides from.
func (f *schedFacts) newest() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n time.Time
	for _, at := range f.published {
		if at.After(n) {
			n = at
		}
	}
	return n
}

func (f *schedFacts) snapshot() (calls int, names map[string]time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	names = make(map[string]time.Time, len(f.published))
	for k, v := range f.published {
		names[k] = v
	}
	return f.calls, names
}

// factsProvider opens a provider for v1 with a schedule of period p over
// root and remote; facts is the store its schedule decides from.
func factsProvider(t *testing.T, root string, remote storage.ChunkRemote, p time.Duration, facts *schedFacts) *durable.DurableProvider {
	t.Helper()
	pr, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: root, BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkSnapshotPeriod:    p,
		ChunkScheduledNewest:   func(context.Context, string) (time.Time, error) { return facts.newest(), nil },
		ChunkScheduledSnapshot: facts.take,
		ChunkRemoteFor:         func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	return pr
}

// schedPhase is v1's offset within p, in whole seconds (a scheduled
// snapshot's name carries its slot to the second).
func schedPhase(p time.Duration) time.Duration {
	return hostlimit.Phase("v1:snapshot", p).Truncate(time.Second)
}

// slotStart is the start of the slot holding t on v1's grid of period p.
func slotStart(t time.Time, p time.Duration) time.Time {
	ph := int64(schedPhase(p))
	n := t.UnixNano() - ph
	k := n / int64(p)
	if n < 0 && n%int64(p) != 0 {
		k--
	}
	return time.Unix(0, ph+k*int64(p)).UTC()
}

// periodWithPhase returns a period in [lo, hi] whose v1 phase is at least
// frac of it (frac > 0.5) or below frac of it (frac < 0.5, "small"), so a
// run that waits for the phase is told apart from one taken at once.
func periodWithPhase(t *testing.T, lo, hi time.Duration, large bool, frac float64) time.Duration {
	t.Helper()
	for p := lo; p <= hi; p += 7 * time.Millisecond {
		r := float64(schedPhase(p)) / float64(p)
		if (large && r >= frac) || (!large && r <= frac) {
			return p
		}
	}
	t.Fatalf("no period in [%v, %v] with phase fraction %s %.2f", lo, hi, map[bool]string{true: ">=", false: "<="}[large], frac)
	return 0
}

// TestRedScheduleFirstRunAtOnce: a volume with no scheduled snapshot takes
// its first run at once, named for the current slot. Today the first run
// waits for the volume's phase (here at least 70% of the period).
func TestRedScheduleFirstRunAtOnce(t *testing.T) {
	p := periodWithPhase(t, 6*time.Second, 30*time.Second, true, 0.7)
	facts := newSchedFacts()
	pr := factsProvider(t, t.TempDir(), storage.NewMemChunkRemote(), p, facts)
	t.Cleanup(func() { _ = pr.Close() })
	start := time.Now()
	openRecovered(t, pr)
	limit := p * 3 / 10
	waitFor(t, limit, "the first scheduled run (phase "+schedPhase(p).Round(time.Millisecond).String()+" of "+p.Round(time.Millisecond).String()+")", func() bool {
		c, _ := facts.snapshot()
		return c >= 1
	})
	t.Logf("first run %v after start", time.Since(start).Round(time.Millisecond))
}

// TestRedScheduleMissedRunMadeUpAfterRestart: the newest scheduled
// snapshot is for the previous slot, so a run is due; a process that
// starts now takes it at once. Today a restart re-arms at start + phase.
func TestRedScheduleMissedRunMadeUpAfterRestart(t *testing.T) {
	p := periodWithPhase(t, 6*time.Second, 30*time.Second, true, 0.7)
	facts := newSchedFacts()
	facts.seed(slotStart(time.Now(), p).Add(-p))
	pr := factsProvider(t, t.TempDir(), storage.NewMemChunkRemote(), p, facts)
	t.Cleanup(func() { _ = pr.Close() })
	openRecovered(t, pr)
	waitFor(t, p*3/10, "the missed run after the restart", func() bool {
		c, _ := facts.snapshot()
		return c >= 1
	})
}

// TestRedScheduleRepeatedRestartsDoNotPostpone: a process restarted more
// often than its phase still takes one run per period. Today every restart
// re-arms at start + phase, so no run is ever taken.
func TestRedScheduleRepeatedRestartsDoNotPostpone(t *testing.T) {
	p := periodWithPhase(t, 4*time.Second, 20*time.Second, true, 0.8)
	facts := newSchedFacts()
	root, remote := t.TempDir(), storage.NewMemChunkRemote()
	every := schedPhase(p) / 2
	end := time.Now().Add(5 * p / 2)
	restarts := 0
	for time.Now().Before(end) {
		pr := factsProvider(t, root, remote, p, facts)
		openRecovered(t, pr)
		time.Sleep(every)
		_ = pr.Close()
		restarts++
	}
	calls, names := facts.snapshot()
	slots := map[time.Time]bool{}
	for _, at := range names {
		slots[slotStart(at, p)] = true
	}
	if len(slots) < 2 {
		t.Fatalf("restarted %d times every %v over %v (period %v): %d calls, %d slots with a snapshot; want at least 2", restarts, every.Round(time.Millisecond), 5*p/2, p, calls, len(slots))
	}
}

// TestRedScheduleAttemptTimeIsTheDueSlot: the time a scheduled attempt is
// given (and so its name and request id) is the start of the slot it is
// due for, not the moment of the attempt, so every attempt for one slot
// (a retry, a restart, a second holder) converges on one name. Today it is
// time.Now() at the attempt.
func TestRedScheduleAttemptTimeIsTheDueSlot(t *testing.T) {
	const p = 2 * time.Second
	facts := newSchedFacts()
	pr := factsProvider(t, t.TempDir(), storage.NewMemChunkRemote(), p, facts)
	t.Cleanup(func() { _ = pr.Close() })
	openRecovered(t, pr)
	waitFor(t, 3*p, "a published scheduled snapshot", func() bool {
		_, names := facts.snapshot()
		return len(names) >= 1
	})
	_, names := facts.snapshot()
	for name, at := range names {
		if want := slotStart(at, p); !at.Equal(want) {
			t.Fatalf("attempt %s was given %s; want its slot start %s (phase %v)", name, at.UTC().Format(time.RFC3339Nano), want.Format(time.RFC3339Nano), schedPhase(p))
		}
	}
}

// TestRedScheduleOutcomeUnknownThenRestartIsOneSnapshot: a scheduled
// create whose outcome was unknown did land; the process restarts within
// the same slot. The slot has its snapshot, so the new process takes no
// second one. Today the new process runs again under a new name: two
// scheduled snapshots for one slot.
func TestRedScheduleOutcomeUnknownThenRestartIsOneSnapshot(t *testing.T) {
	p := periodWithPhase(t, 10*time.Minute, 20*time.Minute, false, 0.002)
	facts := newSchedFacts()
	facts.unknownOnce = true
	root, remote := t.TempDir(), storage.NewMemChunkRemote()
	first := factsProvider(t, root, remote, p, facts)
	openRecovered(t, first)
	waitFor(t, 5*time.Second, "the first scheduled run", func() bool {
		c, _ := facts.snapshot()
		return c >= 1
	})
	time.Sleep(1100 * time.Millisecond) // a later attempt gets a later name today
	_ = first.Close()
	second := factsProvider(t, root, remote, p, facts)
	t.Cleanup(func() { _ = second.Close() })
	openRecovered(t, second)
	time.Sleep(5 * time.Second) // well past the phase (< 2.4 s here)
	calls, names := facts.snapshot()
	if len(names) == 0 {
		t.Fatalf("%d calls published nothing; want the first run's snapshot", calls)
	}
	perSlot := map[time.Time]int{}
	for _, at := range names {
		perSlot[slotStart(at, p)]++
	}
	for slot, n := range perSlot {
		if n > 1 {
			t.Fatalf("%d scheduled snapshots for the slot at %s (%d calls, names %v); want one per slot", n, slot.Format(time.RFC3339), calls, names)
		}
	}
}

// TestRedScheduleOverdueFromNewestAfterRestart: overdue is computed from
// the newest scheduled snapshot, so a process that starts while the newest
// is older than one period plus the retry cap reports overdue as soon as it
// has read it.
// Today it counts from the process start and reports not overdue.
func TestRedScheduleOverdueFromNewestAfterRestart(t *testing.T) {
	const p = time.Hour
	facts := newSchedFacts()
	facts.seed(slotStart(time.Now(), p).Add(-3 * p))
	facts.block = make(chan struct{}) // a run in progress does not change the answer
	t.Cleanup(func() { close(facts.block) })
	pr := factsProvider(t, t.TempDir(), storage.NewMemChunkRemote(), p, facts)
	t.Cleanup(func() { _ = pr.Close() })
	openRecovered(t, pr)
	waitFor(t, 5*time.Second, "overdue in the status (newest "+facts.newest().Format(time.RFC3339)+", period 1h)", func() bool {
		st := scheduledStatus(t, pr)
		return st != nil && st.Overdue
	})
}
