package durable

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	// snapshotRetryMin and snapshotRetryMax bound the backoff after a
	// failed scheduled snapshot: it doubles from the minimum up to the cap.
	snapshotRetryMin = time.Minute
	snapshotRetryMax = time.Hour
)

// SnapshotSkipRestoring is the skip reason of a volume still fetching the
// chunks its HEAD names (a restore's hydration).
const SnapshotSkipRestoring = "restoring"

// SnapshotSkip is returned by a scheduled snapshot that was not attempted
// (for example, the volume is restoring). It is counted by Reason and not
// treated as a failure.
type SnapshotSkip struct{ Reason string }

func (s *SnapshotSkip) Error() string { return "durable: scheduled snapshot skipped: " + s.Reason }

// SnapshotFailure is a failed scheduled snapshot with its typed code.
type SnapshotFailure struct {
	Code string
	Err  error
}

func (f *SnapshotFailure) Error() string {
	if f.Err != nil {
		return "durable: scheduled snapshot failed: " + f.Code + ": " + f.Err.Error()
	}
	return "durable: scheduled snapshot failed: " + f.Code
}

func (f *SnapshotFailure) Unwrap() error { return f.Err }

// ScheduledSnapshotFailure is the last failed attempt.
type ScheduledSnapshotFailure struct {
	// Code is the failure's code, as the control endpoint reports it.
	Code string `json:"code"`
	// AtNs is when the attempt failed.
	AtNs int64 `json:"atNs"`
}

// ScheduledSnapshotStatus is one volume's scheduled-snapshot state.
type ScheduledSnapshotStatus struct {
	// PeriodNs is the schedule period; zero means off.
	PeriodNs int64 `json:"periodNs"`
	// NewestNs is the slot of the newest published scheduled snapshot, as
	// last read; zero: none, or not read yet.
	NewestNs int64 `json:"newestNs,omitempty"`
	// LastOkNs is when this process last took a scheduled snapshot; zero:
	// none since it started.
	LastOkNs int64 `json:"lastOkNs"`
	// NextDueNs is when the schedule next decides whether a run is due.
	NextDueNs int64 `json:"nextDueNs"`
	// LastFailure is the last failed attempt since the last success.
	LastFailure *ScheduledSnapshotFailure `json:"lastFailure,omitempty"`
	// Attempts counts the failed attempts since the last success.
	Attempts int `json:"attempts"`
	// Skipped counts attempts not made, by reason.
	Skipped map[string]int64 `json:"skipped,omitempty"`
	// Overdue reports that the newest scheduled snapshot is older than one
	// period plus the retry cap (before the newest is read: counted from
	// the start of the schedule).
	Overdue bool `json:"snapshot_overdue"`
	// ClockSuspect reports a newest scheduled snapshot whose slot lies more
	// than the retry cap ahead of this host's clock: a clock stepped back
	// here, or one that ran ahead where it was taken. No run is due until
	// the clock passes that slot.
	ClockSuspect bool `json:"clock_suspect,omitempty"`
}

// snapshotSchedule decides when one volume's scheduled snapshot is due from
// a stored fact: the newest published scheduled snapshot. Slots are a fixed
// per-volume grid on the wall clock, slotStart(t) = phase + whole periods,
// the phase being the volume's offset within the period (whole seconds). A
// run is due when no published scheduled snapshot names the current slot's
// start or a later one; the first run is due at once. Each run is given its
// slot's start, so its name and request id are the slot's: every attempt
// for one slot (a retry, a restart, another holder) converges on one
// snapshot. Only the retry state (failures, back-off) is in memory; a
// restart loses it and decides again from the fact. It is not safe for
// concurrent use.
type snapshotSchedule struct {
	period   time.Duration
	phase    time.Duration
	started  time.Time
	lastOK   time.Time
	newest   time.Time // the newest published scheduled snapshot's slot, as last observed
	next     time.Time // when to decide next
	failure  *ScheduledSnapshotFailure
	attempts int
	skipped  map[string]int64
}

// newSnapshotSchedule starts vol's schedule at now; its first decision is
// at once.
func newSnapshotSchedule(vol string, period time.Duration, now time.Time) *snapshotSchedule {
	return &snapshotSchedule{period: period, phase: snapshotPhase(vol, period), started: now, next: now, skipped: map[string]int64{}}
}

// snapshotPhase is vol's offset within period, in whole seconds (a
// scheduled snapshot's name carries its slot to the second).
func snapshotPhase(vol string, period time.Duration) time.Duration {
	return hostlimit.Phase(vol+":snapshot", period).Truncate(time.Second)
}

// slotStart returns the start of the slot holding t.
func (s *snapshotSchedule) slotStart(t time.Time) time.Time {
	n, p := t.UnixNano()-int64(s.phase), int64(s.period)
	k := n / p
	if n < 0 && n%p != 0 {
		k--
	}
	return time.Unix(0, int64(s.phase)+k*p)
}

// decide records newest, the slot of the newest published scheduled
// snapshot read at now (zero: none), and returns the slot a run is due for
// and whether one is due. When none is due the next decision is at the
// next slot's start.
func (s *snapshotSchedule) decide(now, newest time.Time) (time.Time, bool) {
	s.newest = newest
	slot := s.slotStart(now)
	if newest.IsZero() || slot.Truncate(time.Second).After(newest.Truncate(time.Second)) {
		return slot, true
	}
	s.next = slot.Add(s.period)
	return slot, false
}

// due returns when the schedule next decides.
func (s *snapshotSchedule) due() time.Time { return s.next }

// record records the outcome, at now, of an attempt for slot, or of the
// read of the newest scheduled snapshot before it: nil is a success (slot
// is now the newest; the next decision is at the next slot's start); a
// *SnapshotSkip is counted by reason and decided again within the backoff
// cap; any other error is a failure, recorded with its code, and decided
// again after a backoff that doubles from one minute up to one hour.
func (s *snapshotSchedule) record(now, slot time.Time, err error) {
	var skip *SnapshotSkip
	switch {
	case err == nil:
		s.lastOK, s.failure, s.attempts = now, nil, 0
		if slot.After(s.newest) {
			s.newest = slot
		}
		s.next = later(slot.Add(s.period), now)
	case errors.As(err, &skip):
		s.skipped[skip.Reason]++
		s.next = earlier(now.Add(snapshotRetryMax), later(s.slotStart(now).Add(s.period), now))
	default:
		s.attempts++
		s.failure = &ScheduledSnapshotFailure{Code: snapshotFailureCode(err), AtNs: now.UnixNano()}
		backoff := snapshotRetryMin << min(s.attempts-1, 30)
		if backoff > snapshotRetryMax || backoff <= 0 {
			backoff = snapshotRetryMax
		}
		s.next = now.Add(backoff)
	}
}

// status returns the schedule's state at now.
func (s *snapshotSchedule) status(now time.Time) ScheduledSnapshotStatus {
	from := s.started
	if !s.newest.IsZero() {
		from = s.newest
	}
	st := ScheduledSnapshotStatus{PeriodNs: int64(s.period), NextDueNs: s.next.UnixNano(), Attempts: s.attempts,
		Overdue: now.Sub(from) > s.period+snapshotRetryMax}
	if !s.newest.IsZero() {
		st.NewestNs = s.newest.UnixNano()
		st.ClockSuspect = s.newest.Sub(now) > snapshotRetryMax
	}
	if !s.lastOK.IsZero() {
		st.LastOkNs = s.lastOK.UnixNano()
	}
	if s.failure != nil {
		f := *s.failure
		st.LastFailure = &f
	}
	if len(s.skipped) > 0 {
		st.Skipped = make(map[string]int64, len(s.skipped))
		for k, v := range s.skipped {
			st.Skipped[k] = v
		}
	}
	return st
}

// snapshotFailureCode is a failure's typed code: a SnapshotFailure's own
// code, else "unknown".
func snapshotFailureCode(err error) string {
	var f *SnapshotFailure
	if errors.As(err, &f) && f.Code != "" {
		return f.Code
	}
	return "unknown"
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// snapshotSlot guards one running loop's schedule: the loop records
// outcomes, status readers read it.
type snapshotSlot struct {
	mu sync.Mutex
	s  *snapshotSchedule
}

func (sl *snapshotSlot) status(now time.Time) ScheduledSnapshotStatus {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return sl.s.status(now)
}

func (sl *snapshotSlot) due() time.Time {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return sl.s.due()
}

func (sl *snapshotSlot) decide(now, newest time.Time) (time.Time, bool) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return sl.s.decide(now, newest)
}

func (sl *snapshotSlot) record(now, slot time.Time, err error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.s.record(now, slot, err)
}

// chunkSnapshotLoop takes volumeID's scheduled snapshots while the
// attachment is current. Each decision is one bounded pass (its deadline,
// and cancelled when the layer is fenced or the attachment is no longer
// current): it reads the newest published scheduled snapshot
// (ChunkScheduledNewest) and, only when the current slot has none, takes
// one urgent cut and the snapshot record, named for its slot. A restoring
// volume is skipped. A pass cut short by the loop's own end is not
// recorded. chunkBackground joins it with its other workers
// (INV-MAINT-JOIN).
func (p *DurableProvider) chunkSnapshotLoop(ctx context.Context, volumeID string, layer storage.ChunkLayer, current func() bool, slot *snapshotSlot) {
	timer := time.NewTimer(time.Until(slot.due()))
	defer timer.Stop()
	timeout := 2 * p.cfg.ChunkCutTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	cut := func(ctx context.Context) (storage.ChunkHead, error) {
		r, err := p.CutVolume(hostlimit.WithClass(ctx, hostlimit.Urgent), volumeID, 0)
		return r.Head, err
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if ctx.Err() != nil || layer.Fenced() || !current() {
			return
		}
		var at time.Time
		due := true
		err := boundedPass(ctx, layer, current, timeout, func(passCtx context.Context) error {
			newest, err := p.cfg.ChunkScheduledNewest(passCtx, volumeID)
			if err != nil {
				return err
			}
			if at, due = slot.decide(time.Now(), newest); !due {
				return nil
			}
			if layer.Stats().AbsentChunks > 0 {
				// Restoring: HEAD names chunks this volume has not fetched yet.
				return &SnapshotSkip{Reason: SnapshotSkipRestoring}
			}
			return p.cfg.ChunkScheduledSnapshot(passCtx, volumeID, at, cut)
		})
		if ctx.Err() != nil {
			return
		}
		if err == nil && !due {
			timer.Reset(time.Until(slot.due()))
			continue
		}
		slot.record(time.Now(), at, err)
		if err != nil {
			log.Printf("durable: scheduled snapshot volume=%s: %v (next attempt in %s)", volumeID, err, time.Until(slot.due()).Round(time.Second))
		}
		timer.Reset(time.Until(slot.due()))
	}
}
