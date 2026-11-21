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

// snapshotWake bounds how long the loop sleeps before it reads the wall
// clock again, so a wall-clock step is observed within it.
var snapshotWake = time.Minute

// snapshotWallNow is the wall clock the schedule runs on, without the
// monotonic reading: slots, durations and overdue all follow wall-clock
// steps.
func snapshotWallNow() time.Time { return time.Now().Round(0) }

// snapshotNow is the provider's scheduled-snapshot clock.
func (p *DurableProvider) snapshotNow() time.Time {
	if p.testSnapshotClock != nil {
		return p.testSnapshotClock()
	}
	return snapshotWallNow()
}

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
	// LastOkNs is when the last scheduled snapshot succeeded; zero: none
	// since the schedule started.
	LastOkNs int64 `json:"lastOkNs"`
	// NextDueNs is when the next attempt is due.
	NextDueNs int64 `json:"nextDueNs"`
	// LastFailure is the last failed attempt since the last success.
	LastFailure *ScheduledSnapshotFailure `json:"lastFailure,omitempty"`
	// Attempts counts the failed attempts since the last success.
	Attempts int `json:"attempts"`
	// Skipped counts attempts not made, by reason.
	Skipped map[string]int64 `json:"skipped,omitempty"`
	// Overdue reports that no scheduled snapshot succeeded within one
	// period plus the retry cap, counted from the last success or, before
	// any, from the start of the schedule.
	Overdue bool `json:"snapshot_overdue"`
}

// snapshotSchedule decides when one volume's scheduled snapshot is due and
// records the outcome of each attempt. Each slot starts at a multiple of
// the period (from the Unix epoch) plus the volume's offset, spread over
// the whole period and stable across restarts: "daily" is once per 24 h
// at a per-volume offset, not at a wall-clock hour. It is not safe for
// concurrent use.
type snapshotSchedule struct {
	period   time.Duration
	offset   time.Duration
	started  time.Time
	lastOK   time.Time
	next     time.Time
	failure  *ScheduledSnapshotFailure
	attempts int
	skipped  map[string]int64
}

// newSnapshotSchedule starts vol's schedule at now.
func newSnapshotSchedule(vol string, period time.Duration, now time.Time) *snapshotSchedule {
	s := &snapshotSchedule{period: period, offset: hostlimit.Phase(vol+":snapshot", period), started: now, skipped: map[string]int64{}}
	s.next = s.slotAfter(now)
	return s
}

// slotAfter returns the first slot start strictly after t.
func (s *snapshotSchedule) slotAfter(t time.Time) time.Time {
	p := int64(s.period)
	base := t.UnixNano() / p * p
	slot := time.Unix(0, base).Add(s.offset)
	if !slot.After(t) {
		slot = slot.Add(s.period)
	}
	return slot
}

// due returns when the next attempt is due.
func (s *snapshotSchedule) due() time.Time { return s.next }

// align moves the next attempt to the first slot after now when a wall
// clock step backwards left it further away than any slot or retry.
func (s *snapshotSchedule) align(now time.Time) {
	if s.next.Sub(now) > s.period+snapshotRetryMax {
		s.next = s.slotAfter(now)
	}
}

// record records the outcome of an attempt made at now: nil is a
// success (the next attempt is the next slot); a *SnapshotSkip is counted
// by reason and retried after the backoff cap; any other error is a
// failure, recorded with its code, and retried with a doubling backoff.
func (s *snapshotSchedule) record(now time.Time, err error) {
	var skip *SnapshotSkip
	switch {
	case err == nil:
		s.lastOK, s.failure, s.attempts = now, nil, 0
		s.next = s.slotAfter(now)
	case errors.As(err, &skip):
		s.skipped[skip.Reason]++
		s.next = earlier(now.Add(snapshotRetryMax), s.slotAfter(now))
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
	if !s.lastOK.IsZero() {
		from = s.lastOK
	}
	st := ScheduledSnapshotStatus{PeriodNs: int64(s.period), NextDueNs: s.next.UnixNano(), Attempts: s.attempts,
		Overdue: now.Sub(from) > s.period+snapshotRetryMax}
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

// alignedDue aligns the schedule to now and returns its due time.
func (sl *snapshotSlot) alignedDue(now time.Time) time.Time {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.s.align(now)
	return sl.s.due()
}

func (sl *snapshotSlot) record(now time.Time, err error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.s.record(now, err)
}

// chunkSnapshotLoop takes volumeID's scheduled snapshots while the
// attachment is current. It sleeps at most snapshotWake at a time and
// compares the wall clock with the due time on each wake, so wall-clock
// steps are observed. A restoring volume is skipped. Each attempt is one
// bounded pass: one urgent cut and the snapshot record. An attempt cut
// short by the loop's own end is not recorded. chunkBackground joins it
// with its other workers (INV-MAINT-JOIN).
func (p *DurableProvider) chunkSnapshotLoop(ctx context.Context, volumeID string, layer storage.ChunkLayer, current func() bool, slot *snapshotSlot) {
	sleep := func() time.Duration {
		now := p.snapshotNow()
		return min(max(slot.alignedDue(now).Sub(now), 0), snapshotWake)
	}
	timer := time.NewTimer(sleep())
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
		at := p.snapshotNow()
		if at.Before(slot.alignedDue(at)) {
			timer.Reset(sleep())
			continue
		}
		var err error
		if layer.Stats().AbsentChunks > 0 {
			// Restoring: HEAD names chunks this volume has not fetched yet.
			err = &SnapshotSkip{Reason: SnapshotSkipRestoring}
		} else {
			err = boundedPass(ctx, layer, current, timeout, func(passCtx context.Context) error {
				return p.cfg.ChunkScheduledSnapshot(passCtx, volumeID, at, cut)
			})
		}
		if ctx.Err() != nil {
			return
		}
		now := p.snapshotNow()
		slot.record(now, err)
		if err != nil {
			log.Printf("durable: scheduled snapshot volume=%s: %v (next attempt %s)", volumeID, err, slot.alignedDue(now).UTC().Format(time.RFC3339))
		}
		timer.Reset(sleep())
	}
}
