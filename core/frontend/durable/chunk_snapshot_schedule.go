package durable

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// capturePendingNote names a capture refused because an earlier HEAD commit's
// outcome is unresolved (Stats.PendingUnresolved): until it settles every
// cut, snapshot and detach of the volume is refused, so the logs say so, with
// the cause and the cure, once per attempt (never per write).
func capturePendingNote(err error) (string, bool) {
	if !errors.Is(err, storage.ErrChunkCASOutcomeUnknown) {
		return "", false
	}
	return fmt.Sprintf("PendingUnresolved: an earlier HEAD commit's outcome is unresolved, so every cut, snapshot and detach of this volume is refused until it settles; cause: %v; it is retried at every capture; if it persists, check the filer, then restart the volume (the next holder settles it)", err), true
}

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
// records the outcome of each attempt, on this process's monotonic time
// (every time in it is a time.Now() reading or derived from one), so
// wall-clock steps move nothing. Slots are the start plus the volume's
// offset (spread over the whole period) plus whole periods: "daily" is
// once per 24 h at a per-volume offset, not at a wall-clock hour. A
// restart starts a fresh schedule. It is not safe for concurrent use.
type snapshotSchedule struct {
	period   time.Duration
	origin   time.Time // the first slot: the start plus the volume's offset
	started  time.Time
	lastOK   time.Time
	next     time.Time
	failure  *ScheduledSnapshotFailure
	attempts int
	skipped  map[string]int64
}

// newSnapshotSchedule starts vol's schedule at now, a time.Now() reading.
func newSnapshotSchedule(vol string, period time.Duration, now time.Time) *snapshotSchedule {
	s := &snapshotSchedule{period: period, origin: now.Add(hostlimit.Phase(vol+":snapshot", period)), started: now, skipped: map[string]int64{}}
	s.next = s.origin
	return s
}

// slotAfter returns the first slot strictly after t: the origin plus a
// whole number of periods.
func (s *snapshotSchedule) slotAfter(t time.Time) time.Time {
	if t.Before(s.origin) {
		return s.origin
	}
	return s.origin.Add((t.Sub(s.origin)/s.period + 1) * s.period)
}

// due returns when the next attempt is due.
func (s *snapshotSchedule) due() time.Time { return s.next }

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

func (sl *snapshotSlot) due() time.Time {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return sl.s.due()
}

func (sl *snapshotSlot) record(now time.Time, err error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.s.record(now, err)
}

// chunkSnapshotLoop takes volumeID's scheduled snapshots while the
// attachment is current, on this process's monotonic time. A restoring
// volume is skipped. Each attempt is one bounded pass: one urgent cut and
// the snapshot record. An attempt cut short by the loop's own end is not
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
		at := time.Now()
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
		slot.record(time.Now(), err)
		if err != nil {
			next := time.Until(slot.due()).Round(time.Second)
			if note, stuck := capturePendingNote(err); stuck {
				log.Printf("durable: scheduled snapshot volume=%s REFUSED: %s (next attempt in %s)", volumeID, note, next)
			} else {
				log.Printf("durable: scheduled snapshot volume=%s: %v (next attempt in %s)", volumeID, err, next)
			}
		}
		timer.Reset(time.Until(slot.due()))
	}
}
