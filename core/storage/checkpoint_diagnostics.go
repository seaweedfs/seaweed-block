package storage

import (
	"errors"
	"log"
	"time"
)

// CheckpointPersistenceStatus counts flusher checkpoint calls in this open.
// Success includes a safely clamped/unchanged checkpoint; it is not a slot-write
// count. Advances counts only the successes that moved the persisted
// CheckpointLSN, and LastAdvance is when the latest one did (zero: none in
// this open), so a starved checkpoint shows Successes rising while
// LastAdvance stays frozen. Fields are cumulative atomic observations, not a
// coherent frontier.
type CheckpointPersistenceStatus struct {
	Successes     uint64    // checkpoint calls that succeeded (including unchanged ones)
	Refusals      uint64    // checkpoint calls refused because the map coverage was stale
	Failures      uint64    // checkpoint calls that failed with an error
	CheckpointLSN uint64    // the persisted checkpoint LSN
	Advances      uint64    // successes that moved CheckpointLSN forward
	LastAdvance   time.Time // when the latest advance happened; zero if none in this open
}

func (s *WALStore) CheckpointPersistence() CheckpointPersistenceStatus {
	if s.flusher == nil {
		return CheckpointPersistenceStatus{CheckpointLSN: s.CheckpointLSN()}
	}
	i := &s.flusher.instr
	st := CheckpointPersistenceStatus{
		Successes:     i.checkpointSuccesses.Load(),
		Refusals:      i.checkpointRefusals.Load(),
		Failures:      i.checkpointFailures.Load(),
		CheckpointLSN: s.CheckpointLSN(),
		Advances:      i.checkpointAdvances.Load(),
	}
	if ns := i.checkpointLastAdvanceNanos.Load(); ns != 0 {
		st.LastAdvance = time.Unix(0, ns)
	}
	return st
}

// recordCheckpoint classifies one flusher checkpoint call; before and after
// are the persisted CheckpointLSN around it.
func (i *flusherInstrumentation) recordCheckpoint(err error, before, after uint64, now time.Time) {
	switch {
	case err == nil:
		i.checkpointSuccesses.Add(1)
		if after > before {
			i.checkpointAdvances.Add(1)
			i.checkpointLastAdvanceNanos.Store(now.UnixNano())
		}
	case errors.Is(err, ErrMapCoverageStale):
		i.checkpointRefusals.Add(1)
	default:
		i.checkpointFailures.Add(1)
	}
}

type mapCoverageRefusal string

func (r mapCoverageRefusal) Error() string { return ErrMapCoverageStale.Error() + ": " + string(r) }
func (r mapCoverageRefusal) Unwrap() error { return ErrMapCoverageStale }

// Owned exclusively by the flusher run loop; no storage lock or I/O ordering
// changes. Terminal errors are never rate limited.
type checkpointLogLimiter struct {
	last       time.Time
	suppressed uint64
}

func (l *checkpointLogLimiter) report(err error, now time.Time) {
	if !errors.Is(err, ErrMapCoverageStale) {
		log.Printf("storage: flusher: %v", err)
		return
	}
	if !l.last.IsZero() && now.Sub(l.last) < 30*time.Second {
		l.suppressed++
		return
	}
	var reason mapCoverageRefusal
	if !errors.As(err, &reason) {
		reason = "unspecified"
	}
	log.Printf("storage: flusher: checkpoint deferred reason=%s suppressed=%d: %v", string(reason), l.suppressed, err)
	l.last, l.suppressed = now, 0
}
