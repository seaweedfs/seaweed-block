package storage

import (
	"errors"
	"log"
	"time"
)

// CheckpointPersistenceStatus counts flusher checkpoint calls in this open.
// Success includes a safely clamped/unchanged checkpoint; it is not a slot-write
// count. Fields are cumulative atomic observations, not a coherent frontier.
type CheckpointPersistenceStatus struct {
	Successes uint64
	Refusals  uint64
	Failures  uint64
}

func (s *WALStore) CheckpointPersistence() CheckpointPersistenceStatus {
	if s.flusher == nil {
		return CheckpointPersistenceStatus{}
	}
	i := &s.flusher.instr
	return CheckpointPersistenceStatus{i.checkpointSuccesses.Load(), i.checkpointRefusals.Load(), i.checkpointFailures.Load()}
}

func (i *flusherInstrumentation) recordCheckpoint(err error) {
	switch {
	case err == nil:
		i.checkpointSuccesses.Add(1)
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
