package storage

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// SeedChunkFrontier installs an empty fresh host's inherited remote frontier.
// Unlike AdvanceFrontier + Sync, it publishes no allocator/head watermark until
// both the data sync and checkpoint-metadata sync have succeeded. Call before
// attaching the chunk layer or admitting writes. Repeating after failure is safe.
// Order: captureMu -> lifecycleMu -> mu; no remote call or layer callback.
func (s *WALStore) SeedChunkFrontier(lsn uint64) error {

	return s.seedChunkFrontierV3(lsn)

}

// seedChunkFrontierV3 is SeedChunkFrontier on a dark v3 store (plan row 9):
// the same preconditions (an open, unattached store with no local writes
// beyond its checkpoint), a data fsync, and then the one checkpoint
// publication (publishCheckpointV3): the metadata barrier, the owned-map
// candidate at lsn and the sole slot publisher, which raises W and the
// checkpoint to lsn together. The volatile frontier moves only after that
// publication succeeded; any failure publishes nothing and may be retried.
// An unmapped v3 store has no map candidate for the new checkpoint and is
// refused. Order: captureMu -> lifecycleMu -> (syncMu ->) mu.
func (s *WALStore) seedChunkFrontierV3(lsn uint64) error {
	var t seedTimes
	err := s.seedChunkFrontierV3Locked(lsn, &t)
	// Logged after the capture and lifecycle locks are released: a slow log
	// writer never extends them. A refused or no-op seed did no I/O.
	if !t.validated.IsZero() {
		stage := func(from, to time.Time) string {
			if to.IsZero() {
				return "-"
			}
			return to.Sub(from).String()
		}
		log.Printf("storage: seed frontier %d: capture+lifecycle lock wait %s, store mutex+validation %s, data sync (incl. sync serialization) %s, checkpoint publish %s, err=%v",
			lsn, t.locked.Sub(t.start), t.validated.Sub(t.locked), stage(t.validated, t.synced), stage(t.synced, t.published), err)
	}
	return err
}

// CanSeedChunkFrontier reports whether SeedChunkFrontier(lsn) may proceed
// on this store as it is now: the one precondition the seed itself checks,
// so a caller can refuse a position before it writes anything elsewhere.
func (s *WALStore) CanSeedChunkFrontier(lsn uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seedRefusalLocked(lsn)
}

// seedRefusalLocked is the seed precondition (the caller holds s.mu): an
// open, unattached store without a sticky fault, no local writes beyond its
// checkpoint, no install, an lsn at or above the checkpoint with room for a
// next LSN, and the owned map for the checkpoint coverage.
func (s *WALStore) seedRefusalLocked(lsn uint64) error {
	v := s.v3
	if s.closed || s.chunk.Load() != nil {
		return errors.New("storage: seed frontier requires an open, unattached store")
	}
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	d := v.pub.current()
	if lsn == ^uint64(0) || lsn < d.CheckpointLSN || v.appended.Load() > d.CheckpointLSN ||
		s.walHead > s.checkpointLSN || s.nextLSN > lsn+1 || d.BaseState != BaseNone {
		return errors.New("storage: seed frontier cannot replace local writes, an install or wrap LSN")
	}
	if v.ownedMap == nil {
		return fmt.Errorf("%w: a v3 seed needs the owned map for its checkpoint coverage", errV3NotYet)
	}
	return nil
}

// seedTimes are the stage boundaries of one v3 seed (zero: not reached).
type seedTimes struct{ start, locked, validated, synced, published time.Time }

func (s *WALStore) seedChunkFrontierV3Locked(lsn uint64, t *seedTimes) (err error) {
	v := s.v3
	t.start = time.Now()
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	t.locked = time.Now()
	s.mu.Lock()
	if err := s.seedRefusalLocked(lsn); err != nil {
		s.mu.Unlock()
		return err
	}
	d := v.pub.current()
	if d.CheckpointLSN == lsn && d.W >= lsn && s.nextLSN == lsn+1 {
		s.mu.Unlock()
		return nil
	}
	gen := v.recoverGen.Load()
	s.mu.Unlock()
	t.validated = time.Now()
	err = s.syncFile(nil, s.fd.Sync)
	t.synced = time.Now()
	if err != nil {
		return fmt.Errorf("storage: seed data sync: %w", err)
	}
	_, err = s.publishCheckpointV3(lsn, true, gen)
	t.published = time.Now()
	if err != nil {
		return fmt.Errorf("storage: seed frontier %d: %w", lsn, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpointLSN, s.syncedLSN, s.walHead, s.nextLSN = lsn, lsn, lsn, lsn+1
	return nil
}
