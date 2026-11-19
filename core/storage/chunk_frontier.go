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
	if s.v3 != nil {
		return s.seedChunkFrontierV3(lsn)
	}
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.chunk.Load() != nil {
		return errors.New("storage: seed frontier requires an open, unattached store")
	}
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	if lsn == ^uint64(0) || lsn < s.checkpointLSN || s.walHead > s.checkpointLSN || s.nextLSN > lsn+1 {
		return errors.New("storage: seed frontier cannot replace local writes or wrap LSN")
	}
	if s.checkpointLSN == lsn && s.syncedLSN >= lsn && s.nextLSN == lsn+1 {
		return nil
	}
	if err := s.syncCache(); err != nil {
		return fmt.Errorf("storage: seed data sync: %w", err)
	}
	if s.metadata != nil {
		if err := s.metadata.BeforeCheckpointAdvance(lsn); err != nil {
			return fmt.Errorf("storage: block metadata before seed frontier %d: %w", lsn, err)
		}
	}
	next := *s.sb
	next.WALCheckpointLSN = lsn
	next.WALHead = s.wal.logicalHeadValue()
	buf := newSimpleByteBuf()
	if _, err := next.writeTo(buf); err != nil {
		return err
	}
	if err := s.syncFile(nil, func() error { return s.syncDirectFrontierMetadata(buf.bytes()) }); err != nil {
		return fmt.Errorf("storage: seed metadata sync: %w", err)
	}
	*s.sb = next
	s.checkpointLSN, s.syncedLSN, s.walHead, s.nextLSN = lsn, lsn, lsn, lsn+1
	s.pendingDirectFrontierLSN = 0
	return nil
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
func (s *WALStore) seedChunkFrontierV3(lsn uint64) (err error) {
	v := s.v3
	start := time.Now()
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	locked := time.Now()
	var synced, published time.Time
	defer func() {
		if synced.IsZero() {
			return // refused or already seeded: no I/O to time
		}
		stage := func(from, to time.Time) string {
			if to.IsZero() {
				return "-"
			}
			return to.Sub(from).String()
		}
		log.Printf("storage: seed frontier %d: lock wait %s, data fsync %s, checkpoint publish %s, err=%v",
			lsn, locked.Sub(start), synced.Sub(locked), stage(synced, published), err)
	}()
	s.mu.Lock()
	if s.closed || s.chunk.Load() != nil {
		s.mu.Unlock()
		return errors.New("storage: seed frontier requires an open, unattached store")
	}
	if err := s.stickyIOFault(); err != nil {
		s.mu.Unlock()
		return err
	}
	d := v.pub.current()
	if lsn == ^uint64(0) || lsn < d.CheckpointLSN || v.appended.Load() > d.CheckpointLSN ||
		s.walHead > s.checkpointLSN || s.nextLSN > lsn+1 || d.BaseState != BaseNone {
		s.mu.Unlock()
		return errors.New("storage: seed frontier cannot replace local writes, an install or wrap LSN")
	}
	if v.ownedMap == nil {
		s.mu.Unlock()
		return fmt.Errorf("%w: a v3 seed needs the owned map for its checkpoint coverage", errV3NotYet)
	}
	if d.CheckpointLSN == lsn && d.W >= lsn && s.nextLSN == lsn+1 {
		s.mu.Unlock()
		return nil
	}
	gen := v.recoverGen.Load()
	s.mu.Unlock()
	err = s.syncFile(nil, s.fd.Sync)
	synced = time.Now()
	if err != nil {
		return fmt.Errorf("storage: seed data sync: %w", err)
	}
	_, err = s.publishCheckpointV3(lsn, true, gen)
	published = time.Now()
	if err != nil {
		return fmt.Errorf("storage: seed frontier %d: %w", lsn, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpointLSN, s.syncedLSN, s.walHead, s.nextLSN = lsn, lsn, lsn, lsn+1
	s.pendingDirectFrontierLSN = 0
	return nil
}
