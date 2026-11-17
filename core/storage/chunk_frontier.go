package storage

import (
	"errors"
	"fmt"
)

// SeedChunkFrontier installs an empty fresh host's inherited remote frontier.
// Unlike AdvanceFrontier + Sync, it publishes no allocator/head watermark until
// both the data sync and checkpoint-metadata sync have succeeded. Call before
// attaching the chunk layer or admitting writes. Repeating after failure is safe.
// Order: captureMu -> lifecycleMu -> mu; no remote call or layer callback.
func (s *WALStore) SeedChunkFrontier(lsn uint64) error {
	if s.v3 != nil {
		return fmt.Errorf("%w: seed requires the v3 descriptor install transaction", errV3NotYet)
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
