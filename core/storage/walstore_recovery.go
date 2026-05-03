package storage

// Walstore recovery muscle: per-LSN `wal_replay` sub-mode of the
// `wal_delta` recovery content kind.
//
// Key difference from smartwal: walstore's WAL stores full entry
// data (LBA + LSN + Data per record). Per-LSN replay is faithful —
// data is contemporaneous with LSN; no "data staleness" semantic;
// no per-LBA dedup.

import (
	"errors"
	"fmt"
)

// ScanLBAs emits RecoveryEntry callbacks for every WAL record with
// LSN >= fromLSN, in LSN-ascending order (the order the entries were
// appended). Returns ErrWALRecycled if fromLSN is at or below the
// store's current `checkpointLSN` — those entries have been flushed
// to extent and the WAL space may have been reused.
//
// Per-LSN: 3 writes to the same LBA in [fromLSN, head] produce 3
// entries (no dedup).
//
// Concurrent live-write + recovery-read is observable from caller;
// walstore's WAL preserves per-LSN data so emitted entries carry
// contemporaneous data — no smartwal-style staleness.
//
// Called by: transport recovery executor wal_replay path.
// Owns: per-call scan buffer; LSN-filter pass.
// Borrows: fn callback — caller retains; ScanLBAs does not retain
// references to fn or to RecoveryEntry.Data past the callback
// return.
func (s *WALStore) ScanLBAs(fromLSN uint64, fn func(RecoveryEntry) error) error {
	if fn == nil {
		return errors.New("storage: walstore ScanLBAs: nil callback")
	}

	// Snapshot store state under lock.
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("storage: walstore ScanLBAs after Close")
	}
	checkpointLSN := s.checkpointLSN
	headLSN := s.nextLSN
	retentionLSNs := s.recoveryRetentionLSNs
	s.mu.RUnlock()

	// Retention gate: by default the recycle threshold is
	// checkpointLSN; an operator-tunable retention window lets a
	// slow replica keep a recovery scan path while it lags within
	// the configured envelope.
	//
	// Effective recycle floor:
	//   floor = checkpointLSN - retentionLSNs   (saturating to 0)
	//
	// fromLSN is recycled when fromLSN <= floor (strict <= — an LSN
	// equal to the floor IS recycled). retentionLSNs == 0 reduces
	// to the simple 'fromLSN <= checkpointLSN' gate.
	//
	// Pinned by: INV-G6-CATCHUP-CONVERGES-WITHIN-RETENTION (positive)
	// + INV-G6-RETENTION-POLICY-OPERATOR-VISIBLE (the knob itself).
	floor := checkpointLSN
	if retentionLSNs >= floor {
		floor = 0
	} else {
		floor = checkpointLSN - retentionLSNs
	}
	if fromLSN <= floor && checkpointLSN > 0 {
		// Wrap in typed RecoveryFailure so transport can extract
		// the kind via errors.As. errors.Is(err, ErrWALRecycled)
		// still works via Unwrap.
		return NewWALRecycledFailure(
			ErrWALRecycled,
			fmt.Sprintf("fromLSN=%d checkpointLSN=%d headLSN=%d retentionLSNs=%d floor=%d",
				fromLSN, checkpointLSN, headLSN, retentionLSNs, floor),
		)
	}
	if fromLSN >= headLSN {
		return nil // caller is at-or-ahead of head; nothing to ship
	}

	// Snapshot WAL writer's logical head/tail under its own lock,
	// then walk the active region. Wraparound handled the same way
	// recovery.go:recoverWAL does it.
	logicalHead := s.wal.logicalHeadValue()
	logicalTail := s.wal.logicalTailValue()
	walSize := s.sb.WALSize
	walOffset := s.sb.WALOffset

	if logicalHead == logicalTail {
		return nil // WAL is empty
	}

	physHead := logicalHead % walSize
	physTail := logicalTail % walSize
	type scanRange struct{ start, end uint64 }
	var ranges []scanRange
	if physHead > physTail {
		ranges = append(ranges, scanRange{physTail, physHead})
	} else {
		ranges = append(ranges, scanRange{physTail, walSize})
		if physHead > 0 {
			ranges = append(ranges, scanRange{0, physHead})
		}
	}

	blockSize := s.sb.BlockSize

	for _, r := range ranges {
		pos := r.start
		for pos < r.end {
			remaining := r.end - pos
			if remaining < uint64(walEntryHeaderSize) {
				break
			}
			headerBuf := make([]byte, walEntryHeaderSize)
			absOff := int64(walOffset + pos)
			if _, err := s.fd.ReadAt(headerBuf, absOff); err != nil {
				return fmt.Errorf("storage: walstore ScanLBAs read header at %d: %w", pos, err)
			}
			entryType := headerBuf[16]
			lengthField := parseLengthFromHeader(headerBuf)

			if entryType == walEntryPadding {
				pos += uint64(walEntryHeaderSize) + uint64(lengthField)
				continue
			}
			var payloadLen uint64
			if entryType == walEntryWrite {
				payloadLen = uint64(lengthField)
			}
			entrySize := uint64(walEntryHeaderSize) + payloadLen
			if entrySize > remaining {
				// Torn / partial entry; stop this range.
				break
			}
			fullBuf := make([]byte, entrySize)
			if _, err := s.fd.ReadAt(fullBuf, absOff); err != nil {
				return fmt.Errorf("storage: walstore ScanLBAs read entry at %d: %w", pos, err)
			}
			entry, err := decodeWALEntry(fullBuf)
			if err != nil {
				// Torn or trailing-zero region — stop scanning.
				break
			}

			pos += entrySize

			// LSN filter: skip entries below fromLSN; stop walking
			// is NOT correct here because LSN order in the WAL is
			// monotonic only WITHIN A RANGE — we may need to keep
			// scanning past entries below fromLSN in the wraparound
			// case. Filter inline rather than break.
			if entry.LSN < fromLSN {
				continue
			}
			if entry.LSN >= headLSN {
				continue
			}

			// Build RecoveryEntry. Walstore stores full data in
			// the WAL — emit byte-for-byte.
			switch entry.Type {
			case walEntryWrite:
				blocks := entry.Length / blockSize
				if blocks == 0 {
					blocks = 1
				}
				// One WAL entry may cover multiple consecutive LBAs
				// (entry.Length spans multiple block-sized chunks).
				// Emit one RecoveryEntry per block, all carrying the
				// same LSN — V2-faithful since each LSN labels the
				// full multi-block write atomically.
				for i := uint32(0); i < blocks; i++ {
					blockData := entry.Data[i*blockSize : (i+1)*blockSize]
					emit := RecoveryEntry{
						LSN:   entry.LSN,
						LBA:   uint32(entry.LBA) + i,
						Flags: walEntryWrite,
						Data:  blockData,
					}
					if err := fn(emit); err != nil {
						return err
					}
				}
			case walEntryTrim:
				blocks := entry.Length / blockSize
				if blocks == 0 {
					blocks = 1
				}
				for i := uint32(0); i < blocks; i++ {
					emit := RecoveryEntry{
						LSN:   entry.LSN,
						LBA:   uint32(entry.LBA) + i,
						Flags: walEntryTrim,
						Data:  nil, // trim has no data
					}
					if err := fn(emit); err != nil {
						return err
					}
				}
			case walEntryBarrier:
				// barrier entries don't carry data; skip emission
				// (they're an internal marker, not a recovery payload)
			}
		}
	}
	return nil
}

// WALCapacityBytes exposes the WAL region size for POC retention
// characterization. Provisional API; not part of LogicalStorage.
func (s *WALStore) WALCapacityBytes() uint64 {
	return s.sb.WALSize
}

// CheckpointLSNForPOC exposes the current checkpoint boundary for
// retention-boundary tests. Same value as `CheckpointLSN()` (already
// public for diagnostics) — sibling name documents POC use.
func (s *WALStore) CheckpointLSNForPOC() uint64 {
	return s.CheckpointLSN()
}
