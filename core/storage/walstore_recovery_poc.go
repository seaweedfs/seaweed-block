package storage

// T4c-pre-A POC code for walstore — `wal_replay` (V2-faithful) tier-1
// mode. Mirror of smartwal's `core/storage/smartwal/recovery_poc.go`
// for the `state_convergence` mode. NOT production. The API shape
// (ScanFrom + RecoveryEntry) is provisional; final form lands at
// T4c-3 muscle port behind a `LogicalStorage.ScanLBAs`-style method.
//
// Key difference from smartwal: walstore's WAL stores full entry
// data (LBA + LSN + Data per record). Per-LSN replay is faithful —
// data is contemporaneous with LSN; no "data staleness" semantic;
// no per-LBA dedup. Maps directly to `INV-REPL-RECOVERY-STREAM-LBA-
// DEDUP` and `INV-REPL-RECOVERY-STREAM-LSN-IS-SCAN-TIME` being
// **NOT** subject to walstore (per memo §13.6 round-34 scoping).

import (
	"errors"
	"fmt"
)

// ErrWALRecycled is returned by ScanFrom when fromLSN is below the
// substrate's checkpoint boundary — the WAL space at that LSN may
// have been reused by newer entries after the flusher advanced the
// checkpoint. Mirrors V2 walstore semantics
// (`weed/storage/blockvol/wal_writer.go:218`).
var ErrWALRecycled = errors.New("storage: walstore WAL recycled past requested LSN")

// RecoveryEntry is the per-record payload ScanFrom emits for tier-1
// `wal_replay` mode. Carries the LBA, the contemporaneous LSN, the
// flags (write vs trim), and the entry's data byte-for-byte from
// the WAL. Walstore preserves data per LSN, so this is V2-faithful.
//
// Sibling shape to `smartwal.RecoveryEntry`. The two POC types
// remain in their respective packages to keep substrate ownership
// clear; T4c-3 muscle port will land a uniform shape behind the
// `LogicalStorage` interface.
type RecoveryEntry struct {
	LSN   uint64
	LBA   uint32
	Flags uint8
	Data  []byte
}

// ScanFrom emits RecoveryEntry callbacks for every WAL record with
// LSN >= fromLSN, in LSN-ascending order (the order the entries were
// appended). Returns ErrWALRecycled if fromLSN is at or below the
// store's current `checkpointLSN` — those entries have been flushed
// to extent and the WAL space may have been reused.
//
// V2-faithful per-LSN: 3 writes to the same LBA in [fromLSN, head]
// produce 3 entries (no dedup). This is the `wal_replay` tier-1
// sub-mode per memo §5.1 / §13.0a.
//
// POC scope (per design memo §12.2 capability #1):
//   - basic LSN-range scan with no concurrent writer
//   - ErrWALRecycled detection at checkpoint boundary
//   - retention boundary characterization
//
// Concurrent live-write + recovery-read (capability #3) is
// observable from caller; walstore's WAL preserves per-LSN data so
// emitted entries carry contemporaneous data — no smartwal-style
// staleness. POC report §3.b documents.
//
// Called by: T4c-pre-A POC tests; future transport recovery
// executor wal_replay path (post T4c-3 muscle port).
// Owns: per-call scan buffer; LSN-filter pass.
// Borrows: fn callback — caller retains; ScanFrom does not retain
// references to fn or to RecoveryEntry.Data past the callback
// return.
func (s *WALStore) ScanFrom(fromLSN uint64, fn func(RecoveryEntry) error) error {
	if fn == nil {
		return errors.New("storage: walstore ScanFrom: nil callback")
	}

	// Snapshot store state under lock.
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("storage: walstore ScanFrom after Close")
	}
	checkpointLSN := s.checkpointLSN
	headLSN := s.nextLSN
	s.mu.RUnlock()

	// ErrWALRecycled: fromLSN must be > checkpointLSN. The flusher
	// only advances checkpointLSN AFTER writing the corresponding
	// extent blocks; entries with LSN <= checkpointLSN are no longer
	// available in the WAL (their physical WAL space may have been
	// reused by newer appends).
	if fromLSN <= checkpointLSN && checkpointLSN > 0 {
		return fmt.Errorf("%w: fromLSN=%d checkpointLSN=%d headLSN=%d",
			ErrWALRecycled, fromLSN, checkpointLSN, headLSN)
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
				return fmt.Errorf("storage: walstore ScanFrom read header at %d: %w", pos, err)
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
				return fmt.Errorf("storage: walstore ScanFrom read entry at %d: %w", pos, err)
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
