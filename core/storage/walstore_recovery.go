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
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if fn == nil {
		return errors.New("storage: walstore ScanLBAs: nil callback")
	}

	// Snapshot store state under lock.
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("storage: walstore ScanLBAs after Close")
	}
	s.mu.RUnlock()

	// The v3 source-state gate must precede even an empty scan.
	return s.scanLBAsV3(fromLSN, fn)

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

// scanLBAsV3 is ScanLBAs on a v3 store (the primary's catch-up source).
// It is all or nothing: it captures the recovery
// generation, the descriptor and the head, refuses an installing source
// (ErrBaseInstalling) before emitting anything, scans the trusted v3 chain
// into memory, revalidates the capture (a Recover, reset or BASE transition
// meanwhile retries the whole scan, or refuses if the source is now
// installing), and requires the trusted records to cover every LSN of
// [fromLSN, head): a chain that stops short (the scanner's CRC, binding or
// truncation stop cause) is a typed WAL integrity failure, and a WAL that
// no longer holds fromLSN a typed WALRecycled failure. Only then are the
// entries emitted, from memory, with no store lock held across the
// callbacks. A write record is one entry; a batch one entry per block.
func (s *WALStore) scanLBAsV3(fromLSN uint64, fn func(RecoveryEntry) error) error {
	v := s.v3
	const attempts = 8
	for attempt := 0; attempt < attempts; attempt++ {
		s.mu.RLock()
		gen := v.recoverGen.Load()
		installEpoch := v.installEpoch
		d := v.pub.current()
		headLSN := s.nextLSN
		checkpointLSN, retentionLSNs := d.CheckpointLSN, s.catchUpRetentionLocked(d)
		s.mu.RUnlock()
		if d.BaseState == BaseInstalling {
			return fmt.Errorf("%w: catch-up from an installing source", ErrBaseInstalling)
		}
		if gen%2 != 0 {
			continue // a recovery or reset is running; capture again
		}
		// A completed BASE authoritatively replaces its prefix; those LSNs
		// need a rebuild even if unused ring bytes stop on nonce binding.
		// Do not downgrade damage in the retained suffix after BaseLSN.
		if d.BaseState == BaseInstalled && fromLSN <= d.BaseLSN {
			return NewWALRecycledFailure(ErrWALRecycled, fmt.Sprintf(
				"fromLSN=%d is covered by installed BaseLSN=%d", fromLSN, d.BaseLSN))
		}
		floor := catchUpFloorV3(checkpointLSN, retentionLSNs)
		if checkpointLSN > 0 && fromLSN <= floor {
			return NewWALRecycledFailure(ErrWALRecycled, fmt.Sprintf(
				"fromLSN=%d checkpointLSN=%d headLSN=%d retentionLSNs=%d floor=%d",
				fromLSN, checkpointLSN, headLSN, retentionLSNs, floor))
		}
		if fromLSN >= headLSN {
			return nil
		}
		if h := v.testScanBeforeRead; h != nil {
			h()
		}
		scan, err := scanWALV3Accepted(s.fd, d, v.pub.accept)
		if err != nil {
			return fmt.Errorf("storage: walstore ScanLBAs (v3): %w", err)
		}
		if h := v.testScanRead; h != nil {
			h()
		}
		s.mu.RLock()
		now := v.pub.current()
		stable := v.recoverGen.Load() == gen && v.installEpoch == installEpoch &&
			now.Nonce == d.Nonce && now.BaseState == d.BaseState &&
			now.BaseInstallID == d.BaseInstallID && now.BaseCursor == d.BaseCursor &&
			now.RetainedTail == d.RetainedTail && now.CheckpointLSN == d.CheckpointLSN &&
			s.catchUpRetentionLocked(now) == retentionLSNs
		s.mu.RUnlock()
		if !stable {
			if now.BaseState == BaseInstalling {
				return fmt.Errorf("%w: catch-up source began installing", ErrBaseInstalling)
			}
			continue // a transition ran meanwhile: scan again from a fresh capture
		}
		entries, err := v3CatchUpEntries(scan, d, fromLSN, headLSN)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := fn(e); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("storage: walstore ScanLBAs (v3) did not get a stable capture in %d attempts", attempts)
}

// v3CatchUpEntries cuts [fromLSN, headLSN) out of a trusted scan, or fails:
// WALRecycled when the chain starts after fromLSN, WAL integrity (with the
// scanner's stop cause) when it ends before headLSN.
func v3CatchUpEntries(scan walScanResultV3, d descriptorV3, fromLSN, headLSN uint64) ([]RecoveryEntry, error) {
	blockSize := uint64(d.BlockSize)
	next := fromLSN
	var out []RecoveryEntry
	for _, r := range scan.Records {
		if next >= headLSN {
			break
		}
		e := r.Entry
		count := uint64(1)
		switch e.Type {
		case walEntryWrite:
		case walEntryWriteBatch:
			count = e.BatchCount
		default:
			continue // padding
		}
		if e.LSN+count-1 < next {
			continue
		}
		if e.LSN > next {
			if next == fromLSN {
				return nil, NewWALRecycledFailure(ErrWALRecycled, fmt.Sprintf(
					"v3 WAL starts at LSN %d, after the requested %d (head %d)", e.LSN, fromLSN, headLSN))
			}
			return nil, NewWALIntegrityFailure(nil, fmt.Sprintf("v3 catch-up: LSN %d missing before %d", next, e.LSN))
		}
		for i := uint64(0); i < count; i++ {
			lsn := e.LSN + i
			if lsn < next || lsn >= headLSN {
				continue
			}
			out = append(out, RecoveryEntry{LSN: lsn, LBA: uint32(e.LBA + i), Flags: RecoveryEntryWrite,
				Data: e.Data[i*blockSize : (i+1)*blockSize]})
			next = lsn + 1
		}
	}
	if next < headLSN {
		if next == fromLSN && len(out) == 0 && scan.StopCause == nil {
			return nil, NewWALRecycledFailure(ErrWALRecycled, fmt.Sprintf(
				"v3 WAL holds no record at LSN %d (head %d)", fromLSN, headLSN))
		}
		return nil, NewWALIntegrityFailure(scan.StopCause, fmt.Sprintf(
			"v3 catch-up stopped at LSN %d, short of the captured head %d", next, headLSN))
	}
	return out, nil
}
