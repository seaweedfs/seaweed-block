package smartwal

// T4c-pre-A POC code (per design memo §12). NOT production. The
// API shape (ScanFrom + RecoveryEntry) is provisional — final form
// depends on architect+QA decision on the semantic question
// surfaced in v3-phase-15-t4c-pre-poc-report.md.
//
// This file lives alongside store.go (not in _test.go) so the POC
// methods can be exercised from external callers (e.g., a future
// transport-side Recovery executor prototype) without re-exposing
// internal record/ring types. Marked POC by file name + this
// godoc; not advertised as a stable API.

import (
	"errors"
	"fmt"
	"hash/crc32"
)

// ErrWALRecycled is returned by ScanFrom when fromLSN is below the
// substrate's retained-WAL window. Mirrors V2 walstore semantics
// (`weed/storage/blockvol/wal_writer.go:218 ErrWALRecycled`).
//
// For smartwal: returned when fromLSN < (head - capacity), i.e.,
// fromLSN's slot has been overwritten by a newer LSN's record.
var ErrWALRecycled = errors.New("smartwal: WAL slot recycled past requested LSN")

// RecoveryEntry is the per-LBA payload ScanFrom emits for tier-1
// recovery streaming. Carries the LBA, the LATEST LSN within the
// scan range that wrote that LBA, the current extent block, and
// flags (write vs trim).
//
// IMPORTANT POC NOTE — semantic open question (see report §3.1):
//
// smartwal's WAL stores metadata only; the extent holds only the
// LATEST per-LBA data. ScanFrom therefore CANNOT deliver every
// LSN-versioned entry in the range (V2-strict-parity semantic).
// It delivers latest-per-LBA in range (state-convergence-equivalent
// semantic). For protocol correctness this is sufficient: replica
// state converges to the same value either way, AND the bitmap
// mask rule (design memo §2.2) handles cross-lane conflicts
// independent of LSN labeling. But it diverges from V2's wire
// observability — replica sees fewer entries than the primary
// wrote. Architect+QA decision required (per POC report §3.1).
//
// Field LSN is "the highest LSN within [fromLSN, head] that wrote
// this LBA." Concurrent live-write may bump the extent's actual
// data ahead of this LSN — see report §3.2 concurrency discussion.
type RecoveryEntry struct {
	LSN   uint64
	LBA   uint32
	Flags uint8
	Data  []byte
}

// ScanFrom emits RecoveryEntry callbacks for every LBA modified
// within the LSN range [fromLSN, head], in LSN-ascending order
// (last-writer-wins per LBA). Returns ErrWALRecycled if fromLSN's
// slot has been overwritten.
//
// Uses LSN-ordering of the slot scan to deliver entries in the
// order their LSNs occur (not LBA order). This matches V2's
// StreamEntries shape closer than a raw map iteration would.
//
// POC scope (per design memo §12.2 capability #1):
//   - basic range scan with no concurrent writer
//   - ErrWALRecycled detection
//   - retention boundary characterization
//
// Concurrent live-write + recovery-read interaction (capability #3)
// is observable by callers but is NOT made safe at this layer —
// see POC report §3.2 for the data-staleness concern.
//
// Called by: T4c-pre-A POC tests; future transport recovery
// executor prototype (post architect decision on semantic).
// Owns: per-call buffer for the slot scan; per-LBA last-writer-wins
// map.
// Borrows: fn callback — caller retains; ScanFrom does not retain
// references to fn or to RecoveryEntry.Data past the callback
// return.
func (s *Store) ScanFrom(fromLSN uint64, fn func(RecoveryEntry) error) error {
	if fn == nil {
		return errors.New("smartwal: ScanFrom: nil callback")
	}

	// Snapshot ring capacity + current head under lock so we have
	// a stable upper bound for the scan range.
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("smartwal: ScanFrom after Close")
	}
	head := s.nextLSN
	capacity := s.ring.capacity
	blockSize := int(s.hdr.BlockSize)
	s.mu.RUnlock()

	if fromLSN >= head {
		return nil // nothing to ship; caller is at-or-ahead of head
	}

	// Compute oldestPreserved: the smallest LSN whose slot has not
	// been overwritten. For ring capacity C and head H (exclusive),
	// last C LSNs are preserved: [H-C+1, H-1] inclusive. Equivalent:
	// oldestPreserved = H - C (if H > C) else 1.
	var oldestPreserved uint64
	if head > capacity {
		oldestPreserved = head - capacity
	} else {
		oldestPreserved = 1
	}
	if fromLSN < oldestPreserved {
		return fmt.Errorf("%w: fromLSN=%d oldestPreserved=%d head=%d",
			ErrWALRecycled, fromLSN, oldestPreserved, head)
	}

	// Scan all valid slot records, filter by LSN range, last-writer-
	// wins per LBA.
	records, err := s.ring.scanValid()
	if err != nil {
		return fmt.Errorf("smartwal: ScanFrom ring scan: %w", err)
	}

	// Records arrive sorted by LSN ascending from scanValid().
	// Walk them, keeping per-LBA latest record within range.
	type lbaState struct {
		rec record
	}
	latestByLBA := make(map[uint32]lbaState)
	for _, rec := range records {
		if rec.LSN < fromLSN {
			continue
		}
		if rec.LSN >= head {
			continue
		}
		latestByLBA[rec.LBA] = lbaState{rec: rec}
	}

	// Mid-scan recycle detection: if any record we collected has
	// an LSN below the current oldestPreserved at emit time, the
	// scan saw a torn boundary. Re-check after collection.
	s.mu.RLock()
	currentHead := s.nextLSN
	s.mu.RUnlock()
	currentOldest := uint64(1)
	if currentHead > capacity {
		currentOldest = currentHead - capacity
	}
	if fromLSN < currentOldest {
		return fmt.Errorf("%w: mid-scan recycle, fromLSN=%d shifted-oldest=%d",
			ErrWALRecycled, fromLSN, currentOldest)
	}

	// Emit in LSN-ascending order. Build a slice from the map so
	// callers see a deterministic order matching V2 StreamEntries.
	emitOrder := make([]record, 0, len(latestByLBA))
	for _, st := range latestByLBA {
		emitOrder = append(emitOrder, st.rec)
	}
	// scanValid already sorted records by LSN ascending; the map
	// pass loses that order. Re-sort here.
	sortRecordsByLSN(emitOrder)

	for _, rec := range emitOrder {
		// Read current extent[LBA]. NOTE: this is the LATEST data
		// for that LBA, which may differ from what rec.DataCRC32
		// covers if a concurrent live-write happened between the
		// ring scan and this read. See POC report §3.2.
		data := make([]byte, blockSize)
		off := s.extentBase + int64(rec.LBA)*int64(blockSize)
		if _, err := s.fd.ReadAt(data, off); err != nil {
			return fmt.Errorf("smartwal: ScanFrom read extent LBA %d: %w", rec.LBA, err)
		}

		// CRC sanity: if the extent's current data does not match
		// the slot's recorded CRC, log it (not an error in
		// last-writer-wins semantic; the extent's current bytes
		// may belong to a NEWER LSN that wrote the same LBA).
		// Surfaces via report §3.2 discussion.
		if crc32.ChecksumIEEE(data) != rec.DataCRC32 {
			// In tier-1 state-convergence semantic, we still emit
			// the current bytes — replica converges to the latest
			// state. The CRC mismatch is informational.
		}

		entry := RecoveryEntry{
			LSN:   rec.LSN,
			LBA:   rec.LBA,
			Flags: rec.Flags,
			Data:  data,
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

// sortRecordsByLSN sorts in-place by LSN ascending. Inline to avoid
// importing sort just for this one helper at the POC layer.
func sortRecordsByLSN(recs []record) {
	// Simple insertion sort — POC scope; record counts in tests
	// are small (<1000 typically).
	for i := 1; i < len(recs); i++ {
		j := i
		for j > 0 && recs[j-1].LSN > recs[j].LSN {
			recs[j-1], recs[j] = recs[j], recs[j-1]
			j--
		}
	}
}

// RingCapacity exposes the smartwal slot count for POC tests that
// need to characterize retention boundaries. Provisional API; not
// part of LogicalStorage.
func (s *Store) RingCapacity() uint64 {
	return s.ring.capacity
}
