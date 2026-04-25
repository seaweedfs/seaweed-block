package smartwal

// T4c-2 production smartwal recovery muscle: tier-1 `state_convergence`
// (V3-native, per-LBA last-writer-wins) sub-mode of the `wal_delta`
// recovery content kind (memo §13.0a).
//
// Promoted from T4c-pre-A POC code (commit `66495f9`). Per memo §13
// round-34: this sub-mode delivers state-convergent semantics — 3
// writes to LBA=L produce 1 RecoveryEntry whose data is the latest-
// observed value at scan time. The replica's converged state is
// byte-equivalent to the primary's after the scan completes; the
// `INV-REPL-RECOVERY-STREAM-LBA-DEDUP` and `INV-REPL-RECOVERY-STREAM-
// LSN-IS-SCAN-TIME` invariants apply ONLY to this sub-mode.
//
// Scan-time LSN: the LSN field on emitted RecoveryEntry is the
// highest LSN within [fromLSN, head] that wrote that LBA. Concurrent
// live-write may bump the extent's actual data ahead of this LSN at
// scan time — POC report §3.2 documents the concurrency interaction.

import (
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Local aliases keep the call-site code unchanged when promoting from
// POC types. The aliases re-export the unified storage-package types
// without imposing a substrate-specific name on callers.
type RecoveryEntry = storage.RecoveryEntry

// ErrWALRecycled is the unified sentinel from storage; re-export so
// existing call sites continue to compile.
var ErrWALRecycled = storage.ErrWALRecycled

// ScanLBAs emits RecoveryEntry callbacks for every LBA modified
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
// Borrows: fn callback — caller retains; ScanLBAs does not retain
// references to fn or to RecoveryEntry.Data past the callback
// return.
func (s *Store) ScanLBAs(fromLSN uint64, fn func(RecoveryEntry) error) error {
	if fn == nil {
		return errors.New("smartwal: ScanLBAs: nil callback")
	}

	// Snapshot ring capacity + current head under lock so we have
	// a stable upper bound for the scan range.
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("smartwal: ScanLBAs after Close")
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
		return fmt.Errorf("smartwal: ScanLBAs ring scan: %w", err)
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
			return fmt.Errorf("smartwal: ScanLBAs read extent LBA %d: %w", rec.LBA, err)
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
