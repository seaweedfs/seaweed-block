// Package memorywal is an in-memory implementation of
// core/storage.LogicalStorage that uses an append-only record log
// to faithfully model V2-style WAL replay semantics — distinct from
// core/storage.BlockStore (which only stores current LBA state and
// synthesizes a scan-time LSN).
//
// Use memorywal when a test or component needs:
//
//   - Each Write captures its contemporaneous LSN; the record is
//     never rewritten in place.
//   - ScanLBAs(fromLSN, fn) emits each record's REAL write-time LSN
//     (matches the walstore "RecoveryModeWALReplay" sub-mode).
//   - AdvanceWALTail(newTail) trims records with LSN < newTail; a
//     subsequent ScanLBAs(fromLSN < newTail) returns the typed
//     storage.RecoveryFailure with Kind=WALRecycled.
//   - AppliedLSNs returns a real per-LBA highest-LSN map (BlockStore
//     returns ErrAppliedLSNsNotTracked).
//
// Memory vs disk is the wrong axis: the difference between
// memorywal and BlockStore is the data model — append-only WAL log
// vs current-state map — not the storage medium. Both run in
// memory.
//
// Sister packages in this tree:
//
//   - core/storage (BlockStore): minimal in-memory current-state map.
//   - core/storage (WALStore): on-disk WAL+extent.
//   - core/storage/smartwal (Store): on-disk extent + ring metadata.
//   - core/storage/memorywal (this package): in-memory append-only
//     WAL log with V2-faithful per-LSN replay.
package memorywal

import (
	"errors"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Store is the in-memory append-only WAL implementation of
// storage.LogicalStorage. Concurrency: a single sync.RWMutex guards
// records + frontier counters; safe for concurrent Read while
// Write/Apply hold the writer side.
type Store struct {
	numBlocks uint32
	blockSize int

	mu        sync.RWMutex
	closed    bool
	records   []walRecord
	syncedLSN uint64
	nextLSN   uint64

	// retainStart is the S boundary: lowest LSN currently in records.
	// 0 until the first Write/ApplyEntry. Bumped by AdvanceWALTail.
	// Reported via Boundaries() — purely informational about what the
	// records list still holds.
	retainStart uint64

	// recycleFloor is the explicit "WAL recycled past" frontier, set
	// only by AdvanceWALTail. ScanLBAs gates on this — NOT on
	// retainStart — so a caller asking for LSN > 0 against a store
	// whose records happen to start at LSN 100 (because ApplyEntry
	// installed an entry at high LSN) gets the available records
	// rather than a spurious WALRecycled error.
	recycleFloor uint64
}

type walRecord struct {
	LSN   uint64
	LBA   uint32
	Flags uint8
	Data  []byte
}

// NewStore constructs an empty WAL substrate covering [0, numBlocks).
// blockSize=0 falls back to storage.DefaultBlockSize.
func NewStore(numBlocks uint32, blockSize int) *Store {
	if blockSize == 0 {
		blockSize = storage.DefaultBlockSize
	}
	return &Store{
		numBlocks: numBlocks,
		blockSize: blockSize,
		nextLSN:   1,
	}
}

func (s *Store) Write(lba uint32, data []byte) (uint64, error) {
	if lba >= s.numBlocks {
		return 0, fmt.Errorf("memorywal: LBA %d out of range (max %d)", lba, s.numBlocks-1)
	}
	if len(data) != s.blockSize {
		return 0, fmt.Errorf("memorywal: data size %d != block size %d", len(data), s.blockSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("memorywal: Write after Close")
	}
	cp := make([]byte, s.blockSize)
	copy(cp, data)
	lsn := s.nextLSN
	s.nextLSN++
	if s.retainStart == 0 {
		s.retainStart = lsn
	}
	s.records = append(s.records, walRecord{LSN: lsn, LBA: lba, Flags: storage.RecoveryEntryWrite, Data: cp})
	return lsn, nil
}

func (s *Store) Read(lba uint32) ([]byte, error) {
	if lba >= s.numBlocks {
		return nil, fmt.Errorf("memorywal: LBA %d out of range", lba)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.records) - 1; i >= 0; i-- {
		if s.records[i].LBA == lba {
			cp := make([]byte, s.blockSize)
			copy(cp, s.records[i].Data)
			return cp, nil
		}
	}
	return make([]byte, s.blockSize), nil
}

func (s *Store) Sync() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("memorywal: Sync after Close")
	}
	if s.nextLSN > 1 {
		s.syncedLSN = s.nextLSN - 1
	}
	return s.syncedLSN, nil
}

func (s *Store) Recover() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncedLSN, nil
}

func (s *Store) Boundaries() (R, S, H uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	H = s.syncedLSN
	if s.nextLSN > 1 {
		H = s.nextLSN - 1
	}
	return s.syncedLSN, s.retainStart, H
}

func (s *Store) NextLSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextLSN
}

func (s *Store) NumBlocks() uint32 { return s.numBlocks }
func (s *Store) BlockSize() int    { return s.blockSize }

func (s *Store) AdvanceFrontier(lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lsn > s.syncedLSN {
		s.syncedLSN = lsn
	}
	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
}

// AdvanceWALTail trims records with LSN < newTail, simulating WAL
// recycling. After this, ScanLBAs(fromLSN < newTail) returns
// storage.RecoveryFailure(Kind=WALRecycled). Sets recycleFloor as
// the explicit "you asked for LSNs we've recycled" gate.
func (s *Store) AdvanceWALTail(newTail uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newTail <= s.recycleFloor {
		return
	}
	keep := make([]walRecord, 0, len(s.records))
	for _, r := range s.records {
		if r.LSN >= newTail {
			keep = append(keep, r)
		}
	}
	s.records = keep
	s.retainStart = newTail
	s.recycleFloor = newTail
}

func (s *Store) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	if lba >= s.numBlocks {
		return fmt.Errorf("memorywal: apply LBA %d out of range", lba)
	}
	if len(data) != s.blockSize {
		return fmt.Errorf("memorywal: apply data size %d != block size %d", len(data), s.blockSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("memorywal: ApplyEntry after Close")
	}
	cp := make([]byte, s.blockSize)
	copy(cp, data)
	s.records = append(s.records, walRecord{LSN: lsn, LBA: lba, Flags: storage.RecoveryEntryWrite, Data: cp})
	if s.retainStart == 0 || lsn < s.retainStart {
		s.retainStart = lsn
	}
	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	return nil
}

// AllBlocks returns last-writer-wins per LBA across retained records,
// filtering out all-zero blocks (matches the LogicalStorage contract).
func (s *Store) AllBlocks() map[uint32][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	latest := make(map[uint32][]byte)
	latestLSN := make(map[uint32]uint64)
	for _, r := range s.records {
		if r.LSN >= latestLSN[r.LBA] {
			latestLSN[r.LBA] = r.LSN
			cp := make([]byte, s.blockSize)
			copy(cp, r.Data)
			latest[r.LBA] = cp
		}
	}
	zero := make([]byte, s.blockSize)
	for lba, data := range latest {
		nonZero := false
		for i, b := range data {
			if b != zero[i] {
				nonZero = true
				break
			}
		}
		if !nonZero {
			delete(latest, lba)
		}
	}
	return latest
}

// ScanLBAs is V2-faithful: emits each retained WAL record with its
// real write-time LSN. fromLSN is exclusive (LSN > fromLSN are
// emitted). Returns storage.RecoveryFailure(Kind=WALRecycled) when
// fromLSN is below the retention boundary.
func (s *Store) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	if fn == nil {
		return nil
	}
	s.mu.RLock()
	// Recycled gate: only fires when AdvanceWALTail has explicitly
	// trimmed below newTail. A "natural" gap (e.g. ApplyEntry
	// installed an entry at LSN=100 with no prior writes, and
	// fromLSN=0) is NOT recycled — there was simply nothing in
	// 1..99 to begin with.
	if s.recycleFloor > 0 && fromLSN+1 < s.recycleFloor {
		floor := s.recycleFloor
		s.mu.RUnlock()
		return storage.NewWALRecycledFailure(nil,
			fmt.Sprintf("fromLSN=%d recycleFloor=%d", fromLSN, floor))
	}
	snapshot := make([]walRecord, len(s.records))
	copy(snapshot, s.records)
	s.mu.RUnlock()

	for _, r := range snapshot {
		if r.LSN <= fromLSN {
			continue
		}
		cp := make([]byte, s.blockSize)
		copy(cp, r.Data)
		entry := storage.RecoveryEntry{
			LSN:   r.LSN,
			LBA:   r.LBA,
			Flags: r.Flags,
			Data:  cp,
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

// RecoveryMode reports memorywal's sub-mode: V2-faithful WAL replay.
// Distinct from BlockStore which reports state-convergence.
func (s *Store) RecoveryMode() storage.RecoveryMode {
	return storage.RecoveryModeWALReplay
}

// AppliedLSNs returns the highest LSN per LBA from the retained WAL
// records. Unlike BlockStore (which returns ErrAppliedLSNsNotTracked),
// memorywal can compute this for free from its log.
func (s *Store) AppliedLSNs() (map[uint32]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[uint32]uint64)
	for _, r := range s.records {
		if r.LSN > out[r.LBA] {
			out[r.LBA] = r.LSN
		}
	}
	return out, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// Compile-time interface assertion.
var _ storage.LogicalStorage = (*Store)(nil)
