// Package storage provides a minimal block store for the V3 sparrow.
// It stores blocks in memory with LSN tracking. No persistence —
// the sparrow is a proof-of-route, not a production storage engine.
//
// For production, this would be replaced by LogicalStorage (ClassicWAL
// or SmartWAL) from the seaweedfs repo.
package storage

import (
	"fmt"
	"sync"
)

const DefaultBlockSize = 4096

// BlockStore is a minimal in-memory block store with LSN tracking.
type BlockStore struct {
	mu        sync.RWMutex
	blocks    map[uint32][]byte // LBA → data
	blockSize int
	numBlocks uint32

	nextLSN   uint64
	syncedLSN uint64 // last synced frontier

	// WAL boundary tracking for recovery classification.
	walTail uint64 // oldest retained LSN (S)
	walHead uint64 // newest written LSN (H)
}

// NewBlockStore creates an empty block store.
func NewBlockStore(numBlocks uint32, blockSize int) *BlockStore {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}
	return &BlockStore{
		blocks:    make(map[uint32][]byte),
		blockSize: blockSize,
		numBlocks: numBlocks,
		nextLSN:   1,
	}
}

// Write stores a block and advances the LSN.
func (s *BlockStore) Write(lba uint32, data []byte) (lsn uint64, err error) {
	if lba >= s.numBlocks {
		return 0, fmt.Errorf("storage: LBA %d out of range (max %d)", lba, s.numBlocks-1)
	}
	if len(data) != s.blockSize {
		return 0, fmt.Errorf("storage: data size %d != block size %d", len(data), s.blockSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]byte, s.blockSize)
	copy(cp, data)
	s.blocks[lba] = cp

	lsn = s.nextLSN
	s.nextLSN++
	s.walHead = lsn
	if s.walTail == 0 {
		s.walTail = lsn
	}
	return lsn, nil
}

// Read retrieves a block. Returns zeros if never written.
func (s *BlockStore) Read(lba uint32) ([]byte, error) {
	if lba >= s.numBlocks {
		return nil, fmt.Errorf("storage: LBA %d out of range", lba)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if data, ok := s.blocks[lba]; ok {
		cp := make([]byte, s.blockSize)
		copy(cp, data)
		return cp, nil
	}
	return make([]byte, s.blockSize), nil
}

// Sync marks all current writes as durable. Returns the synced frontier.
// In-memory storage cannot fail to "flush", so the error is always nil —
// the (uint64, error) signature matches the LogicalStorage contract so
// file-backed and SmartWAL implementations can return real fsync errors
// through the same API.
func (s *BlockStore) Sync() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextLSN > 1 {
		s.syncedLSN = s.nextLSN - 1
	}
	return s.syncedLSN, nil
}

// Recover is a no-op for the in-memory BlockStore — there is no on-disk
// state to replay. Returns (0, nil) to satisfy the LogicalStorage
// contract. File-backed and SmartWAL implementations do real work here.
func (s *BlockStore) Recover() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncedLSN, nil
}

// Close is a no-op for the in-memory BlockStore. Returns nil to satisfy
// the LogicalStorage contract.
func (s *BlockStore) Close() error {
	return nil
}

// Compile-time assertion: BlockStore must satisfy LogicalStorage so
// callers of LogicalStorage can transparently use either backend.
var _ LogicalStorage = (*BlockStore)(nil)

// ScanLBAs satisfies the LogicalStorage tier-1 recovery contract.
// BlockStore is in-memory and has NO WAL retention — it cannot
// distinguish "LBA was written at LSN N" from "LBA holds bytes that
// happen to look like X." For T4c-2 catch-up scenarios that use
// BlockStore (transport tests + calibration harness), the substrate
// behaves as a state_convergence-equivalent: emit one entry per
// currently-stored LBA, carrying the current bytes and the current
// frontier LSN. fromLSN is effectively a "scan start hint"; we
// honor the at-or-ahead-of-head shortcut (returns nil) but otherwise
// do not enforce a retention boundary. Real production substrates
// (walstore, smartwal) have proper retention + ErrWALRecycled
// semantics.
//
// Per memo §13.0a, BlockStore reports its mode as
// `RecoveryModeStateConvergence` for observability; callers that
// require V2-faithful per-LSN replay MUST NOT use BlockStore as the
// recovery substrate.
//
// Called by: transport.BlockExecutor.doCatchUp (T4c-2) when the
// primary's substrate is BlockStore (test/calibration scenarios).
// Owns: per-call snapshot of stored LBAs.
// Borrows: fn callback — caller retains.
func (s *BlockStore) ScanLBAs(fromLSN uint64, fn func(RecoveryEntry) error) error {
	if fn == nil {
		return nil
	}
	s.mu.RLock()
	if fromLSN >= s.nextLSN {
		s.mu.RUnlock()
		return nil // caller is at-or-ahead of head; nothing to ship
	}
	// Snapshot the LBA-data map under the lock.
	snapshot := make(map[uint32][]byte, len(s.blocks))
	for lba, data := range s.blocks {
		buf := make([]byte, len(data))
		copy(buf, data)
		snapshot[lba] = buf
	}
	// Synthesized scan-time LSN = walHead (the newest LSN actually
	// written). nextLSN = walHead+1 would push every entry past the
	// caller's targetLSN (typically equal to walHead) and cause the
	// catch-up callback to skip everything via the
	// `entry.LSN > targetLSN` cap.
	frontierLSN := s.walHead
	if frontierLSN == 0 {
		frontierLSN = 1
	}
	s.mu.RUnlock()

	// Emit in LBA order so behavior is deterministic across runs.
	lbas := make([]uint32, 0, len(snapshot))
	for lba := range snapshot {
		lbas = append(lbas, lba)
	}
	for i := 1; i < len(lbas); i++ {
		j := i
		for j > 0 && lbas[j-1] > lbas[j] {
			lbas[j-1], lbas[j] = lbas[j], lbas[j-1]
			j--
		}
	}
	// Use the frontier LSN as the synthesized scan-time LSN. This
	// matches smartwal state_convergence semantic: emitted LSN is
	// scan-time, not write-time.
	for _, lba := range lbas {
		entry := RecoveryEntry{
			LSN:   frontierLSN,
			LBA:   lba,
			Flags: RecoveryEntryWrite,
			Data:  snapshot[lba],
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

// RecoveryMode reports BlockStore's recovery sub-mode (T4d-4 part A;
// T4c §I row 6). BlockStore is in-memory; its ScanLBAs synthesizes
// entries from current state — closest to state-convergence.
func (s *BlockStore) RecoveryMode() RecoveryMode {
	return RecoveryModeStateConvergence
}

// AppliedLSNs satisfies the T4d-1 LogicalStorage extension. BlockStore
// is in-memory and does NOT track per-LBA applied LSN — explicit
// not-tracked return per kickoff §2.5 #1 architect Option C hybrid.
// The replica recovery apply gate (T4d-2) handles the sentinel by
// falling back to session-only tracking.
//
// Called by: T4d-2 replica recovery apply gate at session start.
// Owns: nothing (returns sentinel + nil map).
// Borrows: nothing.
func (s *BlockStore) AppliedLSNs() (map[uint32]uint64, error) {
	return nil, ErrAppliedLSNsNotTracked
}

// Boundaries returns the current R/S/H recovery boundaries.
//   - R (syncedLSN): what's durable on this node
//   - S (walTail): oldest retained LSN
//   - H (walHead): newest written LSN
func (s *BlockStore) Boundaries() (R, S, H uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncedLSN, s.walTail, s.walHead
}

// NextLSN returns the next LSN to be assigned.
func (s *BlockStore) NextLSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextLSN
}

// NumBlocks returns the block count.
func (s *BlockStore) NumBlocks() uint32 {
	return s.numBlocks
}

// BlockSize returns the block size.
func (s *BlockStore) BlockSize() int {
	return s.blockSize
}

// AdvanceFrontier advances nextLSN and walHead metadata without writing
// any block data. Used after rebuild to set the replica's frontier to
// match the primary's head without corrupting user blocks.
func (s *BlockStore) AdvanceFrontier(lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	if lsn > s.walHead {
		s.walHead = lsn
	}
}

// AdvanceWALTail moves the WAL tail forward, simulating WAL recycling.
// After this, entries before newTail are no longer available for catch-up.
func (s *BlockStore) AdvanceWALTail(newTail uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newTail > s.walTail {
		s.walTail = newTail
	}
}

// ApplyEntry applies a replicated entry from the primary.
func (s *BlockStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	if lba >= s.numBlocks {
		return fmt.Errorf("storage: apply LBA %d out of range", lba)
	}
	if len(data) != s.blockSize {
		return fmt.Errorf("storage: apply data size %d != block size %d", len(data), s.blockSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]byte, s.blockSize)
	copy(cp, data)
	s.blocks[lba] = cp

	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	// Round-43 substrate hardening (defense-in-depth): walHead is
	// the "stable frontier never goes backward" boundary per
	// LogicalStorage contract §3 rule 3. Older-LSN ApplyEntry must
	// NOT regress walHead. The replica recovery apply gate (T4d-2
	// primary fix) prevents the older-LSN apply from reaching this
	// path under recovery flow; this guard is the substrate-level
	// safety net for any path that bypasses the gate (live duplicate,
	// retry, future cross-substrate scenarios).
	if lsn > s.walHead {
		s.walHead = lsn
	}
	return nil
}

// AllBlocks returns all written LBAs and their data (for rebuild serving).
func (s *BlockStore) AllBlocks() map[uint32][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[uint32][]byte, len(s.blocks))
	for lba, data := range s.blocks {
		cp := make([]byte, len(data))
		copy(cp, data)
		result[lba] = cp
	}
	return result
}
