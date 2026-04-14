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
	s.walHead = lsn
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
