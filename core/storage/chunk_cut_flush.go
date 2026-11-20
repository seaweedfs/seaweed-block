package storage

import "sync"

// A capture borrows one chunk's local image; a flusher borrows its durable
// transition. Both are nonblocking acquisitions: a busy local flush aborts
// the point instead of parking a guest write. Neither borrow spans remote I/O.
// Gates belong to the store so a concurrent first attach cannot bypass them.
func (s *WALStore) chunkFlushGate(idx uint64) *sync.RWMutex {
	value, _ := s.chunkFlushGates.LoadOrStore(idx, new(sync.RWMutex))
	return value.(*sync.RWMutex)
}

// Keep every skipped entry live. Other chunks still reach durable extents,
// but checkpoint/reclaim cannot cross the earliest skipped LSN. Locks are
// released only after extent fsync, descriptor publication and DM retirement.
func (s *WALStore) borrowFlushChunks(entries []snapshotEntry, blocks uint64) ([]snapshotEntry, uint64, func()) {
	if blocks == 0 {
		return entries, ^uint64(0), func() {}
	}
	borrowed := map[uint64]*sync.RWMutex{}
	skipped := map[uint64]bool{}
	for _, e := range entries {
		idx := e.LBA / blocks
		if borrowed[idx] != nil || skipped[idx] {
			continue
		}
		gate := s.chunkFlushGate(idx)
		if gate.TryLock() {
			borrowed[idx] = gate
		} else {
			skipped[idx] = true
		}
	}
	limit := ^uint64(0)
	kept := make([]snapshotEntry, 0, len(entries))
	for _, e := range entries {
		if skipped[e.LBA/blocks] {
			limit = min(limit, e.LSN-1)
			continue
		}
		kept = append(kept, e)
	}
	return kept, limit, func() {
		for _, gate := range borrowed {
			gate.Unlock()
		}
	}
}
