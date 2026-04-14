package storage

import "sync"

// dirtyEntry tracks where a single LBA's most recent value lives in
// the WAL — used by readers to fetch unflushed data and by the
// flusher to apply WAL records to the extent region.
type dirtyEntry struct {
	walOffset uint64
	lsn       uint64
	length    uint32
}

// dirtyShard is one independently-locked partition of the dirtyMap.
// Sharding cuts contention on concurrent writes.
type dirtyShard struct {
	mu sync.RWMutex
	m  map[uint64]dirtyEntry
}

// dirtyMap maps LBA → most-recent WAL location, sharded for low
// contention. Concurrency-safe; readers and writers share it.
type dirtyMap struct {
	shards []dirtyShard
	mask   uint64 // numShards-1
}

// newDirtyMap creates an empty map with numShards shards. numShards
// MUST be a power of 2 (or 1).
func newDirtyMap(numShards int) *dirtyMap {
	if numShards <= 0 {
		numShards = 1
	}
	if numShards&(numShards-1) != 0 {
		panic("storage: dirtyMap numShards must be power of 2")
	}
	shards := make([]dirtyShard, numShards)
	for i := range shards {
		shards[i].m = make(map[uint64]dirtyEntry)
	}
	return &dirtyMap{shards: shards, mask: uint64(numShards - 1)}
}

func (d *dirtyMap) shard(lba uint64) *dirtyShard { return &d.shards[lba&d.mask] }

func (d *dirtyMap) put(lba, walOffset, lsn uint64, length uint32) {
	s := d.shard(lba)
	s.mu.Lock()
	s.m[lba] = dirtyEntry{walOffset: walOffset, lsn: lsn, length: length}
	s.mu.Unlock()
}

func (d *dirtyMap) get(lba uint64) (walOffset, lsn uint64, length uint32, ok bool) {
	s := d.shard(lba)
	s.mu.RLock()
	e, found := s.m[lba]
	s.mu.RUnlock()
	if !found {
		return 0, 0, 0, false
	}
	return e.walOffset, e.lsn, e.length, true
}

func (d *dirtyMap) delete(lba uint64) {
	s := d.shard(lba)
	s.mu.Lock()
	delete(s.m, lba)
	s.mu.Unlock()
}

// snapshotEntry is one entry returned by snapshot().
type snapshotEntry struct {
	LBA       uint64
	WALOffset uint64
	LSN       uint64
	Length    uint32
}

// snapshot copies all current entries lock-free for the caller. Each
// shard is locked individually; entries are released into the result
// slice without holding any lock so the caller may safely call back
// into the map.
func (d *dirtyMap) snapshot() []snapshotEntry {
	var out []snapshotEntry
	for i := range d.shards {
		s := &d.shards[i]
		s.mu.RLock()
		for lba, e := range s.m {
			out = append(out, snapshotEntry{
				LBA: lba, WALOffset: e.walOffset, LSN: e.lsn, Length: e.length,
			})
		}
		s.mu.RUnlock()
	}
	return out
}

func (d *dirtyMap) len() int {
	n := 0
	for i := range d.shards {
		s := &d.shards[i]
		s.mu.RLock()
		n += len(s.m)
		s.mu.RUnlock()
	}
	return n
}

func (d *dirtyMap) clear() {
	for i := range d.shards {
		s := &d.shards[i]
		s.mu.Lock()
		s.m = make(map[uint64]dirtyEntry)
		s.mu.Unlock()
	}
}
