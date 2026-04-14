package smartwal

import (
	"fmt"
	"os"
	"sort"
	"sync"
)

// ring is a slot-based circular buffer of fixed-size records inside a
// portion of an open file. All slot writes are pwrites — no seek
// state is maintained. Concurrent appends are safe because each
// caller computes its own absolute slot offset and the underlying
// fd.WriteAt is serialized only by the ring's own mutex.
//
// Slot index = LSN % capacity. This couples ring slot positions to
// the LSN counter, but for monotonically-increasing LSNs that's
// exactly the desired property: each LSN's metadata lands in a
// deterministic slot, and overflow (LSN > capacity) overwrites the
// oldest slot first.
//
// Sync is a separate concern: the ring does NOT fsync on append. The
// caller (Store.Sync) does one fsync per durability boundary, which
// covers both the extent and ring writes when they share a file.
type ring struct {
	fd       *os.File // the store file; ring lives in [base .. base+capacity*recordSize)
	base     int64    // absolute offset where the ring starts
	capacity uint64   // number of slots
	mu       sync.Mutex
}

// newRing constructs a ring view over [base .. base+capacity*recordSize).
// The caller is responsible for ensuring the file region is allocated
// and zero-filled (a fresh slot reads zeros, which decode to "empty").
func newRing(fd *os.File, base int64, capacity uint64) *ring {
	return &ring{fd: fd, base: base, capacity: capacity}
}

// appendAt writes one record into the slot determined by lsn. Since
// slot = lsn % capacity, two writes with LSNs that collide modulo
// capacity will overwrite each other — but that only happens after
// `capacity` distinct LSNs, by which point the older record's
// metadata is no longer needed (the LBA's most recent write defines
// truth).
func (r *ring) appendAt(lsn uint64, rec record) error {
	slot := lsn % r.capacity
	offset := r.base + int64(slot)*recordSize
	enc := encode(rec)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.fd.WriteAt(enc[:], offset); err != nil {
		return fmt.Errorf("smartwal: ring write slot %d: %w", slot, err)
	}
	return nil
}

// scanValid reads every slot, decodes the valid ones, and returns
// them sorted by LSN ascending. Empty/torn slots are silently
// skipped — a fresh ring returns an empty slice with no error.
//
// Used during recovery to rebuild the in-memory view.
func (r *ring) scanValid() ([]record, error) {
	totalSize := r.capacity * recordSize
	buf := make([]byte, totalSize)
	r.mu.Lock()
	_, err := r.fd.ReadAt(buf, r.base)
	r.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("smartwal: ring scan read: %w", err)
	}
	var out []record
	for i := uint64(0); i < r.capacity; i++ {
		off := i * recordSize
		rec, ok := decode(buf[off : off+recordSize])
		if !ok {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LSN < out[j].LSN })
	return out, nil
}
