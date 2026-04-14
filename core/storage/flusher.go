package storage

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// flusher copies WAL entries to the extent region and frees WAL
// space. It runs as one background goroutine and can also be
// triggered manually.
//
// This is a faithful port of V2's blockvol.Flusher (without the
// CoW snapshot machinery, the io_uring batch-IO backend, and the
// replica-aware retention floor — those are V2 features outside
// V3 Phase 08 scope). The core algorithm is unchanged:
//
//   1. Snapshot the dirty map.
//   2. For each entry, read the WAL record header at the recorded
//      offset and compare the on-disk LSN to the snapshot LSN. A
//      mismatch means the WAL slot was recycled — skip the entry.
//   3. Read the data section, write it to the extent at the LBA's
//      natural offset.
//   4. fsync the file once for the whole batch.
//   5. Advance the on-disk checkpoint LSN to the highest LSN that
//      was actually verified-and-flushed.
//   6. Remove flushed entries from the dirty map using
//      compare-and-delete: only drop an entry whose LSN still
//      matches what we flushed. A concurrent Write() that bumped
//      the entry's LSN must NOT have its newer data lost.
//
// Note: V2 has no batch-size cutoff. The flusher processes the
// whole snapshot in one cycle. This was a deliberate V2 choice
// (perf-acceptable; correctness simpler). An earlier draft of
// this file added a `maxBatch` truncation; that truncation
// silently broke the checkpoint invariant — if the truncated
// portion contained entries with smaller LSNs, the checkpoint
// could advance past unflushed older records and recovery would
// later skip them. Removed.
type flusher struct {
	store    *WALStore     // the local data process this flusher serves
	interval time.Duration // upper bound on flush latency
	stopCh   chan struct{}
	doneCh   chan struct{}
	notifyCh chan struct{} // wakeup channel; bumped by admission under WAL pressure
	stopOnce sync.Once
	flushes  atomic.Uint64 // diagnostic
	bytesOut atomic.Uint64 // diagnostic
}

type flusherConfig struct {
	Interval time.Duration // default 100ms (matches V2's default)
}

func newFlusher(store *WALStore, cfg flusherConfig) *flusher {
	if cfg.Interval == 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	return &flusher{
		store:    store,
		interval: cfg.Interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		notifyCh: make(chan struct{}, 1),
	}
}

// Notify wakes the flusher for an immediate flush cycle. Idempotent
// — multiple Notify calls coalesce into one wakeup (the channel has
// buffer 1).
func (f *flusher) Notify() {
	select {
	case f.notifyCh <- struct{}{}:
	default:
	}
}

// NotifyUrgent wakes the flusher for an urgent flush (WAL pressure).
// Today delegates to Notify(); V2 keeps the distinct name so a future
// priority channel can differentiate pressure-driven wakes from
// scheduled ticks. Port-faithful to weed/storage/blockvol/flusher.go.
func (f *flusher) NotifyUrgent() {
	f.Notify()
}

// run drives the flush loop. Wakes on either the periodic ticker
// or an explicit Notify(). Call once in a goroutine.
func (f *flusher) run() {
	defer close(f.doneCh)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-f.stopCh:
			// One last best-effort flush so a clean shutdown advances
			// the checkpoint as far as possible.
			_ = f.flushOnce()
			return
		case <-ticker.C:
			if err := f.flushOnce(); err != nil {
				log.Printf("storage: flusher: %v", err)
			}
		case <-f.notifyCh:
			if err := f.flushOnce(); err != nil {
				log.Printf("storage: flusher (notify): %v", err)
			}
		}
	}
}

// Stop signals the flusher to stop and waits for the run loop to
// exit. Idempotent.
func (f *flusher) Stop() {
	f.stopOnce.Do(func() {
		close(f.stopCh)
		<-f.doneCh
	})
}

func (f *flusher) FlushCount() uint64   { return f.flushes.Load() }
func (f *flusher) BytesFlushed() uint64 { return f.bytesOut.Load() }

// flushOnce performs one flush cycle. Returns nil if there was
// nothing to flush, or surfaces extent-write / fsync errors.
//
// Algorithm faithful to V2 weed/storage/blockvol/flusher.go
// FlushOnce. See type doc for the full sequence and the rationale
// for each invariant.
func (f *flusher) flushOnce() error {
	store := f.store
	store.mu.RLock()
	if store.closed {
		store.mu.RUnlock()
		return nil
	}
	store.mu.RUnlock()

	entries := store.dm.snapshot()
	if len(entries) == 0 {
		return nil
	}

	// Step 1: read every WAL header. For each entry, validate that
	// the on-disk LSN at the recorded WAL offset still matches the
	// snapshot LSN. A mismatch means the WAL slot was recycled
	// (the dirty map points at a slot whose contents have changed)
	// — skip such entries; they'll fall out via compare-and-delete
	// below.
	type pending struct {
		idx      int
		dataLen  uint32
		isWrite  bool
	}
	var pendings []pending
	var maxLSN uint64

	for i, e := range entries {
		hdrBuf := make([]byte, walEntryHeaderSize)
		absOff := int64(store.sb.WALOffset + e.WALOffset)
		if _, err := store.fd.ReadAt(hdrBuf, absOff); err != nil {
			return fmt.Errorf("flusher: read WAL header at %d: %w", e.WALOffset, err)
		}
		entryLSN := binary.LittleEndian.Uint64(hdrBuf[0:8])
		if entryLSN != e.LSN {
			// Stale slot — WAL has been recycled at this offset.
			// Drop the dirty map entry only if it still matches the
			// stale snapshot we just observed (compare-and-delete).
			store.dm.compareAndDelete(e.LBA, e.LSN)
			continue
		}
		entryType := hdrBuf[16]
		switch entryType {
		case walEntryWrite:
			dataLen := parseLengthFromHeader(hdrBuf)
			if dataLen == 0 {
				continue
			}
			pendings = append(pendings, pending{idx: i, dataLen: dataLen, isWrite: true})
		case walEntryTrim:
			pendings = append(pendings, pending{idx: i, isWrite: false})
		default:
			// Padding / barrier — nothing to flush.
			continue
		}
		if e.LSN > maxLSN {
			maxLSN = e.LSN
		}
	}

	// Step 2: for each verified entry, read the data section and
	// pwrite it into the extent at the LBA's natural offset.
	for _, p := range pendings {
		e := entries[p.idx]
		var data []byte
		if p.isWrite {
			d, err := store.readFromWAL(e.WALOffset)
			if err != nil {
				return fmt.Errorf("flusher: read WAL data for LBA %d: %w", e.LBA, err)
			}
			data = d
		} else {
			// Trim — extent destination is zeros.
			data = make([]byte, store.sb.BlockSize)
		}
		if err := store.writeExtent(uint32(e.LBA), data); err != nil {
			return fmt.Errorf("flusher: write extent LBA %d: %w", e.LBA, err)
		}
		f.bytesOut.Add(uint64(len(data)))
	}

	// Step 3: one fsync covers all extent writes in this batch
	// (and any other in-flight WAL writes since they share a file).
	if err := store.fd.Sync(); err != nil {
		return fmt.Errorf("flusher: fsync after extent writes: %w", err)
	}

	// Step 4: advance the on-disk checkpoint to the highest LSN
	// that was successfully flushed in this cycle. Because we
	// processed the ENTIRE snapshot (no batch cutoff), every entry
	// with LSN <= maxLSN that needed flushing is in extent. Newer
	// concurrent writes have LSN > maxLSN and remain dirty.
	if maxLSN > 0 {
		if err := store.persistCheckpoint(maxLSN); err != nil {
			return fmt.Errorf("flusher: persist checkpoint %d: %w", maxLSN, err)
		}
	}

	// Step 5: remove flushed entries from the dirty map using
	// compare-and-delete. If a concurrent Write() bumped an entry's
	// LSN after our snapshot, this leaves the newer entry alone.
	for _, p := range pendings {
		e := entries[p.idx]
		store.dm.compareAndDelete(e.LBA, e.LSN)
	}
	f.flushes.Add(1)
	return nil
}
