package storage

import (
	"fmt"
	"sort"
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
//  1. Snapshot the dirty map.
//  2. For each entry, read the WAL record header at the recorded
//     offset and compare the on-disk LSN to the snapshot LSN. A
//     mismatch means the WAL slot was recycled — skip the entry.
//  3. Read the data section, write it to the extent at the LBA's
//     natural offset.
//  4. fsync the file once for the whole batch.
//  5. Advance the on-disk checkpoint LSN to the highest LSN that
//     was actually verified-and-flushed.
//  6. Remove flushed entries from the dirty map using
//     compare-and-delete: only drop an entry whose LSN still
//     matches what we flushed. A concurrent Write() that bumped
//     the entry's LSN must NOT have its newer data lost.
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
	stopErr  error
	flushes  atomic.Uint64 // diagnostic
	bytesOut atomic.Uint64 // diagnostic
	instr    flusherInstrumentation

	// afterSnapshotForTest, when set, runs after the dirty-map snapshot is
	// taken and the publication lock released: the window in which a
	// concurrent append is not in this cycle's checkpoint. Tests only.
	afterSnapshotForTest func()
	// beforeSnapshotForTest, when set, runs after a cycle's entry checks
	// and before it takes store.mu for the dirty-map snapshot.
	beforeSnapshotForTest func()
	// Pauses after materialization/fsync, before map proof preparation. Tests only.
	afterExtentSyncForTest func()
	// Pauses after checkpoint/reuse publication, before the outer DM cleanup.
	afterCheckpointForTest func()
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
	f.runWithStartSignal(nil)
}

func (f *flusher) runWithStartSignal(started chan<- struct{}) {
	defer close(f.doneCh)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	if started != nil {
		close(started)
	}
	var diagnostics checkpointLogLimiter
	for {
		select {
		case <-f.stopCh:
			// One last best-effort flush so a clean shutdown advances
			// the checkpoint as far as possible.
			f.stopErr = f.flushOnceAllowClosed()
			return
		case <-ticker.C:
			if err := f.flushOnce(); err != nil {
				diagnostics.report(err, time.Now())
			}
		case <-f.notifyCh:
			if err := f.flushOnce(); err != nil {
				diagnostics.report(err, time.Now())
			}
		}
	}
}

// Stop signals the flusher to stop and waits for the run loop to
// exit. Idempotent.
func (f *flusher) Stop() error {
	f.stopOnce.Do(func() {
		close(f.stopCh)
		<-f.doneCh
	})
	return f.stopErr
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
	return f.flushOnceInternal(false)
}

func (f *flusher) flushOnceAllowClosed() error {
	return f.flushOnceInternal(true)
}

func (f *flusher) flushOnceInternal(allowClosed bool) (err error) {
	cycleStart := time.Now()
	store := f.store
	if err := store.stickyIOFault(); err != nil {
		return err // no checkpoint advance after a failed fsync
	}

	if err := store.syncWALPressureV3(); err != nil {
		return fmt.Errorf("flusher: pressure WAL sync: %w", err)
	}

	if f.beforeSnapshotForTest != nil {
		f.beforeSnapshotForTest()
	}
	store.mu.RLock()
	if store.closed && !allowClosed {
		store.mu.RUnlock()
		return nil
	}
	// A store opened for chunk use drains nothing before its chunk layer
	// attaches: the dirty blocks stay in the WAL (replayed again after a
	// crash) and are served from the dirty map; the first drain after
	// attach marks them local (OnExtentWrite). This includes the final
	// flush of a stop. Checked under store.mu, with the snapshot: the owner
	// sets the hold before Recover, which takes store.mu, so a cycle that
	// started before the hold and snapshots the recovered dirty map sees it.
	if store.drainHeld.Load() {
		store.mu.RUnlock()
		return nil
	}
	snapshotStart := time.Now()
	// Hold the publication lock across all shards: a later shard must not
	// include a write whose predecessor was omitted from an earlier shard.
	// Release before WAL reads, extent writes or fsync.
	// Bind materialization to the same Recover lifetime as this snapshot.
	// Recover holds store.mu at both generation boundaries.
	var recoveryGen uint64

	recoveryGen = store.v3.recoverGen.Load()

	entries := store.dm.snapshot()
	// Write-ahead (INV-WAL-V3-WRITE-AHEAD-001): only entries whose WAL
	// record is durable reach the extent. An entry above the synced WAL
	// frontier stays dirty for a later cycle: written now, a power loss
	// could keep its extent page and lose its record, leaving extent bytes
	// that no recovered record or map entry accounts for. A cycle with no
	// synced entry is idle: no extent write and no fsync.
	synced := store.v3.wack.Load()
	store.mu.RUnlock()
	kept := entries[:0]
	for _, e := range entries {
		if e.LSN <= synced {
			kept = append(kept, e)
		}
	}
	entries = kept
	if len(entries) == 0 {

		return store.refreshW() // the mode-C idle refresh (DESIGN 6.2)

	}
	if f.afterSnapshotForTest != nil {
		f.afterSnapshotForTest()
	}
	finishCycle := f.instr.recordCycle(
		cycleStart, time.Since(snapshotStart), entries, store.sb.BlockSize,
	)
	defer func() { finishCycle(err == nil) }()

	// Validate and materialize each complete WAL record before using it for
	// checkpoint progress. Partial extent writes are safe because checkpoint
	// publication happens only after the whole snapshot and fsync succeed.
	var maxLSN uint64
	writtenEntries := make([]snapshotEntry, 0, len(entries))
	defer func() {
		f.instr.recordWrittenOpportunity(writtenEntries, store.sb.BlockSize)
	}()

	// Adjacent blocks of a native batch share one immutable, validated record.
	// Keep only one record (at most WALSize bytes), scoped to this Recover lifetime.

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].WALOffset == entries[j].WALOffset {
			return entries[i].DataOffset < entries[j].DataOffset
		}
		return entries[i].WALOffset < entries[j].WALOffset
	})
	recordCache := &flushRecordCacheV3{recoveryGen: recoveryGen}

	for _, e := range entries {
		data, _, err := f.readDirtyRecordV3Cached(e, recordCache)
		if err != nil {
			return err
		}
		writeStart := time.Now()
		written, err := store.writeExtentIfCurrent(e.LBA, e.LSN, data)
		if written || err != nil {
			f.instr.recordExtentWrite(len(data), time.Since(writeStart), err)
		}
		if err != nil {
			return fmt.Errorf("flusher: write extent LBA %d: %w", e.LBA, err)
		}
		if written {
			f.bytesOut.Add(uint64(len(data)))
			writtenEntries = append(writtenEntries, e)
		} else {
			f.instr.recordSupersededEntry()
		}
		if e.LSN > maxLSN {
			maxLSN = e.LSN
		}
	}
	// Step 2: one fsync covers all extent writes in this batch
	// (and any other in-flight WAL writes since they share a file).
	syncStart := time.Now()
	syncErr := store.syncFile(nil, func() error { return store.syncExtentFile() })
	f.instr.recordExtentSync(time.Since(syncStart), syncErr)
	if syncErr != nil {
		return fmt.Errorf("flusher: fsync after extent writes: %w", syncErr)
	}

	if f.afterExtentSyncForTest != nil {
		f.afterExtentSyncForTest()
	}

	// Step 3: advance the on-disk checkpoint to the highest LSN
	// that was successfully flushed in this cycle. Because we
	// processed the entire consistent snapshot, every entry
	// with LSN <= maxLSN is in extent or superseded by a retained newer
	// WAL record. Concurrent writes remain dirty for the next cycle.
	if maxLSN > 0 {
		// The released tail is persisted with the checkpoint, before the
		// writer may reuse those bytes.
		var publishErr error
		cpBefore := store.CheckpointLSN()

		publishErr = store.persistCheckpointV3(maxLSN, recoveryGen)

		f.instr.recordCheckpoint(publishErr, cpBefore, store.CheckpointLSN(), time.Now())
		if err := publishErr; err != nil {
			return fmt.Errorf("flusher: persist checkpoint %d: %w", maxLSN, err)
		}

	}

	if f.afterCheckpointForTest != nil {
		f.afterCheckpointForTest()
	}

	// Step 4: remove flushed entries from the dirty map using
	// compare-and-delete. If a concurrent Write() bumped an entry's
	// LSN after our snapshot, this leaves the newer entry alone.
	// V3 may have clamped its checkpoint to Wack (or held it for a map).
	// Keep the uncovered entries as retry work even though their extent
	// bytes were fsynced: a later Sync plus cycle must advance checkpoint
	// AND coverage, rather than merely refreshing W on an empty dirty map.

	// Serialize validation and LSN-only retirement with Recover. A new
	// lifetime can reuse the same LBA, physical offset and LSN.
	store.mu.Lock()
	if !store.v3.flushLifetimeValidV3(recoveryGen) {
		store.mu.Unlock()
		return mapCoverageRefusal("recovery_lifetime_changed")
	}
	checkpoint := store.checkpointLSN
	if checkpoint >= maxLSN && maxLSN+1 > store.walTail {
		store.walTail = maxLSN + 1
	}

	for _, e := range entries {
		if e.LSN > checkpoint {
			continue
		}
		store.dm.compareAndDelete(e.LBA, e.LSN)
	}

	store.mu.Unlock()

	f.flushes.Add(1)

	return store.refreshW() // the mode-C idle refresh (DESIGN 6.2)

}

func (f *flusher) readDirtyRecord(e snapshotEntry) (data []byte, entrySize uint64, err error) {

	return f.readDirtyRecordV3(e)

}
