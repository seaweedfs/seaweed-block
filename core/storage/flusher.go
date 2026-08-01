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
	for {
		select {
		case <-f.stopCh:
			// One last best-effort flush so a clean shutdown advances
			// the checkpoint as far as possible.
			f.stopErr = f.flushOnceAllowClosed()
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
	store.mu.RLock()
	if store.closed && !allowClosed {
		store.mu.RUnlock()
		return nil
	}
	store.mu.RUnlock()

	snapshotStart := time.Now()
	entries := store.dm.snapshot()
	if len(entries) == 0 {
		return nil
	}
	finishCycle := f.instr.recordCycle(
		cycleStart, time.Since(snapshotStart), entries, store.sb.BlockSize,
	)
	defer func() { finishCycle(err == nil) }()

	// Validate and materialize each complete WAL record before using it for
	// checkpoint progress. Partial extent writes are safe because checkpoint
	// publication happens only after the whole snapshot and fsync succeed.
	var maxLSN uint64
	var maxLSNPhys uint64
	var maxLSNEntrySize uint64
	writtenEntries := make([]snapshotEntry, 0, len(entries))
	defer func() {
		f.instr.recordWrittenOpportunity(writtenEntries, store.sb.BlockSize)
	}()

	for _, e := range entries {
		data, entrySize, err := f.readDirtyRecord(e)
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
			maxLSNPhys = e.WALOffset
			maxLSNEntrySize = entrySize
		}
	}
	// Step 2: one fsync covers all extent writes in this batch
	// (and any other in-flight WAL writes since they share a file).
	syncStart := time.Now()
	syncErr := store.fd.Sync()
	f.instr.recordExtentSync(time.Since(syncStart), syncErr)
	if syncErr != nil {
		return fmt.Errorf("flusher: fsync after extent writes: %w", syncErr)
	}

	// Step 3: advance the on-disk checkpoint to the highest LSN
	// that was successfully flushed in this cycle. Because we
	// processed the ENTIRE snapshot (no batch cutoff), every entry
	// with LSN <= maxLSN that needed flushing is in extent. Newer
	// concurrent writes have LSN > maxLSN and remain dirty.
	if maxLSN > 0 {
		if err := store.persistCheckpoint(maxLSN); err != nil {
			return fmt.Errorf("flusher: persist checkpoint %d: %w", maxLSN, err)
		}
		if store.CheckpointLSN() >= maxLSN {
			store.wal.advanceTailPastEntry(maxLSNPhys, maxLSNEntrySize)
			store.mu.Lock()
			if maxLSN+1 > store.walTail {
				store.walTail = maxLSN + 1
			}
			store.mu.Unlock()
		}
	}

	// Step 4: remove flushed entries from the dirty map using
	// compare-and-delete. If a concurrent Write() bumped an entry's
	// LSN after our snapshot, this leaves the newer entry alone.
	for _, e := range entries {
		store.dm.compareAndDelete(e.LBA, e.LSN)
	}
	f.flushes.Add(1)
	return nil
}

func (f *flusher) readDirtyRecord(e snapshotEntry) (data []byte, entrySize uint64, err error) {
	store := f.store
	readFailed := false
	defer func() {
		switch {
		case err == nil:
			f.instr.recordValidatedRecord()
		case !readFailed:
			f.instr.recordValidationFailure()
		}
	}()

	absOff := int64(store.sb.WALOffset + e.WALOffset)
	header := make([]byte, walEntryHeaderSize)
	headerStart := time.Now()
	n, readErr := store.fd.ReadAt(header, absOff)
	f.instr.recordWALHeaderRead(n, time.Since(headerStart), readErr)
	if readErr != nil {
		readFailed = true
		return nil, 0, fmt.Errorf("flusher: read WAL header at %d: %w", e.WALOffset, readErr)
	}

	entryLSN := binary.LittleEndian.Uint64(header[0:8])
	entryType := header[16]
	if entryType != walEntryWriteBatch && entryLSN != e.LSN {
		return nil, 0, fmt.Errorf(
			"flusher: WAL slot mismatch LBA %d offset %d dirty LSN %d record LSN %d",
			e.LBA, e.WALOffset, e.LSN, entryLSN)
	}
	if entryType != walEntryWrite && entryType != walEntryWriteBatch && entryType != walEntryTrim {
		return nil, 0, fmt.Errorf(
			"flusher: invalid dirty WAL record LBA %d offset %d type %d",
			e.LBA, e.WALOffset, entryType)
	}

	payloadLen := uint64(0)
	if entryType == walEntryWrite || entryType == walEntryWriteBatch {
		payloadLen = uint64(parseLengthFromHeader(header))
		if payloadLen == 0 {
			return nil, 0, fmt.Errorf(
				"flusher: invalid dirty WAL record LBA %d offset %d with zero payload",
				e.LBA, e.WALOffset)
		}
	}
	entrySize = uint64(walEntryHeaderSize) + payloadLen
	if entrySize > store.sb.WALSize || e.WALOffset+entrySize > store.sb.WALSize {
		return nil, 0, fmt.Errorf(
			"flusher: invalid dirty WAL record LBA %d offset %d size %d WAL size %d",
			e.LBA, e.WALOffset, entrySize, store.sb.WALSize)
	}

	full := make([]byte, entrySize)
	recordStart := time.Now()
	n, readErr = store.fd.ReadAt(full, absOff)
	f.instr.recordWALRecordRead(n, time.Since(recordStart), readErr)
	if readErr != nil {
		readFailed = true
		return nil, 0, fmt.Errorf("flusher: read WAL record at %d: %w", e.WALOffset, readErr)
	}

	entryLSN = binary.LittleEndian.Uint64(full[0:8])
	entryType = full[16]
	if entryType != walEntryWriteBatch && entryLSN != e.LSN {
		return nil, 0, fmt.Errorf(
			"flusher: WAL slot mismatch LBA %d offset %d dirty LSN %d record LSN %d",
			e.LBA, e.WALOffset, e.LSN, entryLSN)
	}
	if entryType != walEntryWrite && entryType != walEntryWriteBatch && entryType != walEntryTrim {
		return nil, 0, fmt.Errorf(
			"flusher: invalid dirty WAL record LBA %d offset %d type %d",
			e.LBA, e.WALOffset, entryType)
	}
	expectedRecordSize := uint64(walEntryHeaderSize)
	if entryType == walEntryWrite || entryType == walEntryWriteBatch {
		payloadLen := uint64(parseLengthFromHeader(full))
		if payloadLen == 0 {
			return nil, 0, fmt.Errorf(
				"flusher: invalid dirty WAL record LBA %d offset %d with zero payload",
				e.LBA, e.WALOffset)
		}
		expectedRecordSize += payloadLen
	}
	if expectedRecordSize != entrySize || uint64(len(full)) != entrySize {
		return nil, 0, fmt.Errorf(
			"flusher: invalid dirty WAL record LBA %d offset %d record size %d expected %d",
			e.LBA, e.WALOffset, entrySize, expectedRecordSize)
	}
	entry, err := decodeWALEntry(full)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"flusher: invalid dirty WAL record LBA %d offset %d: %w",
			e.LBA, e.WALOffset, err)
	}
	if entry.Flags != 0 {
		return nil, 0, fmt.Errorf(
			"flusher: invalid dirty WAL record LBA %d offset %d flags %d",
			e.LBA, e.WALOffset, entry.Flags)
	}
	data, err = f.validateMaterializedDirtyEntry(e, &entry)
	if err != nil {
		return nil, 0, err
	}
	return data, entrySize, nil
}

func (f *flusher) validateMaterializedDirtyEntry(e snapshotEntry, entry *walEntry) ([]byte, error) {
	blockSize := f.store.sb.BlockSize
	switch entry.Type {
	case walEntryWrite:
		if entry.LSN != e.LSN || entry.LBA != e.LBA || e.DataOffset != 0 ||
			entry.Length != blockSize || e.Length != blockSize {
			return nil, fmt.Errorf(
				"flusher: WAL slot mismatch LBA %d offset %d dirty LSN %d record LSN %d",
				e.LBA, e.WALOffset, e.LSN, entry.LSN)
		}
		return entry.Data, nil
	case walEntryWriteBatch:
		maxBlocks := uint64(^uint32(0)) / uint64(blockSize)
		if entry.Reserved == 0 || entry.Reserved > maxBlocks {
			return nil, fmt.Errorf(
				"flusher: invalid dirty WAL batch LBA %d offset %d",
				e.LBA, e.WALOffset)
		}
		expectedLength := entry.Reserved * uint64(blockSize)
		if uint64(entry.Length) != expectedLength ||
			e.DataOffset%blockSize != 0 || e.Length != blockSize {
			return nil, fmt.Errorf(
				"flusher: invalid dirty WAL batch LBA %d offset %d",
				e.LBA, e.WALOffset)
		}
		blockIndex := uint64(e.DataOffset / blockSize)
		if blockIndex >= entry.Reserved || entry.LSN+blockIndex != e.LSN ||
			entry.LBA+blockIndex != e.LBA {
			return nil, fmt.Errorf(
				"flusher: WAL slot mismatch LBA %d offset %d dirty LSN %d batch base %d block %d count %d",
				e.LBA, e.WALOffset, e.LSN, entry.LSN, blockIndex, entry.Reserved)
		}
		start := uint64(e.DataOffset)
		end := start + uint64(blockSize)
		if end > uint64(len(entry.Data)) {
			return nil, fmt.Errorf(
				"flusher: invalid dirty WAL batch LBA %d offset %d data range [%d,%d)",
				e.LBA, e.WALOffset, start, end)
		}
		return entry.Data[start:end], nil
	case walEntryTrim:
		trimBlocks := uint64(1)
		if entry.Length > 0 {
			if entry.Length%blockSize != 0 {
				return nil, fmt.Errorf(
					"flusher: invalid dirty WAL trim LBA %d offset %d length %d",
					e.LBA, e.WALOffset, entry.Length)
			}
			trimBlocks = uint64(entry.Length / blockSize)
		}
		if e.DataOffset%blockSize != 0 || e.Length != blockSize {
			return nil, fmt.Errorf(
				"flusher: invalid dirty WAL trim LBA %d offset %d",
				e.LBA, e.WALOffset)
		}
		blockIndex := uint64(e.DataOffset / blockSize)
		if entry.LSN != e.LSN || blockIndex >= trimBlocks ||
			entry.LBA+blockIndex != e.LBA {
			return nil, fmt.Errorf(
				"flusher: WAL slot mismatch trim LBA %d offset %d dirty LSN %d record LSN %d",
				e.LBA, e.WALOffset, e.LSN, entry.LSN)
		}
		return make([]byte, blockSize), nil
	default:
		panic("validated dirty WAL type became unsupported")
	}
}
