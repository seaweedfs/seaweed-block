package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

var (
	// errWALFull is returned when an Append request would overflow the
	// circular WAL region. The caller is expected to either wait for
	// the flusher to advance the tail, or escalate.
	errWALFull = errors.New("storage: WAL region full")
)

// walWriter appends entries to the circular WAL region of a store
// file. It tracks position with monotonically-increasing logical
// counters so head==tail unambiguously means empty (used = head-tail).
//
// The writer performs ONLY pwrite — fsync is the caller's
// responsibility, typically batched through a group committer for
// throughput.
type walWriter struct {
	mu          sync.Mutex
	fd          *os.File
	walOffset   uint64 // absolute file offset where WAL region starts
	walSize     uint64 // size of the WAL region in bytes
	logicalHead uint64 // monotonic write position
	logicalTail uint64 // monotonic flush position
	instr       *writeInstrumentation
}

// newWALWriter constructs a writer over an open file. logicalHead and
// logicalTail come from the superblock when reopening; both 0 for a
// fresh store.
func newWALWriter(fd *os.File, walOffset, walSize, head, tail uint64, instr *writeInstrumentation) *walWriter {
	return &walWriter{
		fd:          fd,
		walOffset:   walOffset,
		walSize:     walSize,
		logicalHead: head,
		logicalTail: tail,
		instr:       instr,
	}
}

func (w *walWriter) physicalPos(logical uint64) uint64 { return logical % w.walSize }

func (w *walWriter) used() uint64 { return w.logicalHead - w.logicalTail }

// append writes one entry and returns the WAL-relative offset where
// it was placed. If the entry would straddle the end of the WAL
// region, a padding entry is written first and the real entry starts
// at physical offset 0.
//
// Returns errWALFull when even the padding+entry would not fit.
func (w *walWriter) append(entry *walEntry) (walRelOffset uint64, err error) {
	buf, err := entry.encodeWithInstrumentation(w.instr)
	if err != nil {
		return 0, fmt.Errorf("walWriter.append: encode: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	entryLen := uint64(len(buf))
	if entryLen > w.walSize {
		return 0, fmt.Errorf("%w: entry size %d exceeds WAL size %d", errWALFull, entryLen, w.walSize)
	}

	physHead := w.physicalPos(w.logicalHead)
	remaining := w.walSize - physHead
	if remaining < entryLen {
		// Pad to end of region and wrap.
		if w.used()+remaining+entryLen > w.walSize {
			return 0, errWALFull
		}
		if err := w.writePadding(remaining, physHead); err != nil {
			return 0, fmt.Errorf("walWriter.append: padding: %w", err)
		}
		if w.instr != nil {
			w.instr.recordWALAppendWrap(remaining)
		}
		w.logicalHead += remaining
		physHead = 0
	}
	if w.used()+entryLen > w.walSize {
		return 0, errWALFull
	}
	absOffset := int64(w.walOffset + physHead)
	writeStart := time.Now()
	if _, err := w.fd.WriteAt(buf, absOffset); err != nil {
		return 0, fmt.Errorf("walWriter.append: pwrite at %d: %w", absOffset, err)
	}
	if w.instr != nil {
		w.instr.recordWALAppend(len(buf), time.Since(writeStart))
	}
	writeOffset := physHead
	w.logicalHead += entryLen
	return writeOffset, nil
}

// appendBatch writes entries under one WAL-writer critical section. Each entry
// remains independently encoded/recoverable; the batch only coalesces adjacent
// bytes into fewer pwrite calls when the circular WAL layout allows it.
func (w *walWriter) appendBatch(entries []walEntry) ([]uint64, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	lengths := make([]uint64, len(entries))
	for i := range entries {
		entryLen, err := entries[i].encodedSize()
		if err != nil {
			return nil, fmt.Errorf("walWriter.appendBatch: validate entry %d: %w", i, err)
		}
		if uint64(entryLen) > w.walSize {
			return nil, fmt.Errorf("%w: entry size %d exceeds WAL size %d", errWALFull, entryLen, w.walSize)
		}
		lengths[i] = uint64(entryLen)
	}
	pendingCapacity := boundedPendingCapacity(lengths)

	w.mu.Lock()
	defer w.mu.Unlock()

	offsets, finalHead, err := w.planAppendBatchLengths(lengths)
	if err != nil {
		return offsets, err
	}

	localHead := w.logicalHead
	var pending []byte
	var pendingStart uint64
	flushPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		writeStart := time.Now()
		if _, err := w.fd.WriteAt(pending, int64(w.walOffset+pendingStart)); err != nil {
			return fmt.Errorf("walWriter.appendBatch: pwrite at %d: %w", w.walOffset+pendingStart, err)
		}
		if w.instr != nil {
			w.instr.recordWALAppend(len(pending), time.Since(writeStart))
		}
		pending = nil
		return nil
	}
	appendPendingBytes := func(phys uint64, buf []byte) error {
		if len(buf) == 0 {
			return nil
		}
		if len(pending) == 0 {
			pendingStart = phys
			pending = append(pending, buf...)
			return nil
		}
		if pendingStart+uint64(len(pending)) != phys {
			if err := flushPending(); err != nil {
				return err
			}
			pendingStart = phys
		}
		pending = append(pending, buf...)
		return nil
	}
	appendPendingEntry := func(phys uint64, entry *walEntry, entryLen uint64) error {
		if len(pending) == 0 {
			pendingStart = phys
			pending = make([]byte, 0, pendingCapacity)
		} else if pendingStart+uint64(len(pending)) != phys {
			if err := flushPending(); err != nil {
				return err
			}
			pendingStart = phys
			pending = make([]byte, 0, pendingCapacity)
		}
		next, err := entry.appendEncoded(pending, w.instr)
		if err != nil {
			return err
		}
		pending = next
		return nil
	}

	for i := range entries {
		entry := &entries[i]
		entryLen := lengths[i]
		physHead := w.physicalPos(localHead)
		remaining := w.walSize - physHead
		if remaining < entryLen {
			padding, err := encodeWALPadding(remaining)
			if err != nil {
				return offsets, fmt.Errorf("walWriter.appendBatch: padding: %w", err)
			}
			if err := appendPendingBytes(physHead, padding); err != nil {
				return offsets, err
			}
			if w.instr != nil {
				w.instr.recordWALAppendWrap(remaining)
			}
			localHead += remaining
			physHead = 0
		}
		if err := appendPendingEntry(physHead, entry, entryLen); err != nil {
			return offsets, err
		}
		localHead += entryLen
	}
	if err := flushPending(); err != nil {
		return offsets, err
	}
	w.logicalHead = finalHead
	return offsets, nil
}

func boundedPendingCapacity(lengths []uint64) int {
	const maxPendingCapacity = 1 << 20
	total := 0
	for _, length := range lengths {
		if length > uint64(maxPendingCapacity) {
			return maxPendingCapacity
		}
		total += int(length)
		if total >= maxPendingCapacity {
			return maxPendingCapacity
		}
	}
	if total == 0 {
		return 0
	}
	return total
}

func (w *walWriter) planAppendBatchLengths(lengths []uint64) ([]uint64, uint64, error) {
	localHead := w.logicalHead
	offsets := make([]uint64, 0, len(lengths))
	used := func() uint64 { return localHead - w.logicalTail }
	for _, entryLen := range lengths {
		physHead := w.physicalPos(localHead)
		remaining := w.walSize - physHead
		if remaining < entryLen {
			if used()+remaining+entryLen > w.walSize {
				return offsets, localHead, errWALFull
			}
			localHead += remaining
			physHead = 0
		}
		if used()+entryLen > w.walSize {
			return offsets, localHead, errWALFull
		}
		offsets = append(offsets, physHead)
		localHead += entryLen
	}
	return offsets, localHead, nil
}

// writePadding fills [physPos, physPos+size) with a padding entry so
// the next write starts at a clean offset. If size is too small for a
// real header, just zeros the bytes — the recovery scanner skips
// header-too-short tails.
func (w *walWriter) writePadding(size, physPos uint64) error {
	buf, err := encodeWALPadding(size)
	if err != nil {
		return err
	}
	if len(buf) == 0 {
		return nil
	}
	if _, err := w.fd.WriteAt(buf, int64(w.walOffset+physPos)); err != nil {
		return err
	}
	if w.instr != nil {
		w.instr.recordWALAppendWriteAt(len(buf))
	}
	return nil
}

func encodeWALPadding(size uint64) ([]byte, error) {
	if size < walEntryHeaderSize {
		return make([]byte, size), nil
	}
	buf := make([]byte, size)
	le := binary.LittleEndian
	off := 0
	le.PutUint64(buf[off:], 0) // LSN
	off += 8
	le.PutUint64(buf[off:], 0) // Reserved
	off += 8
	buf[off] = walEntryPadding
	off++
	buf[off] = 0 // Flags
	off++
	le.PutUint64(buf[off:], 0) // LBA
	off += 8
	paddingDataLen := uint32(size) - uint32(walEntryHeaderSize)
	le.PutUint32(buf[off:], paddingDataLen)
	off += 4
	dataEnd := off + int(paddingDataLen)
	crc := crc32.ChecksumIEEE(buf[:dataEnd])
	le.PutUint32(buf[dataEnd:], crc)
	le.PutUint32(buf[dataEnd+4:], uint32(size))
	return buf, nil
}

// advanceTail moves the tail forward by (newPhysTail - currentPhysTail
// modulo walSize), freeing space. Called by the flusher after entries
// have been applied to the extent region.
func (w *walWriter) advanceTail(newPhysTail uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	physTail := w.physicalPos(w.logicalTail)
	var advance uint64
	if newPhysTail >= physTail {
		advance = newPhysTail - physTail
	} else {
		advance = w.walSize - physTail + newPhysTail
	}
	w.logicalTail += advance
}

// advanceTailPastEntry frees WAL bytes through the entry at entryPhys
// with entryLen bytes. It advances in logical space, so entries that
// end at the same physical offset as the current tail still release a
// wrapped span.
func (w *walWriter) advanceTailPastEntry(entryPhys, entryLen uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	physTail := w.physicalPos(w.logicalTail)
	var distanceToEntry uint64
	if entryPhys >= physTail {
		distanceToEntry = entryPhys - physTail
	} else {
		distanceToEntry = w.walSize - physTail + entryPhys
	}
	advance := distanceToEntry + entryLen
	if advance > w.used() {
		// The entry is no longer within the live WAL window. This
		// makes repeated flush/checkpoint cycles idempotent instead
		// of wrapping around and releasing unflushed entries.
		return
	}
	w.logicalTail += advance
}

// reset truncates the writer to empty. Used after recovery decides
// the WAL region should start fresh.
func (w *walWriter) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logicalHead = 0
	w.logicalTail = 0
}

func (w *walWriter) head() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.physicalPos(w.logicalHead)
}

func (w *walWriter) tail() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.physicalPos(w.logicalTail)
}

func (w *walWriter) logicalHeadValue() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.logicalHead
}

func (w *walWriter) logicalTailValue() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.logicalTail
}

// fsync forces buffered data to durable storage.
func (w *walWriter) fsync() error { return w.fd.Sync() }

// usedFraction returns the fraction of the WAL region currently in
// use, in the range [0, 1]. Used by the admission controller to
// decide whether to throttle or block writers.
func (w *walWriter) usedFraction() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.walSize == 0 {
		return 0
	}
	return float64(w.used()) / float64(w.walSize)
}
