package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
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
}

// newWALWriter constructs a writer over an open file. logicalHead and
// logicalTail come from the superblock when reopening; both 0 for a
// fresh store.
func newWALWriter(fd *os.File, walOffset, walSize, head, tail uint64) *walWriter {
	return &walWriter{
		fd:          fd,
		walOffset:   walOffset,
		walSize:     walSize,
		logicalHead: head,
		logicalTail: tail,
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
	buf, err := entry.encode()
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
		w.logicalHead += remaining
		physHead = 0
	}
	if w.used()+entryLen > w.walSize {
		return 0, errWALFull
	}
	absOffset := int64(w.walOffset + physHead)
	if _, err := w.fd.WriteAt(buf, absOffset); err != nil {
		return 0, fmt.Errorf("walWriter.append: pwrite at %d: %w", absOffset, err)
	}
	writeOffset := physHead
	w.logicalHead += entryLen
	return writeOffset, nil
}

// writePadding fills [physPos, physPos+size) with a padding entry so
// the next write starts at a clean offset. If size is too small for a
// real header, just zeros the bytes — the recovery scanner skips
// header-too-short tails.
func (w *walWriter) writePadding(size, physPos uint64) error {
	if size < walEntryHeaderSize {
		buf := make([]byte, size)
		_, err := w.fd.WriteAt(buf, int64(w.walOffset+physPos))
		return err
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
	_, err := w.fd.WriteAt(buf, int64(w.walOffset+physPos))
	return err
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
