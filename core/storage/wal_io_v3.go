package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

var errWALBindingV3 = errors.New("storage: v3 WAL nonce/cursor binding refused")

// ErrWALLSNOrder refuses a v3 append whose first LSN does not exceed every
// previously appended LSN. This keeps the physical and logical prefixes aligned.
var ErrWALLSNOrder = errors.New("storage: v3 WAL LSN must strictly increase")

// walSnapshotV3 takes the descriptor and its reuse floor in one snapshot.
// It releases descMu before any writer lock, pwrite or callback is acquired.
func (p *descriptorPublisherV3) walSnapshotV3() (descriptorV3, uint64, bool) {
	p.descMu.Lock()
	defer p.descMu.Unlock()
	floor, ok := p.reuseFloorLocked()
	return p.cur, floor, ok
}

// walWriterV3 is dark until activation. Its caller serializes store changes,
// supplies the frozen acknowledged frontier for DurableLSN, and admits appends
// only after recovery/fence/head publication (rows 5/11). It performs pwrite,
// not Sync. A pwrite failure is terminal here and must also latch the store's
// append fault when integrated. No descriptor publication is hidden in append.
type walWriterV3 struct {
	mu         sync.Mutex
	pub        *descriptorPublisherV3
	nonce      [16]byte
	head       uint64
	highestLSN uint64 // includes the last LSN consumed by a batch
	failed     error
	writeAt    func([]byte, int64) (int, error)
	// Volatile record boundaries, never payloads. Recovery reconstructs
	// only the trusted kept prefix; a batch is one indivisible point.
	reclaim []walReclaimPointV3
	instr   *writeInstrumentation
}

type walReclaimPointV3 struct{ lastLSN, end uint64 }

func newWALWriterV3(fd *os.File, pub *descriptorPublisherV3) (*walWriterV3, error) {
	d, _, ok := pub.walSnapshotV3()
	if err := d.validate(pub.accept); err != nil {
		return nil, err
	}
	if d.WALSize > uint64(math.MaxInt64)-d.WALOffset {
		return nil, ErrCounterExhausted
	}
	if !ok || d.MigrationPhase != MigrationNormal {
		return nil, errWALBindingV3
	}
	return &walWriterV3{pub: pub, nonce: d.Nonce, head: d.RetainedHead, highestLSN: max(d.CheckpointLSN, d.BaseLSN), writeAt: fd.WriteAt}, nil
}

func (w *walWriterV3) append(e walEntryV3, durableLSN uint64) (uint64, error) {
	// Descriptor snapshot precedes the writer lock. A concurrent floor advance
	// makes this snapshot conservative; reset must quiesce/recreate the writer.
	d, floor, ok := w.pub.walSnapshotV3()
	lockStart := time.Now()
	w.mu.Lock()
	if w.instr != nil {
		w.instr.recordWALAppendLockWait(time.Since(lockStart))
	}
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, w.failed
	}
	if err := w.pub.io.sticky(); err != nil {
		return 0, err
	}
	if !ok || d.Nonce != w.nonce || w.head < floor || w.head-floor > d.WALSize {
		return 0, errWALBindingV3
	}
	// Only both-slot-released points may leave the bounded index.
	nReleased := 0
	for nReleased < len(w.reclaim) && w.reclaim[nReleased].end <= floor {
		nReleased++
	}
	if nReleased > 0 {
		copy(w.reclaim, w.reclaim[nReleased:])
		w.reclaim = w.reclaim[:len(w.reclaim)-nReleased]
	}
	if e.LSN <= w.highestLSN {
		return 0, fmt.Errorf("%w: first %d after %d", ErrWALLSNOrder, e.LSN, w.highestLSN)
	}

	e.Nonce = w.nonce
	e.LogicalStart = w.head
	e.DurableLSN = durableLSN
	n, err := e.payloadSizeV3(d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
	if err != nil {
		return 0, err
	}
	size := uint64(walEntryHeaderSizeV3) + n
	if size > d.WALSize {
		return 0, errWALFull
	}
	remaining := d.WALSize - w.head%d.WALSize
	gap := uint64(0)
	if size > remaining {
		gap = remaining
	}
	if gap > counterMaxV3-w.head || size > counterMaxV3-w.head-gap {
		return 0, ErrCounterExhausted
	}
	free := d.WALSize - (w.head - floor)
	if gap > free || size > free-gap {
		return 0, errWALFull
	}
	start := w.head + gap
	e.LogicalStart = start
	buf, err := encodeWALEntryV3Instrumented(e, d.BlockSize, d.VolumeSize/uint64(d.BlockSize), w.instr)
	if err != nil {
		return 0, err
	}
	var pad []byte
	if gap >= walEntryHeaderSizeV3 {
		if gap-walEntryHeaderSizeV3 > uint64(^uint32(0)) {
			return 0, errWALFull
		}
		pad, err = encodeWALEntryV3(walEntryV3{Type: walEntryPadding, Length: uint32(gap - walEntryHeaderSizeV3), LogicalStart: w.head, Nonce: w.nonce, Data: make([]byte, int(gap-walEntryHeaderSizeV3))}, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
		if err != nil {
			return 0, err
		}
	}
	write := func(b []byte, pos uint64) error {
		start := time.Now()
		n, err := w.writeAt(b, int64(d.WALOffset+pos%d.WALSize))
		if w.instr != nil {
			w.instr.recordWALAppend(n, time.Since(start))
		}
		if err == nil && n != len(b) {
			err = io.ErrShortWrite
		}
		if err != nil {
			w.failed = fmt.Errorf("%w: v3 append: %w", errWALPwrite, err)
			return w.failed
		}
		return nil
	}
	if len(pad) > 0 {
		if err := write(pad, w.head); err != nil {
			return 0, err
		}
	}
	if gap > 0 && w.instr != nil {
		w.instr.recordWALAppendWrap(gap)
	}
	// A suffix shorter than 70 is skipped geometrically, never decoded as a record.
	if err := write(buf, start); err != nil {
		return 0, err
	}
	w.head = start + size
	w.highestLSN = e.LSN
	if e.Type == walEntryWriteBatch {
		w.highestLSN += e.BatchCount - 1
	}
	w.reclaim = append(w.reclaim, walReclaimPointV3{w.highestLSN, w.head})
	return start % d.WALSize, nil
}

// reclaimThroughV3 returns only a complete, validated record boundary.
// Caller serializes recovery/reset with syncMu; appends can only extend
// the index with higher LSNs. No descriptor lock or I/O is taken here.
func (w *walWriterV3) reclaimThroughV3(lsn uint64) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var end uint64
	for _, point := range w.reclaim {
		if point.lastLSN > lsn {
			break
		}
		end = point.end
	}
	return end
}

func (w *walWriterV3) logicalHeadV3() uint64 { w.mu.Lock(); defer w.mu.Unlock(); return w.head }

// advanceHeadV3 moves the writer forward to p, never back. Plan row 11: a
// covering supersession after an installing reopen restarts the writer at
// P = max(E, persisted retainedHead) after the drop fence zeroed [E, ...).
// It is not a restore: no LSN state changes.
func (w *walWriterV3) advanceHeadV3(p uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p < w.head {
		return fmt.Errorf("%w: writer head %d cannot move back to %d", errWALBindingV3, w.head, p)
	}
	w.head = p
	return nil
}

// restoreScanV3 restores only the validated cursor. It does not grant admission:
// the owner must complete the durable drop fence and descriptor publication
// before another append. No ring/nonce reset or floor waiver occurs here.
func (w *walWriterV3) restoreScanV3(r walScanResultV3) error {
	d, floor, ok := w.pub.walSnapshotV3()
	w.mu.Lock()
	defer w.mu.Unlock()
	if !ok || r.Nonce != w.nonce || d.Nonce != w.nonce || r.End < floor || r.End-floor > d.WALSize {
		return errWALBindingV3
	}
	if r.End > counterMaxV3 || r.Frontier > counterMaxV3 {
		return ErrCounterExhausted
	}
	if r.Frontier < max(d.CheckpointLSN, d.BaseLSN) {
		return errWALBindingV3
	}
	w.head = r.End
	// The owner fences any dropped suffix first. Its raw HighestLSN must not
	// pin the allocator after that suffix has been discarded.
	w.highestLSN = r.Frontier
	w.indexReclaimScanLockedV3(r)
	return nil
}

// rememberReclaimScanV3 is physical-recovery bookkeeping only. It does
// NOT restore or rewind the writer position/LSN, including on an
// installing reopen whose suffix cannot be admitted until resume.
func (w *walWriterV3) rememberReclaimScanV3(r walScanResultV3) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if r.Nonce != w.nonce {
		return errWALBindingV3
	}
	w.indexReclaimScanLockedV3(r)
	return nil
}

func (w *walWriterV3) indexReclaimScanLockedV3(r walScanResultV3) {
	w.reclaim = w.reclaim[:0]
	for _, rec := range r.Records {
		if rec.End > r.End {
			break
		}
		last := rec.Entry.LSN
		if rec.Entry.Type == walEntryWriteBatch {
			last += rec.Entry.BatchCount - 1
		}
		if last > r.Frontier {
			break
		}
		if rec.Entry.Type == walEntryWrite || rec.Entry.Type == walEntryWriteBatch {
			w.reclaim = append(w.reclaim, walReclaimPointV3{last, rec.End})
		}
	}
}

type walScannedRecordV3 struct {
	Entry walEntryV3
	End   uint64
}

// walScanResultV3 is a read-only scan result, not a ready/ACK permission.
// Row 5 consumes Frontier/MaxDurableLSN for R<W* and performs fence/publication
// before replay/admission. StopCause describes the first untrusted tail record;
// it cannot be used to skip that record and resume searching inside payload.
type walScanResultV3 struct {
	Nonce                               [16]byte
	Start, End                          uint64
	Frontier, HighestLSN, MaxDurableLSN uint64
	Records                             []walScannedRecordV3
	StopCause                           error
}

func scanWALV3(fd *os.File, d descriptorV3) (walScanResultV3, error) {
	r := walScanResultV3{Nonce: d.Nonce}
	if err := d.validate(nil); err != nil {
		return r, err
	}
	info, err := fd.Stat()
	if err != nil {
		return r, err
	}
	if info.Size() < 0 || d.WALSize > uint64(info.Size()) || d.WALOffset > uint64(info.Size())-d.WALSize {
		return r, io.ErrUnexpectedEOF
	}
	if d.MigrationPhase != MigrationNormal {
		return r, ErrMigrationIncomplete
	}
	// The mathematical lap end may exceed the last representable cursor.
	// Clamp the scan, not the acknowledged history: exhaustion refuses NEW
	// appends but must not make a valid last record unrecoverable.
	limit := uint64(counterMaxV3)
	if d.WALSize <= counterMaxV3-d.RetainedTail {
		limit = d.RetainedTail + d.WALSize
	}
	start := d.RetainedTail
	base := d.CheckpointLSN
	if d.BaseState != BaseNone {
		start = max(start, d.BaseCursor)
		base = max(base, d.BaseLSN)
	}
	r.Start = start
	r.End = start
	r.Frontier = base
	r.HighestLSN = base
	for cursor := start; cursor < limit; {
		remaining := d.WALSize - cursor%d.WALSize
		if remaining < walEntryHeaderSizeV3 {
			if remaining > limit-cursor {
				break
			}
			cursor += remaining
			r.End = cursor
			continue
		}
		if limit-cursor < walEntryHeaderSizeV3 {
			break
		}
		header := make([]byte, walEntryPrefixSizeV3)
		if _, err := fd.ReadAt(header, int64(d.WALOffset+cursor%d.WALSize)); err != nil {
			if errors.Is(err, io.EOF) {
				r.StopCause = errEntryTruncated
				break
			}
			return r, err
		}
		// Check the binding BEFORE using attacker-controlled Length or count.
		le := binary.LittleEndian
		var nonce [16]byte
		copy(nonce[:], header[46:62])
		if nonce != d.Nonce || le.Uint64(header[38:46]) != cursor {
			r.StopCause = errWALBindingV3
			break
		}
		e := walEntryV3{LSN: le.Uint64(header[0:8]), BatchCount: le.Uint64(header[8:16]), Type: header[16], Flags: header[17], LBA: le.Uint64(header[18:26]), Length: le.Uint32(header[26:30]), DurableLSN: le.Uint64(header[30:38]), LogicalStart: cursor, Nonce: nonce}
		n, err := e.payloadSizeV3(d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
		if err != nil {
			r.StopCause = err
			break
		}
		size := uint64(walEntryHeaderSizeV3) + n
		if (e.Type == walEntryPadding && size != remaining) || size > remaining || size > limit-cursor {
			r.StopCause = errEntryTruncated
			break
		}
		buf := make([]byte, int(size))
		if _, err := fd.ReadAt(buf, int64(d.WALOffset+cursor%d.WALSize)); err != nil {
			if errors.Is(err, io.EOF) {
				r.StopCause = errEntryTruncated
				break
			}
			return r, err
		}
		e, err = decodeWALEntryV3(buf, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
		if err != nil {
			r.StopCause = err
			break
		}
		if e.Nonce != d.Nonce || e.LogicalStart != cursor || uint64(walEntryHeaderSizeV3+len(e.Data)) != size {
			r.StopCause = errWALBindingV3
			break
		}
		cursor += size
		r.End = cursor
		if e.Type == walEntryPadding {
			continue
		}
		r.MaxDurableLSN = max(r.MaxDurableLSN, e.DurableLSN)
		last := e.LSN
		if e.Type == walEntryWriteBatch {
			last += e.BatchCount - 1
		}
		r.HighestLSN = max(r.HighestLSN, last)
		r.Records = append(r.Records, walScannedRecordV3{Entry: e, End: cursor})
	}
	// LSN interval union, not an expansion proportional to an untrusted LSN gap
	// or batch count. Record geometry and endpoints have already been validated.
	type interval struct{ first, last uint64 }
	spans := make([]interval, 0, len(r.Records))
	for _, record := range r.Records {
		e := record.Entry
		last := e.LSN
		if e.Type == walEntryWriteBatch {
			last += e.BatchCount - 1
		}
		spans = append(spans, interval{e.LSN, last})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].first < spans[j].first })
	for _, span := range spans {
		if r.Frontier == counterMaxV3 || span.first > r.Frontier+1 {
			break
		}
		r.Frontier = max(r.Frontier, span.last)
	}
	return r, nil
}
