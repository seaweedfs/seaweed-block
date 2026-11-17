package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

var errWALBindingV3 = errors.New("storage: v3 WAL nonce/cursor binding refused")

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
	mu      sync.Mutex
	pub     *descriptorPublisherV3
	fd      *os.File
	nonce   [16]byte
	head    uint64
	failed  error
	writeAt func([]byte, int64) (int, error)
}

func newWALWriterV3(fd *os.File, pub *descriptorPublisherV3) (*walWriterV3, error) {
	d, _, ok := pub.walSnapshotV3()
	if err := d.validate(pub.accept); err != nil {
		return nil, err
	}
	if !ok || d.MigrationPhase != MigrationNormal {
		return nil, errWALBindingV3
	}
	return &walWriterV3{pub: pub, fd: fd, nonce: d.Nonce, head: d.RetainedHead, writeAt: fd.WriteAt}, nil
}

func (w *walWriterV3) append(e walEntryV3, durableLSN uint64) (uint64, error) {
	// Descriptor snapshot precedes the writer lock. A concurrent floor advance
	// makes this snapshot conservative; reset must quiesce/recreate the writer.
	d, floor, ok := w.pub.walSnapshotV3()
	w.mu.Lock()
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
	buf, err := encodeWALEntryV3(e, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
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
		n, err := w.writeAt(b, int64(d.WALOffset+pos%d.WALSize))
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
	// A suffix shorter than 70 is skipped geometrically, never decoded as a record.
	if err := write(buf, start); err != nil {
		return 0, err
	}
	w.head = start + size
	return start % d.WALSize, nil
}

func (w *walWriterV3) logicalHeadV3() uint64 { w.mu.Lock(); defer w.mu.Unlock(); return w.head }

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
	if r.End > counterMaxV3 {
		return ErrCounterExhausted
	}
	w.head = r.End
	return nil
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

func scanWALV3(fd io.ReaderAt, d descriptorV3) (walScanResultV3, error) {
	r := walScanResultV3{Nonce: d.Nonce}
	if err := d.validate(nil); err != nil {
		return r, err
	}
	if d.MigrationPhase != MigrationNormal {
		return r, ErrMigrationIncomplete
	}
	if d.WALSize > counterMaxV3-d.RetainedTail {
		return r, ErrCounterExhausted
	}
	limit := d.RetainedTail + d.WALSize
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
		if size > remaining || size > limit-cursor {
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
