package parallelwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	segmentHeaderSize       = 64
	segmentEntryHeaderSize  = 32
	segmentMagic            = "PWSG"
	segmentVersion          = 1
	maxSegmentEntries       = 256
	maxSegmentPayloadBytes  = 1 << 20
	segmentHeaderCRCOffset  = 60
	segmentEntriesCRCOffset = 48
)

var (
	errBadSegment    = errors.New("parallelwal: invalid segment")
	errSegmentBounds = errors.New("parallelwal: segment exceeds bounds")
)

type segmentHeader struct {
	Size       uint32
	EntryCount uint32
	BlockSize  uint32
	FirstLSN   uint64
	LastLSN    uint64
	Sequence   uint64
}

type walSegment struct {
	Sequence uint64
	Records  []walRecord
}

// segmentRecoveryWindow is the trusted manifest that a future dual-header
// generation will persist. CommittedBytes alone cannot prove that the logical
// prefix starts and ends at the expected sequence and LSN.
type segmentRecoveryWindow struct {
	StartOffset    int64
	CommittedBytes int64
	SegmentCount   uint64
	FirstSequence  uint64
	FirstLSN       uint64
	LastLSN        uint64
}

func (w segmentRecoveryWindow) validate() error {
	if w.StartOffset < 0 || w.CommittedBytes < 0 ||
		w.StartOffset > int64(^uint64(0)>>1)-w.CommittedBytes {
		return fmt.Errorf("%w: invalid physical window offset=%d bytes=%d",
			errBadSegment, w.StartOffset, w.CommittedBytes)
	}
	if w.CommittedBytes == 0 {
		if w.SegmentCount != 0 || w.FirstSequence != 0 || w.FirstLSN != 0 || w.LastLSN != 0 {
			return fmt.Errorf("%w: non-empty manifest for empty committed prefix", errBadSegment)
		}
		return nil
	}
	if w.SegmentCount == 0 || w.FirstSequence == 0 || w.FirstLSN == 0 || w.LastLSN < w.FirstLSN {
		return fmt.Errorf("%w: invalid recovery window %+v", errBadSegment, w)
	}
	if w.SegmentCount > uint64(w.CommittedBytes/segmentHeaderSize) {
		return fmt.Errorf("%w: segment count %d exceeds committed bytes %d",
			errBadSegment, w.SegmentCount, w.CommittedBytes)
	}
	if w.FirstSequence > ^uint64(0)-(w.SegmentCount-1) {
		return fmt.Errorf("%w: segment sequence range overflows", errBadSegment)
	}
	if w.LastLSN-w.FirstLSN+1 < w.SegmentCount {
		return fmt.Errorf("%w: LSN range [%d,%d] cannot contain %d non-empty segments",
			errBadSegment, w.FirstLSN, w.LastLSN, w.SegmentCount)
	}
	return nil
}

func segmentEncodedSize(entryCount, blockSize uint32) (uint32, error) {
	if entryCount == 0 || entryCount > maxSegmentEntries {
		return 0, fmt.Errorf("%w: entry count %d", errSegmentBounds, entryCount)
	}
	if blockSize == 0 || blockSize > maxSegmentPayloadBytes {
		return 0, fmt.Errorf("%w: block size %d", errSegmentBounds, blockSize)
	}
	payloadBytes := uint64(entryCount) * uint64(blockSize)
	if payloadBytes > maxSegmentPayloadBytes {
		return 0, fmt.Errorf("%w: payload bytes %d", errSegmentBounds, payloadBytes)
	}
	total := uint64(segmentHeaderSize) +
		uint64(entryCount)*segmentEntryHeaderSize +
		payloadBytes
	if total > uint64(^uint32(0)) {
		return 0, fmt.Errorf("%w: encoded bytes %d", errSegmentBounds, total)
	}
	return uint32(total), nil
}

func encodeSegment(sequence uint64, records []walRecord, blockSize, numBlocks uint32) ([]byte, error) {
	if sequence == 0 {
		return nil, fmt.Errorf("%w: sequence must be positive", errBadSegment)
	}
	if len(records) == 0 || uint64(len(records)) > uint64(maxSegmentEntries) {
		return nil, fmt.Errorf("%w: entry count %d", errSegmentBounds, len(records))
	}
	entryCount := uint32(len(records))
	totalSize, err := segmentEncodedSize(entryCount, blockSize)
	if err != nil {
		return nil, err
	}
	firstLSN := records[0].LSN
	if firstLSN == 0 || firstLSN > ^uint64(0)-uint64(entryCount-1) {
		return nil, fmt.Errorf("%w: invalid first LSN %d", errBadSegment, firstLSN)
	}
	if numBlocks == 0 {
		return nil, fmt.Errorf("%w: numBlocks must be positive", errBadSegment)
	}

	buf := make([]byte, totalSize)
	payloadStart := segmentHeaderSize + int(entryCount)*segmentEntryHeaderSize
	for i, rec := range records {
		wantLSN := firstLSN + uint64(i)
		if rec.LSN != wantLSN {
			return nil, fmt.Errorf("%w: entry %d LSN=%d want=%d",
				errBadSegment, i, rec.LSN, wantLSN)
		}
		if rec.LBA >= numBlocks {
			return nil, fmt.Errorf("%w: entry %d LBA=%d numBlocks=%d",
				errBadSegment, i, rec.LBA, numBlocks)
		}
		if rec.Flags != flagWrite {
			return nil, fmt.Errorf("%w: entry %d flags=%d", errBadSegment, i, rec.Flags)
		}
		if len(rec.Data) != int(blockSize) {
			return nil, fmt.Errorf("%w: entry %d payload=%d blockSize=%d",
				errBadSegment, i, len(rec.Data), blockSize)
		}

		entryOffset := segmentHeaderSize + i*segmentEntryHeaderSize
		payloadOffset := payloadStart + i*int(blockSize)
		entry := buf[entryOffset : entryOffset+segmentEntryHeaderSize]
		payload := buf[payloadOffset : payloadOffset+int(blockSize)]
		copy(payload, rec.Data)
		binary.LittleEndian.PutUint64(entry[0:8], rec.LSN)
		binary.LittleEndian.PutUint32(entry[8:12], rec.LBA)
		binary.LittleEndian.PutUint16(entry[12:14], rec.Flags)
		binary.LittleEndian.PutUint32(entry[16:20], uint32(payloadOffset))
		binary.LittleEndian.PutUint32(entry[20:24], blockSize)
		binary.LittleEndian.PutUint32(entry[24:28], crc32.ChecksumIEEE(payload))
		entryCRC := crc32.NewIEEE()
		_, _ = entryCRC.Write(entry[:28])
		_, _ = entryCRC.Write(payload)
		binary.LittleEndian.PutUint32(entry[28:32], entryCRC.Sum32())
	}

	copy(buf[0:4], segmentMagic)
	binary.LittleEndian.PutUint16(buf[4:6], segmentVersion)
	binary.LittleEndian.PutUint16(buf[6:8], segmentHeaderSize)
	binary.LittleEndian.PutUint32(buf[8:12], totalSize)
	binary.LittleEndian.PutUint32(buf[12:16], entryCount)
	binary.LittleEndian.PutUint32(buf[16:20], blockSize)
	binary.LittleEndian.PutUint64(buf[24:32], firstLSN)
	binary.LittleEndian.PutUint64(buf[32:40], firstLSN+uint64(entryCount-1))
	binary.LittleEndian.PutUint64(buf[40:48], sequence)
	binary.LittleEndian.PutUint32(buf[segmentEntriesCRCOffset:52],
		crc32.ChecksumIEEE(buf[segmentHeaderSize:]))
	binary.LittleEndian.PutUint32(buf[segmentHeaderCRCOffset:64],
		crc32.ChecksumIEEE(buf[:segmentHeaderCRCOffset]))
	return buf, nil
}

func decodeSegmentHeader(buf []byte, expectedBlockSize uint32) (segmentHeader, error) {
	if len(buf) < segmentHeaderSize {
		return segmentHeader{}, fmt.Errorf("%w: truncated header size=%d", errBadSegment, len(buf))
	}
	buf = buf[:segmentHeaderSize]
	if string(buf[0:4]) != segmentMagic {
		return segmentHeader{}, fmt.Errorf("%w: bad magic", errBadSegment)
	}
	if got := binary.LittleEndian.Uint16(buf[4:6]); got != segmentVersion {
		return segmentHeader{}, fmt.Errorf("%w: version=%d", errBadSegment, got)
	}
	if got := binary.LittleEndian.Uint16(buf[6:8]); got != segmentHeaderSize {
		return segmentHeader{}, fmt.Errorf("%w: header size=%d", errBadSegment, got)
	}
	if got, want := binary.LittleEndian.Uint32(buf[segmentHeaderCRCOffset:64]),
		crc32.ChecksumIEEE(buf[:segmentHeaderCRCOffset]); got != want {
		return segmentHeader{}, fmt.Errorf("%w: header CRC got=%08x want=%08x", errBadSegment, got, want)
	}
	if flags := binary.LittleEndian.Uint32(buf[20:24]); flags != 0 {
		return segmentHeader{}, fmt.Errorf("%w: header flags=%d", errBadSegment, flags)
	}
	if reserved := binary.LittleEndian.Uint64(buf[52:60]); reserved != 0 {
		return segmentHeader{}, fmt.Errorf("%w: reserved=%d", errBadSegment, reserved)
	}
	h := segmentHeader{
		Size:       binary.LittleEndian.Uint32(buf[8:12]),
		EntryCount: binary.LittleEndian.Uint32(buf[12:16]),
		BlockSize:  binary.LittleEndian.Uint32(buf[16:20]),
		FirstLSN:   binary.LittleEndian.Uint64(buf[24:32]),
		LastLSN:    binary.LittleEndian.Uint64(buf[32:40]),
		Sequence:   binary.LittleEndian.Uint64(buf[40:48]),
	}
	if h.BlockSize != expectedBlockSize {
		return segmentHeader{}, fmt.Errorf("%w: block size=%d want=%d",
			errBadSegment, h.BlockSize, expectedBlockSize)
	}
	wantSize, err := segmentEncodedSize(h.EntryCount, h.BlockSize)
	if err != nil {
		return segmentHeader{}, err
	}
	if h.Size != wantSize {
		return segmentHeader{}, fmt.Errorf("%w: size=%d want=%d", errBadSegment, h.Size, wantSize)
	}
	if h.Sequence == 0 || h.FirstLSN == 0 ||
		h.FirstLSN > ^uint64(0)-uint64(h.EntryCount-1) ||
		h.LastLSN != h.FirstLSN+uint64(h.EntryCount-1) {
		return segmentHeader{}, fmt.Errorf("%w: sequence=%d LSN range=[%d,%d] entries=%d",
			errBadSegment, h.Sequence, h.FirstLSN, h.LastLSN, h.EntryCount)
	}
	return h, nil
}

func decodeSegment(buf []byte, blockSize, numBlocks uint32) (walSegment, error) {
	h, err := decodeSegmentHeader(buf, blockSize)
	if err != nil {
		return walSegment{}, err
	}
	if len(buf) != int(h.Size) {
		return walSegment{}, fmt.Errorf("%w: bytes=%d want=%d", errBadSegment, len(buf), h.Size)
	}
	if numBlocks == 0 {
		return walSegment{}, fmt.Errorf("%w: numBlocks must be positive", errBadSegment)
	}
	if got, want := binary.LittleEndian.Uint32(buf[segmentEntriesCRCOffset:52]),
		crc32.ChecksumIEEE(buf[segmentHeaderSize:]); got != want {
		return walSegment{}, fmt.Errorf("%w: entries CRC got=%08x want=%08x", errBadSegment, got, want)
	}

	records := make([]walRecord, h.EntryCount)
	payloadStart := segmentHeaderSize + int(h.EntryCount)*segmentEntryHeaderSize
	for i := range records {
		entryOffset := segmentHeaderSize + i*segmentEntryHeaderSize
		entry := buf[entryOffset : entryOffset+segmentEntryHeaderSize]
		wantPayloadOffset := payloadStart + i*int(blockSize)
		payloadOffset := binary.LittleEndian.Uint32(entry[16:20])
		payloadSize := binary.LittleEndian.Uint32(entry[20:24])
		if payloadOffset != uint32(wantPayloadOffset) || payloadSize != blockSize {
			return walSegment{}, fmt.Errorf("%w: entry %d payload offset/size=%d/%d want=%d/%d",
				errBadSegment, i, payloadOffset, payloadSize, wantPayloadOffset, blockSize)
		}
		payloadEnd := uint64(payloadOffset) + uint64(payloadSize)
		if payloadEnd > uint64(len(buf)) {
			return walSegment{}, fmt.Errorf("%w: entry %d payload end=%d size=%d",
				errBadSegment, i, payloadEnd, len(buf))
		}
		payload := buf[int(payloadOffset):int(payloadEnd)]
		if got, want := crc32.ChecksumIEEE(payload), binary.LittleEndian.Uint32(entry[24:28]); got != want {
			return walSegment{}, fmt.Errorf("%w: entry %d data CRC got=%08x want=%08x",
				errBadSegment, i, got, want)
		}
		entryCRC := crc32.NewIEEE()
		_, _ = entryCRC.Write(entry[:28])
		_, _ = entryCRC.Write(payload)
		if got, want := entryCRC.Sum32(), binary.LittleEndian.Uint32(entry[28:32]); got != want {
			return walSegment{}, fmt.Errorf("%w: entry %d CRC got=%08x want=%08x",
				errBadSegment, i, got, want)
		}
		lsn := binary.LittleEndian.Uint64(entry[0:8])
		if want := h.FirstLSN + uint64(i); lsn != want {
			return walSegment{}, fmt.Errorf("%w: entry %d LSN=%d want=%d",
				errBadSegment, i, lsn, want)
		}
		lba := binary.LittleEndian.Uint32(entry[8:12])
		if lba >= numBlocks {
			return walSegment{}, fmt.Errorf("%w: entry %d LBA=%d numBlocks=%d",
				errBadSegment, i, lba, numBlocks)
		}
		flags := binary.LittleEndian.Uint16(entry[12:14])
		if flags != flagWrite || binary.LittleEndian.Uint16(entry[14:16]) != 0 {
			return walSegment{}, fmt.Errorf("%w: entry %d flags/reserved=%d/%d",
				errBadSegment, i, flags, binary.LittleEndian.Uint16(entry[14:16]))
		}
		records[i] = walRecord{
			LSN:   lsn,
			LBA:   lba,
			Flags: flags,
			Data:  append([]byte(nil), payload...),
		}
	}
	return walSegment{Sequence: h.Sequence, Records: records}, nil
}

// scanCommittedSegments scans only the trusted committed window. Physical
// bytes after the window are an uncommitted tail and are intentionally
// ignored; malformed bytes or manifest disagreement inside it fail closed.
// Callers must not publish state accumulated by visit until this returns nil.
func scanCommittedSegments(
	f *os.File,
	window segmentRecoveryWindow,
	blockSize, numBlocks uint32,
	visit func(walSegment) error,
) error {
	if err := window.validate(); err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("parallelwal: stat segment file: %w", err)
	}
	committedEnd := window.StartOffset + window.CommittedBytes
	if committedEnd > st.Size() {
		return fmt.Errorf("%w: committed end=%d file size=%d",
			errBadSegment, committedEnd, st.Size())
	}

	offset := window.StartOffset
	var segmentCount uint64
	var lastLSN uint64
	for offset < committedEnd {
		if segmentCount >= window.SegmentCount {
			return fmt.Errorf("%w: committed bytes contain more than %d segments",
				errBadSegment, window.SegmentCount)
		}
		if committedEnd-offset < segmentHeaderSize {
			return fmt.Errorf("%w: committed tail has %d header bytes",
				errBadSegment, committedEnd-offset)
		}
		headerBytes := make([]byte, segmentHeaderSize)
		if n, err := f.ReadAt(headerBytes, offset); err != nil || n != len(headerBytes) {
			return fmt.Errorf("%w: read header at %d: n=%d err=%v", errBadSegment, offset, n, err)
		}
		h, err := decodeSegmentHeader(headerBytes, blockSize)
		if err != nil {
			return fmt.Errorf("parallelwal: decode segment at %d: %w", offset, err)
		}
		if int64(h.Size) > committedEnd-offset {
			return fmt.Errorf("%w: committed segment at %d size=%d remaining=%d",
				errBadSegment, offset, h.Size, committedEnd-offset)
		}
		segmentBytes := make([]byte, h.Size)
		if n, err := f.ReadAt(segmentBytes, offset); err != nil || n != len(segmentBytes) {
			return fmt.Errorf("%w: read segment at %d: n=%d err=%v", errBadSegment, offset, n, err)
		}
		segment, err := decodeSegment(segmentBytes, blockSize, numBlocks)
		if err != nil {
			return fmt.Errorf("parallelwal: decode segment at %d: %w", offset, err)
		}
		wantSequence := window.FirstSequence + segmentCount
		if segment.Sequence != wantSequence {
			return fmt.Errorf("%w: segment sequence=%d want=%d",
				errBadSegment, segment.Sequence, wantSequence)
		}
		wantFirstLSN := window.FirstLSN
		if segmentCount != 0 {
			if lastLSN == ^uint64(0) {
				return fmt.Errorf("%w: LSN range overflows", errBadSegment)
			}
			wantFirstLSN = lastLSN + 1
		}
		if firstLSN := segment.Records[0].LSN; firstLSN != wantFirstLSN {
			return fmt.Errorf("%w: segment first LSN=%d want=%d",
				errBadSegment, firstLSN, wantFirstLSN)
		}
		lastLSN = segment.Records[len(segment.Records)-1].LSN
		if lastLSN > window.LastLSN {
			return fmt.Errorf("%w: segment last LSN=%d exceeds manifest=%d",
				errBadSegment, lastLSN, window.LastLSN)
		}
		if visit != nil {
			if err := visit(segment); err != nil {
				return fmt.Errorf("parallelwal: visit segment %d: %w", segment.Sequence, err)
			}
		}
		segmentCount++
		offset += int64(h.Size)
	}
	if segmentCount != window.SegmentCount || lastLSN != window.LastLSN {
		return fmt.Errorf("%w: recovered segments/lastLSN=%d/%d want=%d/%d",
			errBadSegment, segmentCount, lastLSN, window.SegmentCount, window.LastLSN)
	}
	return nil
}
