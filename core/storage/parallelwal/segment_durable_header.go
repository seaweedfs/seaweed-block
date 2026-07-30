package parallelwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	segmentDurableHeaderSize      = 4096
	segmentDurableHeaderSlots     = 2
	segmentDurableHeaderMagic     = "SWSH"
	segmentDurableHeaderVersion   = 1
	segmentDurableHeaderCRCOffset = segmentDurableHeaderSize - 4
	segmentDurableLogOffset       = segmentDurableHeaderSlots * segmentDurableHeaderSize
)

var errBadSegmentDurableHeader = errors.New("parallelwal: invalid segmented durable header")

type segmentDurableHeader struct {
	Generation     uint64
	BlockSize      uint32
	NumBlocks      uint32
	LogOffset      int64
	MaxLogBytes    int64
	CommittedBytes int64
	SegmentCount   uint64
	FirstSequence  uint64
	FirstLSN       uint64
	LastLSN        uint64
}

func (h segmentDurableHeader) recoveryWindow() segmentRecoveryWindow {
	return segmentRecoveryWindow{
		StartOffset:    h.LogOffset,
		CommittedBytes: h.CommittedBytes,
		SegmentCount:   h.SegmentCount,
		FirstSequence:  h.FirstSequence,
		FirstLSN:       h.FirstLSN,
		LastLSN:        h.LastLSN,
	}
}

func (h segmentDurableHeader) validate() error {
	if h.Generation == 0 || h.BlockSize == 0 || h.BlockSize > maxSegmentPayloadBytes ||
		h.NumBlocks == 0 || h.LogOffset < segmentDurableLogOffset ||
		h.LogOffset%segmentDurableHeaderSize != 0 || h.MaxLogBytes <= 0 ||
		h.LogOffset > int64(^uint64(0)>>1)-h.MaxLogBytes ||
		h.CommittedBytes < 0 || h.CommittedBytes > h.MaxLogBytes {
		return fmt.Errorf("%w: geometry %+v", errBadSegmentDurableHeader, h)
	}
	minimumSegmentBytes, err := segmentEncodedSize(1, h.BlockSize)
	if err != nil || h.MaxLogBytes < int64(minimumSegmentBytes) {
		return fmt.Errorf("%w: maxLogBytes=%d minimum=%d",
			errBadSegmentDurableHeader, h.MaxLogBytes, minimumSegmentBytes)
	}
	if err := h.recoveryWindow().validate(); err != nil {
		return fmt.Errorf("%w: %v", errBadSegmentDurableHeader, err)
	}
	return nil
}

func encodeSegmentDurableHeader(h segmentDurableHeader) ([segmentDurableHeaderSize]byte, error) {
	var buf [segmentDurableHeaderSize]byte
	if err := h.validate(); err != nil {
		return buf, err
	}
	copy(buf[0:4], segmentDurableHeaderMagic)
	binary.LittleEndian.PutUint16(buf[4:6], segmentDurableHeaderVersion)
	binary.LittleEndian.PutUint16(buf[6:8], segmentDurableHeaderSize)
	binary.LittleEndian.PutUint64(buf[8:16], h.Generation)
	binary.LittleEndian.PutUint32(buf[16:20], h.BlockSize)
	binary.LittleEndian.PutUint32(buf[20:24], h.NumBlocks)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.LogOffset))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(h.MaxLogBytes))
	binary.LittleEndian.PutUint64(buf[40:48], uint64(h.CommittedBytes))
	binary.LittleEndian.PutUint64(buf[48:56], h.SegmentCount)
	binary.LittleEndian.PutUint64(buf[56:64], h.FirstSequence)
	binary.LittleEndian.PutUint64(buf[64:72], h.FirstLSN)
	binary.LittleEndian.PutUint64(buf[72:80], h.LastLSN)
	binary.LittleEndian.PutUint32(buf[segmentDurableHeaderCRCOffset:],
		crc32.ChecksumIEEE(buf[:segmentDurableHeaderCRCOffset]))
	return buf, nil
}

func decodeSegmentDurableHeader(buf []byte) (segmentDurableHeader, error) {
	if len(buf) != segmentDurableHeaderSize {
		return segmentDurableHeader{}, fmt.Errorf("%w: size=%d", errBadSegmentDurableHeader, len(buf))
	}
	if string(buf[0:4]) != segmentDurableHeaderMagic {
		return segmentDurableHeader{}, fmt.Errorf("%w: bad magic", errBadSegmentDurableHeader)
	}
	if got := binary.LittleEndian.Uint16(buf[4:6]); got != segmentDurableHeaderVersion {
		return segmentDurableHeader{}, fmt.Errorf("%w: version=%d", errBadSegmentDurableHeader, got)
	}
	if got := binary.LittleEndian.Uint16(buf[6:8]); got != segmentDurableHeaderSize {
		return segmentDurableHeader{}, fmt.Errorf("%w: header size=%d", errBadSegmentDurableHeader, got)
	}
	if got, want := binary.LittleEndian.Uint32(buf[segmentDurableHeaderCRCOffset:]),
		crc32.ChecksumIEEE(buf[:segmentDurableHeaderCRCOffset]); got != want {
		return segmentDurableHeader{}, fmt.Errorf("%w: CRC got=%08x want=%08x",
			errBadSegmentDurableHeader, got, want)
	}
	for _, value := range buf[80:segmentDurableHeaderCRCOffset] {
		if value != 0 {
			return segmentDurableHeader{}, fmt.Errorf("%w: non-zero reserved bytes",
				errBadSegmentDurableHeader)
		}
	}
	h := segmentDurableHeader{
		Generation:     binary.LittleEndian.Uint64(buf[8:16]),
		BlockSize:      binary.LittleEndian.Uint32(buf[16:20]),
		NumBlocks:      binary.LittleEndian.Uint32(buf[20:24]),
		LogOffset:      int64(binary.LittleEndian.Uint64(buf[24:32])),
		MaxLogBytes:    int64(binary.LittleEndian.Uint64(buf[32:40])),
		CommittedBytes: int64(binary.LittleEndian.Uint64(buf[40:48])),
		SegmentCount:   binary.LittleEndian.Uint64(buf[48:56]),
		FirstSequence:  binary.LittleEndian.Uint64(buf[56:64]),
		FirstLSN:       binary.LittleEndian.Uint64(buf[64:72]),
		LastLSN:        binary.LittleEndian.Uint64(buf[72:80]),
	}
	if err := h.validate(); err != nil {
		return segmentDurableHeader{}, err
	}
	return h, nil
}

func writeSegmentDurableHeaderAt(file io.WriterAt, slot int, h segmentDurableHeader) error {
	if slot < 0 || slot >= segmentDurableHeaderSlots {
		return fmt.Errorf("%w: slot=%d", errBadSegmentDurableHeader, slot)
	}
	buf, err := encodeSegmentDurableHeader(h)
	if err != nil {
		return err
	}
	n, err := file.WriteAt(buf[:], int64(slot*segmentDurableHeaderSize))
	if err != nil {
		return fmt.Errorf("parallelwal: write segmented header slot %d: %w", slot, err)
	}
	if n != len(buf) {
		return fmt.Errorf("parallelwal: write segmented header slot %d: %w", slot, io.ErrShortWrite)
	}
	return nil
}

func readBestSegmentDurableHeader(file io.ReaderAt) (segmentDurableHeader, int, error) {
	var best segmentDurableHeader
	bestSlot := -1
	var slotErrors [segmentDurableHeaderSlots]error
	for slot := 0; slot < segmentDurableHeaderSlots; slot++ {
		buf := make([]byte, segmentDurableHeaderSize)
		n, err := file.ReadAt(buf, int64(slot*segmentDurableHeaderSize))
		if err != nil || n != len(buf) {
			slotErrors[slot] = fmt.Errorf("read n=%d err=%v", n, err)
			continue
		}
		header, err := decodeSegmentDurableHeader(buf)
		if err != nil {
			slotErrors[slot] = err
			continue
		}
		if bestSlot == -1 || header.Generation > best.Generation {
			best = header
			bestSlot = slot
		}
	}
	if bestSlot == -1 {
		return segmentDurableHeader{}, -1, fmt.Errorf(
			"parallelwal: no valid segmented durable header: slot0=%v slot1=%v",
			slotErrors[0], slotErrors[1])
	}
	return best, bestSlot, nil
}
