package parallelwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	headerSize       = 4096
	headerSlots      = 2
	headerMagic      = "SWPW"
	headerVersion    = 1
	maxLaneCount     = 16
	laneHeadsOffset  = 128
	laneTailsOffset  = laneHeadsOffset + maxLaneCount*8
	headerCRCOffset  = headerSize - 4
	defaultLaneCount = 4
	defaultLaneSlots = 4096
)

var (
	errBadMagic    = errors.New("parallelwal: bad magic")
	errBadVersion  = errors.New("parallelwal: unsupported version")
	errBadHeader   = errors.New("parallelwal: invalid header")
	errBadGeometry = errors.New("parallelwal: invalid geometry")
)

type fileHeader struct {
	Generation    uint64
	CreatedAt     uint64
	BlockSize     uint32
	NumBlocks     uint32
	LaneCount     uint16
	StripeBlocks  uint16
	RecordSize    uint32
	SlotsPerLane  uint64
	DurableLSN    uint64
	CheckpointLSN uint64
	WALTail       uint64
	RetainPerLane uint64
	ActiveExtent  uint8
	LaneHeads     [maxLaneCount]uint64
	LaneTails     [maxLaneCount]uint64
}

func (h fileHeader) validate() error {
	if h.BlockSize == 0 || h.NumBlocks == 0 {
		return fmt.Errorf("%w: blockSize=%d numBlocks=%d", errBadGeometry, h.BlockSize, h.NumBlocks)
	}
	if h.BlockSize > maxCheckpointWriteBytes {
		return fmt.Errorf("%w: blockSize=%d exceeds maximum=%d",
			errBadGeometry, h.BlockSize, maxCheckpointWriteBytes)
	}
	if h.LaneCount == 0 || h.LaneCount > maxLaneCount {
		return fmt.Errorf("%w: laneCount=%d", errBadGeometry, h.LaneCount)
	}
	if h.StripeBlocks == 0 || h.SlotsPerLane == 0 {
		return fmt.Errorf("%w: stripeBlocks=%d slotsPerLane=%d", errBadGeometry, h.StripeBlocks, h.SlotsPerLane)
	}
	wantRecordSize := uint64(recordHeaderSize) + uint64(h.BlockSize)
	if wantRecordSize > uint64(^uint32(0)) || uint64(h.RecordSize) != wantRecordSize {
		return fmt.Errorf("%w: recordSize=%d want=%d", errBadGeometry, h.RecordSize, wantRecordSize)
	}
	if h.CheckpointLSN > h.DurableLSN {
		return fmt.Errorf("%w: checkpoint=%d durable=%d", errBadHeader, h.CheckpointLSN, h.DurableLSN)
	}
	for i := 0; i < int(h.LaneCount); i++ {
		if h.LaneTails[i] > h.LaneHeads[i] || h.LaneHeads[i]-h.LaneTails[i] > h.SlotsPerLane {
			return fmt.Errorf("%w: lane=%d tail=%d head=%d slots=%d",
				errBadHeader, i, h.LaneTails[i], h.LaneHeads[i], h.SlotsPerLane)
		}
	}
	if h.RetainPerLane == 0 || h.RetainPerLane >= h.SlotsPerLane {
		return fmt.Errorf("%w: retainPerLane=%d slotsPerLane=%d", errBadGeometry, h.RetainPerLane, h.SlotsPerLane)
	}
	if h.ActiveExtent > 1 {
		return fmt.Errorf("%w: activeExtent=%d", errBadGeometry, h.ActiveExtent)
	}
	return nil
}

func encodeHeader(h fileHeader) ([headerSize]byte, error) {
	var buf [headerSize]byte
	copy(buf[0:4], headerMagic)
	binary.LittleEndian.PutUint16(buf[4:6], headerVersion)
	binary.LittleEndian.PutUint16(buf[6:8], headerSize)
	binary.LittleEndian.PutUint64(buf[8:16], h.Generation)
	binary.LittleEndian.PutUint64(buf[16:24], h.CreatedAt)
	binary.LittleEndian.PutUint32(buf[24:28], h.BlockSize)
	binary.LittleEndian.PutUint32(buf[28:32], h.NumBlocks)
	binary.LittleEndian.PutUint16(buf[32:34], h.LaneCount)
	binary.LittleEndian.PutUint16(buf[34:36], h.StripeBlocks)
	binary.LittleEndian.PutUint32(buf[36:40], h.RecordSize)
	binary.LittleEndian.PutUint64(buf[40:48], h.SlotsPerLane)
	binary.LittleEndian.PutUint64(buf[48:56], h.DurableLSN)
	binary.LittleEndian.PutUint64(buf[56:64], h.CheckpointLSN)
	binary.LittleEndian.PutUint64(buf[64:72], h.WALTail)
	binary.LittleEndian.PutUint64(buf[72:80], h.RetainPerLane)
	buf[80] = h.ActiveExtent
	for i := 0; i < maxLaneCount; i++ {
		binary.LittleEndian.PutUint64(buf[laneHeadsOffset+i*8:], h.LaneHeads[i])
		binary.LittleEndian.PutUint64(buf[laneTailsOffset+i*8:], h.LaneTails[i])
	}
	if err := h.validate(); err != nil {
		return buf, err
	}
	binary.LittleEndian.PutUint32(buf[headerCRCOffset:], crc32.ChecksumIEEE(buf[:headerCRCOffset]))
	return buf, nil
}

func decodeHeader(buf []byte) (fileHeader, error) {
	if len(buf) != headerSize {
		return fileHeader{}, fmt.Errorf("%w: size=%d", errBadHeader, len(buf))
	}
	if string(buf[0:4]) != headerMagic {
		return fileHeader{}, errBadMagic
	}
	if got := binary.LittleEndian.Uint16(buf[4:6]); got != headerVersion {
		return fileHeader{}, fmt.Errorf("%w: got=%d want=%d", errBadVersion, got, headerVersion)
	}
	if got := binary.LittleEndian.Uint16(buf[6:8]); got != headerSize {
		return fileHeader{}, fmt.Errorf("%w: headerSize=%d", errBadHeader, got)
	}
	gotCRC := binary.LittleEndian.Uint32(buf[headerCRCOffset:])
	wantCRC := crc32.ChecksumIEEE(buf[:headerCRCOffset])
	if gotCRC != wantCRC {
		return fileHeader{}, fmt.Errorf("%w: CRC got=%08x want=%08x", errBadHeader, gotCRC, wantCRC)
	}
	h := fileHeader{
		Generation:    binary.LittleEndian.Uint64(buf[8:16]),
		CreatedAt:     binary.LittleEndian.Uint64(buf[16:24]),
		BlockSize:     binary.LittleEndian.Uint32(buf[24:28]),
		NumBlocks:     binary.LittleEndian.Uint32(buf[28:32]),
		LaneCount:     binary.LittleEndian.Uint16(buf[32:34]),
		StripeBlocks:  binary.LittleEndian.Uint16(buf[34:36]),
		RecordSize:    binary.LittleEndian.Uint32(buf[36:40]),
		SlotsPerLane:  binary.LittleEndian.Uint64(buf[40:48]),
		DurableLSN:    binary.LittleEndian.Uint64(buf[48:56]),
		CheckpointLSN: binary.LittleEndian.Uint64(buf[56:64]),
		WALTail:       binary.LittleEndian.Uint64(buf[64:72]),
		RetainPerLane: binary.LittleEndian.Uint64(buf[72:80]),
		ActiveExtent:  buf[80],
	}
	for i := 0; i < maxLaneCount; i++ {
		h.LaneHeads[i] = binary.LittleEndian.Uint64(buf[laneHeadsOffset+i*8:])
		h.LaneTails[i] = binary.LittleEndian.Uint64(buf[laneTailsOffset+i*8:])
	}
	if err := h.validate(); err != nil {
		return fileHeader{}, err
	}
	return h, nil
}

func readBestHeader(f *os.File) (fileHeader, int, error) {
	var best fileHeader
	bestSlot := -1
	var errs [headerSlots]error
	for slot := 0; slot < headerSlots; slot++ {
		buf := make([]byte, headerSize)
		if _, err := f.ReadAt(buf, int64(slot*headerSize)); err != nil && !errors.Is(err, io.EOF) {
			errs[slot] = err
			continue
		}
		h, err := decodeHeader(buf)
		if err != nil {
			errs[slot] = err
			continue
		}
		if bestSlot == -1 || h.Generation > best.Generation {
			best = h
			bestSlot = slot
		}
	}
	if bestSlot == -1 {
		return fileHeader{}, -1, fmt.Errorf("parallelwal: no valid header: slot0=%v slot1=%v", errs[0], errs[1])
	}
	return best, bestSlot, nil
}

// ProbeStore validates that path has at least one usable parallelwal header.
// It intentionally applies the same dual-header selection as OpenStore.
func ProbeStore(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("parallelwal: probe open %s: %w", path, err)
	}
	defer f.Close()
	if _, _, err := readBestHeader(f); err != nil {
		return err
	}
	return nil
}

func writeHeaderAt(f *os.File, slot int, h fileHeader) error {
	if slot < 0 || slot >= headerSlots {
		return fmt.Errorf("parallelwal: header slot %d out of range", slot)
	}
	buf, err := encodeHeader(h)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(buf[:], int64(slot*headerSize)); err != nil {
		return fmt.Errorf("parallelwal: write header slot %d: %w", slot, err)
	}
	return nil
}
