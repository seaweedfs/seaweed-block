package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// WAL entry kinds.
const (
	walEntryWrite   = 0x01
	walEntryTrim    = 0x02
	walEntryBarrier = 0x03
	walEntryPadding = 0xFF

	// walEntryHeaderSize is the TOTAL fixed overhead per record. Layout:
	//   prefix (30 bytes):
	//     LSN(8) + Reserved(8) + Type(1) + Flags(1) + LBA(8) + Length(4)
	//   variable-length data
	//   trailer (8 bytes):
	//     CRC32(4) + EntrySize(4)
	// Total = 30 + len(Data) + 8. The constant captures the 30+8=38 of
	// fixed overhead so callers can compute total_size = header + data.
	//
	// Reserved holds 0 today. It exists to keep the on-disk layout
	// stable if a future implementation needs to attach an
	// implementation-private uint64 (e.g. a fencing token from the
	// engine) without a format version bump.
	walEntryHeaderSize = 38

	// walEntryPrefixSize is the offset where data starts inside a
	// serialized record. Distinct from walEntryHeaderSize because the
	// 8-byte trailer (CRC + EntrySize) lives AFTER the data, not
	// before it. Readers fetching data slice from absOff +
	// walEntryPrefixSize, NOT walEntryHeaderSize.
	walEntryPrefixSize = 30
)

var (
	errCRCMismatch    = errors.New("storage: WAL entry CRC mismatch")
	errInvalidEntry   = errors.New("storage: invalid WAL entry")
	errEntryTruncated = errors.New("storage: WAL entry truncated")
)

// walEntry is one variable-size record in the WAL region of a store
// file. Stored bytes are exactly walEntryHeaderSize + Length when Type
// is Write/Padding, or walEntryHeaderSize when Type is Trim/Barrier.
type walEntry struct {
	LSN      uint64
	Reserved uint64 // see walEntryHeaderSize comment
	Type     uint8
	Flags    uint8
	LBA      uint64 // in blocks
	Length   uint32 // bytes — data length for Write/Padding, trim extent for Trim
	Data     []byte // present only for Write
}

// encode serializes the entry into a fresh byte slice. CRC32 covers
// every byte from offset 0 through Data inclusive. The trailing 8
// bytes are CRC32 + EntrySize so a reader can detect torn writes by
// verifying both fields.
func (e *walEntry) encode() ([]byte, error) {
	switch e.Type {
	case walEntryWrite:
		if len(e.Data) == 0 {
			return nil, fmt.Errorf("%w: write entry with no data", errInvalidEntry)
		}
		if uint32(len(e.Data)) != e.Length {
			return nil, fmt.Errorf("%w: data len %d != Length %d", errInvalidEntry, len(e.Data), e.Length)
		}
	case walEntryTrim:
		if len(e.Data) != 0 {
			return nil, fmt.Errorf("%w: trim entry must have no data payload", errInvalidEntry)
		}
	case walEntryBarrier:
		if e.Length != 0 || len(e.Data) != 0 {
			return nil, fmt.Errorf("%w: barrier entry must have no data", errInvalidEntry)
		}
	}

	totalSize := uint32(walEntryHeaderSize + len(e.Data))
	buf := make([]byte, totalSize)

	le := binary.LittleEndian
	off := 0
	le.PutUint64(buf[off:], e.LSN)
	off += 8
	le.PutUint64(buf[off:], e.Reserved)
	off += 8
	buf[off] = e.Type
	off++
	buf[off] = e.Flags
	off++
	le.PutUint64(buf[off:], e.LBA)
	off += 8
	le.PutUint32(buf[off:], e.Length)
	off += 4
	if len(e.Data) > 0 {
		copy(buf[off:], e.Data)
		off += len(e.Data)
	}
	checksum := crc32.ChecksumIEEE(buf[:off])
	le.PutUint32(buf[off:], checksum)
	off += 4
	le.PutUint32(buf[off:], totalSize)
	return buf, nil
}

// decodeWALEntry parses a record from buf and verifies its CRC and
// declared size. Returns errEntryTruncated if buf is too short to
// hold the declared entry, errCRCMismatch if the stored CRC does
// not match the recomputed one.
func decodeWALEntry(buf []byte) (walEntry, error) {
	if len(buf) < walEntryHeaderSize {
		return walEntry{}, fmt.Errorf("%w: need %d bytes, have %d",
			errEntryTruncated, walEntryHeaderSize, len(buf))
	}
	le := binary.LittleEndian
	var e walEntry
	off := 0
	e.LSN = le.Uint64(buf[off:])
	off += 8
	e.Reserved = le.Uint64(buf[off:])
	off += 8
	e.Type = buf[off]
	off++
	e.Flags = buf[off]
	off++
	e.LBA = le.Uint64(buf[off:])
	off += 8
	e.Length = le.Uint32(buf[off:])
	off += 4

	var dataLen int
	if e.Type == walEntryWrite || e.Type == walEntryPadding {
		dataLen = int(e.Length)
	}
	dataEnd := off + dataLen
	if dataEnd+8 > len(buf) {
		return walEntry{}, fmt.Errorf("%w: need %d, have %d", errEntryTruncated, dataEnd+8, len(buf))
	}
	if dataLen > 0 {
		e.Data = make([]byte, dataLen)
		copy(e.Data, buf[off:dataEnd])
	}
	storedCRC := le.Uint32(buf[dataEnd:])
	storedSize := le.Uint32(buf[dataEnd+4:])

	expectedCRC := crc32.ChecksumIEEE(buf[:dataEnd])
	if storedCRC != expectedCRC {
		return walEntry{}, fmt.Errorf("%w: stored=%08x computed=%08x",
			errCRCMismatch, storedCRC, expectedCRC)
	}
	expectedSize := uint32(walEntryHeaderSize + dataLen)
	if storedSize != expectedSize {
		return walEntry{}, fmt.Errorf("%w: EntrySize=%d, expected=%d",
			errInvalidEntry, storedSize, expectedSize)
	}
	return e, nil
}

// parseLengthFromHeader reads the Length field from a header-only
// buffer. Used by the recovery scanner to compute the full record
// size before reading the rest. Length is at offset
// 8(LSN) + 8(Reserved) + 1(Type) + 1(Flags) + 8(LBA) = 26.
func parseLengthFromHeader(headerBuf []byte) uint32 {
	return binary.LittleEndian.Uint32(headerBuf[26:])
}
