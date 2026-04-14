// Package smartwal is an experimental LogicalStorage implementation
// where the WAL holds 32-byte metadata records only and data lives
// directly in the extent.
//
// EXPERIMENTAL. Not advertised in the sparrow scope statement; not
// the default backend. Reach for it explicitly via
// smartwal.CreateStore / smartwal.OpenStore. The default crash-safe
// backend is core/storage.WALStore.
//
// Why a separate package: this implementation has different on-disk
// layout, different read/write/recover algorithms, and a smaller
// established blast radius from the rest of the codebase. Living in
// a subpackage signals that the contract surface (LogicalStorage)
// admits more than one implementation and that this one is opt-in.
//
// Design summary:
//
//   - Single preallocated file: [superblock | slot ring | extent]
//   - Each WAL slot is exactly 32 bytes (magic + LSN + LBA + dataCRC + recCRC)
//   - Writes go directly to the extent; only the metadata is logged
//   - Reads always come from the extent — no in-memory dirty map
//     required for the read path
//   - One fsync per Sync covers BOTH the extent pwrite and the WAL
//     append because they live in the same file
//   - Recovery scans the ring, picks the highest-LSN record per LBA,
//     and verifies the extent block's CRC against the record. CRC
//     mismatch (torn pre-Sync write) skips that record so the LBA's
//     prior bytes remain authoritative.
//   - Failure atomicity for WAL-append errors: pre-write extent bytes
//     are saved and rolled back if the metadata append fails, so
//     untracked data never becomes durable through a later Sync.
package smartwal

import (
	"encoding/binary"
	"hash/crc32"
)

// recordSize is the on-disk size of one WAL slot.
const recordSize = 32

// Record flag bits.
const (
	flagWrite   uint8 = 0x01
	flagTrim    uint8 = 0x02
	flagBarrier uint8 = 0x04
)

// recordMagic distinguishes a populated slot from a zero-filled one.
// On a fresh WAL the file is zero, so magic byte 0 means "empty slot"
// for the recovery scanner.
const recordMagic uint8 = 0xAC

// record is the metadata-only WAL entry. Encoded layout (32 bytes):
//
//	[0:1]   magic (0xAC)
//	[1:2]   flags
//	[2:4]   reserved (zeroed, available for a future implementation
//	         field without bumping a format version)
//	[4:8]   LBA          (uint32, little-endian)
//	[8:16]  LSN          (uint64, little-endian)
//	[16:24] reserved2    (zeroed; was Epoch in the V2 prototype —
//	         dropped because the engine owns fencing in V3)
//	[24:28] dataCRC32    (CRC32-IEEE of the 4KB extent block)
//	[28:32] recCRC32     (CRC32-IEEE of bytes 0..27)
type record struct {
	LSN       uint64
	LBA       uint32
	Flags     uint8
	DataCRC32 uint32
}

// encode serializes a record into exactly recordSize bytes.
func encode(r record) [recordSize]byte {
	var buf [recordSize]byte
	buf[0] = recordMagic
	buf[1] = r.Flags
	// buf[2:4] reserved (zeros)
	binary.LittleEndian.PutUint32(buf[4:8], r.LBA)
	binary.LittleEndian.PutUint64(buf[8:16], r.LSN)
	// buf[16:24] reserved2 (zeros — was Epoch, dropped for V3)
	binary.LittleEndian.PutUint32(buf[24:28], r.DataCRC32)
	rcrc := crc32.ChecksumIEEE(buf[:28])
	binary.LittleEndian.PutUint32(buf[28:32], rcrc)
	return buf
}

// decode parses one slot. Returns ok=false for an empty slot
// (magic != 0xAC) or a torn slot (record CRC mismatch).
func decode(buf []byte) (record, bool) {
	if len(buf) < recordSize {
		return record{}, false
	}
	if buf[0] != recordMagic {
		return record{}, false
	}
	stored := binary.LittleEndian.Uint32(buf[28:32])
	expected := crc32.ChecksumIEEE(buf[:28])
	if stored != expected {
		return record{}, false
	}
	return record{
		Flags:     buf[1],
		LBA:       binary.LittleEndian.Uint32(buf[4:8]),
		LSN:       binary.LittleEndian.Uint64(buf[8:16]),
		DataCRC32: binary.LittleEndian.Uint32(buf[24:28]),
	}, true
}
