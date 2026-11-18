// Package v2legacy is the frozen v2/impl1 reader used only by the offline
// migration tool (`sw-block migrate-v3`, format bump v3, DESIGN r17.4
// section 10.1; plan row 13). It is a copy of the v2 superblock layout and
// the v2 WAL entry framing as they stand before the format bump, kept here
// so that the v3 open code holds no legacy reader and row 14 can delete the
// legacy production code. Nothing here writes, recovers or replays: the
// scan only decides whether the tool must refuse.
package v2legacy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// SuperblockSize is the v2 superblock region at offset 0; the v2 WAL starts
// right after it.
const SuperblockSize = 4096

const (
	magic        = "SWBK"
	version      = 2
	implWALStore = 1
	implVersion1 = 1
)

var (
	// ErrNotV2 is any offset-0 content that is not a v2 superblock.
	ErrNotV2 = errors.New("v2legacy: offset 0 is not a v2 superblock")
	// ErrImplUnsupported is a v2 superblock of another impl or impl version
	// (impl 2, C12, is refused by the tool, DESIGN 10).
	ErrImplUnsupported = errors.New("v2legacy: not walstore impl version 1")
)

// Superblock is the frozen v2 superblock (core/storage/superblock.go at
// format version 2).
type Superblock struct {
	Version          uint16
	Flags            uint16
	UUID             [16]byte
	VolumeSize       uint64
	ExtentSize       uint32
	BlockSize        uint32
	WALOffset        uint64
	WALSize          uint64
	WALHead          uint64
	WALTail          uint64
	WALCheckpointLSN uint64
	CreatedAt        uint64
	ImplKind         uint8
	ImplVersion      uint32
}

// ReadSuperblock decodes and validates a v2/impl1 superblock (precondition
// 2: magic, version 2, ImplKind walstore, ImplVersion 1).
func ReadSuperblock(buf []byte) (Superblock, error) {
	if len(buf) < SuperblockSize {
		return Superblock{}, fmt.Errorf("%w: short header (%d bytes)", ErrNotV2, len(buf))
	}
	le := binary.LittleEndian
	if string(buf[0:4]) != magic {
		return Superblock{}, fmt.Errorf("%w: bad magic", ErrNotV2)
	}
	var sb Superblock
	sb.Version = le.Uint16(buf[4:])
	if sb.Version != version {
		return Superblock{}, fmt.Errorf("%w: version %d", ErrNotV2, sb.Version)
	}
	sb.Flags = le.Uint16(buf[6:])
	copy(sb.UUID[:], buf[8:24])
	sb.VolumeSize = le.Uint64(buf[24:])
	sb.ExtentSize = le.Uint32(buf[32:])
	sb.BlockSize = le.Uint32(buf[36:])
	sb.WALOffset = le.Uint64(buf[40:])
	sb.WALSize = le.Uint64(buf[48:])
	sb.WALHead = le.Uint64(buf[56:])
	sb.WALTail = le.Uint64(buf[64:])
	sb.WALCheckpointLSN = le.Uint64(buf[72:])
	sb.CreatedAt = le.Uint64(buf[80:])
	sb.ImplKind = buf[88]
	sb.ImplVersion = le.Uint32(buf[92:])
	switch {
	case sb.VolumeSize == 0, sb.BlockSize == 0, sb.ExtentSize == 0, sb.WALSize == 0:
		return Superblock{}, fmt.Errorf("%w: zero geometry field", ErrNotV2)
	case sb.WALOffset != SuperblockSize:
		return Superblock{}, fmt.Errorf("%w: WALOffset %d", ErrNotV2, sb.WALOffset)
	case sb.VolumeSize%uint64(sb.BlockSize) != 0:
		return Superblock{}, fmt.Errorf("%w: VolumeSize not block aligned", ErrNotV2)
	}
	if sb.ImplKind != implWALStore || sb.ImplVersion != implVersion1 {
		return Superblock{}, fmt.Errorf("%w: ImplKind %d ImplVersion %d", ErrImplUnsupported, sb.ImplKind, sb.ImplVersion)
	}
	return sb, nil
}

// The frozen v2 WAL entry framing (core/storage/wal_entry.go): a 30-byte
// prefix LSN(8) Reserved(8) Type(1) Flags(1) LBA(8) Length(4), then the
// payload (write, batch and padding only), then CRC32-IEEE(4) over prefix
// and payload, and EntrySize(4).
const (
	entryPrefix = 30
	entryHeader = 38

	typeWrite   = 0x01
	typeTrim    = 0x02
	typeBarrier = 0x03
	typeBatch   = 0x04
	typePadding = 0xFF
)

// Finding is one offset of the WAL region that refuses the migration.
type Finding struct {
	Offset  uint64 // relative to the WAL region start
	Type    uint8
	LSN     uint64 // the record's last LSN (a batch counts by its last)
	Reason  string
	Covered bool // LSN <= checkpoint: not a refusal (reported for tests only)
}

// ScanClean is precondition 3 of DESIGN 10.1: an exhaustive,
// refusal-biased scan of the whole v2 WAL region. It tries the frozen
// decoder at every byte offset, reading circularly so a record crossing the
// region end is tried too. An offset refuses when it decodes as a known
// type whose type-derived footprint (38 + Length for write, batch and
// padding; 38 for trim and barrier) fits in the region, whose CRC and
// EntrySize are valid, and whose last LSN is above checkpoint; or when a
// CRC-valid candidate of a known type cannot be classified (a zero or
// overflowing batch count). A CRC-valid record at or below the checkpoint
// is already in the extent and is ignored. It never runs recovery or
// replay. It returns the refusing findings, in offset order.
func ScanClean(wal []byte, checkpoint uint64) []Finding {
	n := uint64(len(wal))
	var out []Finding
	at := func(i uint64) byte { return wal[i%n] }
	read := func(off, length uint64) []byte {
		b := make([]byte, length)
		for i := range b {
			b[i] = at(off + uint64(i))
		}
		return b
	}
	for off := uint64(0); off < n && n >= entryHeader; off++ {
		typ := at(off + 16)
		var payload bool
		switch typ {
		case typeWrite, typeBatch, typePadding:
			payload = true
		case typeTrim, typeBarrier:
		default:
			continue
		}
		prefix := read(off, entryPrefix)
		length := uint64(binary.LittleEndian.Uint32(prefix[26:]))
		footprint := uint64(entryHeader)
		if payload {
			footprint += length
		}
		if footprint > n {
			continue // cannot be a record of this region
		}
		body := read(off, footprint)
		dataEnd := footprint - 8
		if binary.LittleEndian.Uint32(body[dataEnd:]) != crc32.ChecksumIEEE(body[:dataEnd]) ||
			uint64(binary.LittleEndian.Uint32(body[dataEnd+4:])) != footprint {
			continue
		}
		lsn := binary.LittleEndian.Uint64(body[0:])
		last := lsn
		f := Finding{Offset: off, Type: typ}
		if typ == typeBatch {
			count := binary.LittleEndian.Uint64(body[8:])
			if count == 0 || lsn+count-1 < lsn {
				f.LSN, f.Reason = lsn, "CRC-valid batch with an unclassifiable block count"
				out = append(out, f)
				continue
			}
			last = lsn + count - 1
		}
		f.LSN = last
		if last <= checkpoint {
			continue // already in the extent
		}
		f.Reason = fmt.Sprintf("CRC-valid v2 record type %#x with LSN %d above the checkpoint %d", typ, last, checkpoint)
		out = append(out, f)
	}
	return out
}
