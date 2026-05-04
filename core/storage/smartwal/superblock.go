package smartwal

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// File-format identity. Distinct from the WALStore magic so the two
// backends can never accidentally open each other's files.
//
// Version history:
//   1: initial layout.
//   2: T3a Addendum A — added ImplKind + ImplVersion so the
//      DurableProvider can self-identify this as a smartwal file
//      via a uniform field shape parallel to walstore's superblock.
//      Opening a v1 file with v2 code returns errBadVersion
//      (one-way bump; no compat read path).
const (
	headerSize    = 4096
	headerMagic   = "SWAW" // SmartWAL: "SW" + AW
	headerVersion = 2
)

// implKindSmartWAL matches core/storage.ImplKindSmartWAL (=2).
// Duplicated as a raw byte constant here to avoid a reverse
// import; the shared wire value is documented in storage.ImplKind.
const implKindSmartWAL uint8 = 2

// smartwalImplVersion is this package's per-impl schema version.
// Independent from headerVersion (superblock-format version).
const smartwalImplVersion uint32 = 1

var (
	errBadMagic    = errors.New("smartwal: not a smartwal store (bad magic)")
	errBadVersion  = errors.New("smartwal: unsupported version")
	errBadGeometry = errors.New("smartwal: invalid geometry")
	errBadImpl     = errors.New("smartwal: wrong impl kind on disk")
)

// header is the 4KB preamble of a smartwal store file. It records
// geometry only — no recovery state, no checkpoint LSN. Recovery
// works by rescanning the ring on every Open, so the only on-disk
// invariant the header must preserve is the file layout.
type header struct {
	Magic     [4]byte
	Version   uint16
	Flags     uint16
	UUID      [16]byte
	BlockSize uint32 // bytes per addressable block
	NumBlocks uint32 // total addressable blocks
	WALSlots  uint64 // number of 32-byte slots in the ring
	CreatedAt uint64 // unix nanoseconds
	// T3a Addendum A — impl self-identity (parallel to walstore's
	// superblock ImplKind + ImplVersion fields). Always set to
	// implKindSmartWAL + smartwalImplVersion on new files.
	ImplKind    uint8
	ImplVersion uint32
}

func newHeader(blockSize uint32, numBlocks uint32, walSlots uint64) (header, error) {
	h := header{
		Version:     headerVersion,
		BlockSize:   blockSize,
		NumBlocks:   numBlocks,
		WALSlots:    walSlots,
		ImplKind:    implKindSmartWAL,
		ImplVersion: smartwalImplVersion,
	}
	copy(h.Magic[:], headerMagic)
	if _, err := rand.Read(h.UUID[:]); err != nil {
		return header{}, fmt.Errorf("smartwal: header UUID: %w", err)
	}
	return h, nil
}

func (h *header) writeTo(w io.Writer) error {
	buf := make([]byte, headerSize)
	off := 0
	off += copy(buf[off:], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[off:], h.Version)
	off += 2
	binary.LittleEndian.PutUint16(buf[off:], h.Flags)
	off += 2
	off += copy(buf[off:], h.UUID[:])
	binary.LittleEndian.PutUint32(buf[off:], h.BlockSize)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], h.NumBlocks)
	off += 4
	binary.LittleEndian.PutUint64(buf[off:], h.WALSlots)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], h.CreatedAt)
	off += 8
	// T3a Addendum A — ImplKind + ImplVersion.
	buf[off] = h.ImplKind
	off++
	off += 3 // reserved for future kind expansion / alignment
	binary.LittleEndian.PutUint32(buf[off:], h.ImplVersion)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("smartwal: write header: %w", err)
	}
	return nil
}

func readHeader(r io.Reader) (header, error) {
	buf := make([]byte, headerSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return header{}, fmt.Errorf("smartwal: read header: %w", err)
	}
	var h header
	off := 0
	copy(h.Magic[:], buf[off:off+4])
	off += 4
	if string(h.Magic[:]) != headerMagic {
		return header{}, errBadMagic
	}
	h.Version = binary.LittleEndian.Uint16(buf[off:])
	off += 2
	if h.Version != headerVersion {
		return header{}, fmt.Errorf("%w: got %d, want %d", errBadVersion, h.Version, headerVersion)
	}
	h.Flags = binary.LittleEndian.Uint16(buf[off:])
	off += 2
	copy(h.UUID[:], buf[off:off+16])
	off += 16
	h.BlockSize = binary.LittleEndian.Uint32(buf[off:])
	off += 4
	h.NumBlocks = binary.LittleEndian.Uint32(buf[off:])
	off += 4
	h.WALSlots = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	h.CreatedAt = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	// T3a Addendum A — ImplKind + ImplVersion.
	h.ImplKind = buf[off]
	off++
	off += 3 // reserved
	h.ImplVersion = binary.LittleEndian.Uint32(buf[off:])
	return h, nil
}

func (h *header) validate() error {
	if string(h.Magic[:]) != headerMagic {
		return errBadMagic
	}
	if h.BlockSize == 0 {
		return fmt.Errorf("%w: BlockSize=0", errBadGeometry)
	}
	if h.NumBlocks == 0 {
		return fmt.Errorf("%w: NumBlocks=0", errBadGeometry)
	}
	if h.WALSlots == 0 {
		return fmt.Errorf("%w: WALSlots=0", errBadGeometry)
	}
	// T3a Addendum A — ImplKind must match smartwal.
	if h.ImplKind != implKindSmartWAL {
		return fmt.Errorf("%w: got %d, want %d", errBadImpl, h.ImplKind, implKindSmartWAL)
	}
	return nil
}
