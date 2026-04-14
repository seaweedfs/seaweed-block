package storage

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// File-format identity. Bumped only on incompatible layout changes.
const (
	superblockSize    = 4096
	superblockMagic   = "SWBK"
	superblockVersion = 1
)

var (
	errSuperblockNotOurs   = errors.New("storage: not a store file (bad magic)")
	errSuperblockBadVer    = errors.New("storage: unsupported store version")
	errSuperblockBadVolume = errors.New("storage: invalid volume size")
	errSuperblockInvalid   = errors.New("storage: invalid superblock")
)

// superblock is the 4KB header at offset 0 of a store file. It
// identifies the format, records geometry, and tracks WAL state.
//
// Fields are deliberately narrow: nothing here records anything about
// recovery class, mode, replica role, or any other engine-owned
// concern. Storage describes its own bytes; the engine decides what
// they mean.
type superblock struct {
	Magic            [4]byte
	Version          uint16
	Flags            uint16
	UUID             [16]byte
	VolumeSize       uint64 // logical size in bytes
	ExtentSize       uint32 // bytes per extent slot
	BlockSize        uint32 // bytes per block (caller's IO unit)
	WALOffset        uint64 // absolute file offset where WAL region starts
	WALSize          uint64 // WAL region size in bytes
	WALHead          uint64 // monotonic WAL write position
	WALTail          uint64 // monotonic WAL flush position
	WALCheckpointLSN uint64 // last LSN durably present in extent
	CreatedAt        uint64 // unix nanoseconds
}

// newSuperblock constructs a superblock with defaults and a fresh
// UUID. Caller-supplied geometry overrides defaults.
func newSuperblock(volumeSize uint64, opts createOptions) (superblock, error) {
	if volumeSize == 0 {
		return superblock{}, errSuperblockBadVolume
	}
	extentSize := opts.ExtentSize
	if extentSize == 0 {
		extentSize = 64 * 1024
	}
	blockSize := opts.BlockSize
	if blockSize == 0 {
		blockSize = 4096
	}
	walSize := opts.WALSize
	if walSize == 0 {
		walSize = 64 * 1024 * 1024
	}
	sb := superblock{
		Version:    superblockVersion,
		VolumeSize: volumeSize,
		ExtentSize: extentSize,
		BlockSize:  blockSize,
		WALOffset:  superblockSize,
		WALSize:    walSize,
	}
	copy(sb.Magic[:], superblockMagic)
	if _, err := rand.Read(sb.UUID[:]); err != nil {
		return superblock{}, fmt.Errorf("storage: superblock UUID: %w", err)
	}
	return sb, nil
}

// writeTo serializes the superblock into a 4KB block at w.
func (sb *superblock) writeTo(w io.Writer) (int64, error) {
	buf := make([]byte, superblockSize)
	le := binary.LittleEndian
	off := 0
	off += copy(buf[off:], sb.Magic[:])
	le.PutUint16(buf[off:], sb.Version)
	off += 2
	le.PutUint16(buf[off:], sb.Flags)
	off += 2
	off += copy(buf[off:], sb.UUID[:])
	le.PutUint64(buf[off:], sb.VolumeSize)
	off += 8
	le.PutUint32(buf[off:], sb.ExtentSize)
	off += 4
	le.PutUint32(buf[off:], sb.BlockSize)
	off += 4
	le.PutUint64(buf[off:], sb.WALOffset)
	off += 8
	le.PutUint64(buf[off:], sb.WALSize)
	off += 8
	le.PutUint64(buf[off:], sb.WALHead)
	off += 8
	le.PutUint64(buf[off:], sb.WALTail)
	off += 8
	le.PutUint64(buf[off:], sb.WALCheckpointLSN)
	off += 8
	le.PutUint64(buf[off:], sb.CreatedAt)

	n, err := w.Write(buf)
	return int64(n), err
}

// readSuperblock reads and validates the 4KB header at the start of r.
func readSuperblock(r io.Reader) (superblock, error) {
	buf := make([]byte, superblockSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return superblock{}, fmt.Errorf("storage: read superblock: %w", err)
	}
	le := binary.LittleEndian
	var sb superblock
	off := 0
	copy(sb.Magic[:], buf[off:off+4])
	off += 4
	if string(sb.Magic[:]) != superblockMagic {
		return superblock{}, errSuperblockNotOurs
	}
	sb.Version = le.Uint16(buf[off:])
	off += 2
	if sb.Version != superblockVersion {
		return superblock{}, fmt.Errorf("%w: got %d, want %d", errSuperblockBadVer, sb.Version, superblockVersion)
	}
	sb.Flags = le.Uint16(buf[off:])
	off += 2
	copy(sb.UUID[:], buf[off:off+16])
	off += 16
	sb.VolumeSize = le.Uint64(buf[off:])
	off += 8
	if sb.VolumeSize == 0 {
		return superblock{}, errSuperblockBadVolume
	}
	sb.ExtentSize = le.Uint32(buf[off:])
	off += 4
	sb.BlockSize = le.Uint32(buf[off:])
	off += 4
	sb.WALOffset = le.Uint64(buf[off:])
	off += 8
	sb.WALSize = le.Uint64(buf[off:])
	off += 8
	sb.WALHead = le.Uint64(buf[off:])
	off += 8
	sb.WALTail = le.Uint64(buf[off:])
	off += 8
	sb.WALCheckpointLSN = le.Uint64(buf[off:])
	off += 8
	sb.CreatedAt = le.Uint64(buf[off:])
	return sb, nil
}

// validate checks internal consistency. Called after read and before
// any structural use.
func (sb *superblock) validate() error {
	if string(sb.Magic[:]) != superblockMagic {
		return errSuperblockNotOurs
	}
	if sb.Version != superblockVersion {
		return fmt.Errorf("%w: got %d", errSuperblockBadVer, sb.Version)
	}
	if sb.VolumeSize == 0 {
		return errSuperblockBadVolume
	}
	if sb.BlockSize == 0 {
		return fmt.Errorf("%w: BlockSize is 0", errSuperblockInvalid)
	}
	if sb.ExtentSize == 0 {
		return fmt.Errorf("%w: ExtentSize is 0", errSuperblockInvalid)
	}
	if sb.WALSize == 0 {
		return fmt.Errorf("%w: WALSize is 0", errSuperblockInvalid)
	}
	if sb.WALOffset != superblockSize {
		return fmt.Errorf("%w: WALOffset=%d, expected %d", errSuperblockInvalid, sb.WALOffset, superblockSize)
	}
	if sb.VolumeSize%uint64(sb.BlockSize) != 0 {
		return fmt.Errorf("%w: VolumeSize %d not aligned to BlockSize %d",
			errSuperblockInvalid, sb.VolumeSize, sb.BlockSize)
	}
	return nil
}

// createOptions configures a new store file.
type createOptions struct {
	BlockSize  uint32 // default 4096
	ExtentSize uint32 // default 64KB
	WALSize    uint64 // default 64MB
}
