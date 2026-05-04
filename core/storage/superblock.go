package storage

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// File-format identity. Bumped only on incompatible layout changes.
//
// Version history:
//   1: initial layout (magic+version+geometry+WAL state+CreatedAt)
//   2: T3a Addendum A — added ImplKind + ImplVersion at end of
//      layout. Opening a v1 store with v2 code returns
//      errSuperblockBadVer (one-way bump; no compat read path).
const (
	superblockSize    = 4096
	superblockMagic   = "SWBK"
	superblockVersion = 2
)

// ImplKind self-identifies which LogicalStorage implementation
// owns this file. DurableProvider compares the stored ImplKind
// against the selector at Open time and rejects mismatches with
// errSuperblockImplMismatch — prevents silently opening a
// walstore file as smartwal (or vice versa), which would then
// attempt an incompatible format parse. ImplKindUnset (0) on
// disk means the file predates T3a and is a format error.
type ImplKind uint8

const (
	ImplKindUnset    ImplKind = 0
	ImplKindWALStore ImplKind = 1
	ImplKindSmartWAL ImplKind = 2
)

// WALStoreImplVersion is the per-impl schema version for walstore
// files. Independent from superblockVersion (which is the shared
// superblock-format version) — walstore and smartwal evolve their
// WAL+extent layouts on independent clocks.
const WALStoreImplVersion uint32 = 1

var (
	errSuperblockNotOurs     = errors.New("storage: not a store file (bad magic)")
	errSuperblockBadVer      = errors.New("storage: unsupported store version")
	errSuperblockBadVolume   = errors.New("storage: invalid volume size")
	errSuperblockInvalid     = errors.New("storage: invalid superblock")
	errSuperblockImplUnknown = errors.New("storage: unknown impl kind")
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

	// T3a Addendum A Addition 2 — impl self-identity.
	// ImplKind is the store implementation (walstore vs smartwal);
	// ImplVersion is the per-impl schema version. DurableProvider
	// compares stored ImplKind against the selector at Open time
	// and rejects mismatches fast.
	ImplKind    ImplKind
	ImplVersion uint32
}

// newSuperblock constructs a superblock with defaults and a fresh
// UUID. Caller-supplied geometry overrides defaults. The caller
// MUST pass a valid ImplKind (WALStore / SmartWAL) — unset (0)
// rejected at validate() time so no file can ever persist with
// unidentified impl.
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
	if opts.ImplKind == ImplKindUnset {
		return superblock{}, fmt.Errorf("%w: ImplKind is unset (0)", errSuperblockImplUnknown)
	}
	if opts.ImplKind != ImplKindWALStore && opts.ImplKind != ImplKindSmartWAL {
		return superblock{}, fmt.Errorf("%w: ImplKind=%d", errSuperblockImplUnknown, opts.ImplKind)
	}
	implVersion := opts.ImplVersion
	if implVersion == 0 {
		implVersion = WALStoreImplVersion
	}
	sb := superblock{
		Version:     superblockVersion,
		VolumeSize:  volumeSize,
		ExtentSize:  extentSize,
		BlockSize:   blockSize,
		WALOffset:   superblockSize,
		WALSize:     walSize,
		ImplKind:    opts.ImplKind,
		ImplVersion: implVersion,
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
	off += 8
	// T3a Addendum A Addition 2 — ImplKind + ImplVersion.
	buf[off] = uint8(sb.ImplKind)
	off++
	// 3 bytes reserved for future ImplKind expansion / alignment.
	off += 3
	le.PutUint32(buf[off:], sb.ImplVersion)

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
	off += 8
	// T3a Addendum A Addition 2.
	sb.ImplKind = ImplKind(buf[off])
	off++
	off += 3 // reserved
	sb.ImplVersion = le.Uint32(buf[off:])
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
	// T3a Addendum A: ImplKind must be set and recognized.
	if sb.ImplKind == ImplKindUnset {
		return fmt.Errorf("%w: ImplKind is unset (0); file predates T3a or is corrupted",
			errSuperblockImplUnknown)
	}
	if sb.ImplKind != ImplKindWALStore && sb.ImplKind != ImplKindSmartWAL {
		return fmt.Errorf("%w: ImplKind=%d", errSuperblockImplUnknown, sb.ImplKind)
	}
	return nil
}

// createOptions configures a new store file.
type createOptions struct {
	BlockSize  uint32 // default 4096
	ExtentSize uint32 // default 64KB
	WALSize    uint64 // default 64MB

	// T3a Addendum A — impl self-identity. Required (no default);
	// newSuperblock rejects ImplKindUnset (0).
	ImplKind    ImplKind
	ImplVersion uint32 // default WALStoreImplVersion
}
