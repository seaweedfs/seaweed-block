package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"syscall"
)

const checksumUnit = 4096

var checksumTable = crc32.MakeTable(crc32.Castagnoli)
var checksumZeros [checksumUnit]byte
var checksumZeroCRC = crc32.Checksum(checksumZeros[:], checksumTable)

// ErrDataChecksum identifies wrong extent or remote-chunk bytes. A mismatch
// is also EIO; callers must never return the unverified bytes as successful IO.
var ErrDataChecksum = fmt.Errorf("storage: data checksum mismatch: %w", syscall.EIO)
var ErrDataChecksumsRequired = errors.New("storage: data checksums required; walstore v1 is unprotected (no automatic migration)")

type DataChecksumFailure struct {
	LBA    uint32
	Detail string
}

func (e *DataChecksumFailure) Error() string {
	return fmt.Sprintf("%s: LBA %d: %s", ErrDataChecksum, e.LBA, e.Detail)
}
func (e *DataChecksumFailure) Is(target error) bool {
	return target == ErrDataChecksum || target == syscall.EIO
}

func checksumWords(sb *superblock) uint64 {
	return (uint64(sb.BlockSize) + checksumUnit - 1) / checksumUnit
}
func checksumBase(sb *superblock) uint64 { return sb.WALOffset + sb.WALSize + sb.VolumeSize }
func walstoreFileSize(sb *superblock) (int64, error) {
	base := checksumBase(sb)
	if base < sb.VolumeSize || base > math.MaxInt64 {
		return 0, errors.New("storage: walstore size overflow")
	}
	if sb.ImplVersion == 1 {
		return int64(base), nil
	}
	if sb.ImplVersion != WALStoreImplVersion {
		return 0, fmt.Errorf("storage: unsupported walstore implementation version %d", sb.ImplVersion)
	}
	size := (sb.VolumeSize / uint64(sb.BlockSize)) * checksumWords(sb) * 4
	if size > math.MaxInt64-base {
		return 0, errors.New("storage: checksum region size overflow")
	}
	return int64(base + size), nil
}
func validateWALStoreFile(f *os.File, sb *superblock, require bool) error {
	if require && sb.ImplVersion == 1 {
		return ErrDataChecksumsRequired
	}
	size, err := walstoreFileSize(sb)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < size {
		return fmt.Errorf("storage: incomplete walstore format %d: size %d need %d: %w", sb.ImplVersion, info.Size(), size, io.ErrUnexpectedEOF)
	}
	return nil
}
func normalizedCRC(data []byte) uint32 {
	zero := checksumZeroCRC
	if len(data) != checksumUnit {
		zero = crc32.Checksum(checksumZeros[:len(data)], checksumTable)
	}
	return crc32.Checksum(data, checksumTable) ^ zero
}

// XOR with the zero-unit CRC makes a sparse zero checksum region validate
// only a zero-filled new extent; zero entries are never a bypass bit.
func writeExtentChecksum(f *os.File, sb *superblock, lba uint32, data []byte) error {
	if sb.ImplVersion == 1 {
		return nil
	}
	words := checksumWords(sb)
	sums := make([]byte, words*4)
	for i := uint64(0); i < words; i++ {
		start := i * checksumUnit
		end := start + checksumUnit
		if end > uint64(len(data)) {
			end = uint64(len(data))
		}
		binary.LittleEndian.PutUint32(sums[i*4:], normalizedCRC(data[start:end]))
	}
	off := int64(checksumBase(sb) + uint64(lba)*words*4)
	n, err := f.WriteAt(sums, off)
	if err == nil && n != len(sums) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("storage: write checksum LBA %d: %w", lba, err)
	}
	return nil
}
func verifyExtentChecksum(f *os.File, sb *superblock, lba uint32, data []byte) error {
	if sb.ImplVersion == 1 {
		return nil
	}
	words := checksumWords(sb)
	sums := make([]byte, words*4)
	if _, err := f.ReadAt(sums, int64(checksumBase(sb)+uint64(lba)*words*4)); err != nil {
		return fmt.Errorf("storage: read checksum LBA %d: %w", lba, err)
	}
	for i := uint64(0); i < words; i++ {
		start := i * checksumUnit
		end := start + checksumUnit
		if end > uint64(len(data)) {
			end = uint64(len(data))
		}
		want := binary.LittleEndian.Uint32(sums[i*4:])
		if got := normalizedCRC(data[start:end]); got != want {
			return &DataChecksumFailure{LBA: lba, Detail: fmt.Sprintf("unit %d CRC32C got %08x want %08x", i, got, want)}
		}
	}
	return nil
}
func (s *WALStore) FormatVersion() uint32 { return s.sb.ImplVersion }
func (s *WALStore) DataChecksums() bool   { return s.sb.ImplVersion == WALStoreImplVersion }
