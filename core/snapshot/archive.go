package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	archiveMagic        = "SWBSNP01"
	archiveVersion      = uint32(1)
	archiveHeaderSize   = 64
	recordHeaderSize    = 8
	maxArchiveBlockSize = 16 << 20
)

func writeArchive(ctx context.Context, path string, source storage.SnapshotSource) (storage.SnapshotCut, int64, string, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: create archive: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	if _, err := f.Write(make([]byte, archiveHeaderSize)); err != nil {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: write archive header: %w", err)
	}

	var count, dataBytes uint64
	var previousLBA uint32
	havePrevious := false
	observedBlockSize := 0
	cut, err := source.CaptureSnapshot(ctx, func(lba uint32, data []byte) error {
		if len(data) == 0 || len(data) > maxArchiveBlockSize {
			return fmt.Errorf("snapshot: source emitted invalid block size %d at LBA %d", len(data), lba)
		}
		if observedBlockSize == 0 {
			observedBlockSize = len(data)
		} else if len(data) != observedBlockSize {
			return fmt.Errorf("snapshot: source block size changed from %d to %d", observedBlockSize, len(data))
		}
		if havePrevious && lba <= previousLBA {
			return fmt.Errorf("snapshot: source LBAs not strictly ascending: %d after %d", lba, previousLBA)
		}
		var recordHeader [recordHeaderSize]byte
		binary.LittleEndian.PutUint32(recordHeader[0:4], lba)
		binary.LittleEndian.PutUint32(recordHeader[4:8], crc32.ChecksumIEEE(data))
		if _, err := f.Write(recordHeader[:]); err != nil {
			return fmt.Errorf("snapshot: write record header LBA %d: %w", lba, err)
		}
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("snapshot: write record data LBA %d: %w", lba, err)
		}
		count++
		dataBytes += uint64(len(data))
		previousLBA = lba
		havePrevious = true
		return nil
	})
	if err != nil {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: capture source: %w", err)
	}
	if cut.NumBlocks == 0 || cut.BlockSize <= 0 || cut.BlockSize > maxArchiveBlockSize {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: invalid source geometry blocks=%d block_size=%d", cut.NumBlocks, cut.BlockSize)
	}
	if observedBlockSize != 0 && observedBlockSize != cut.BlockSize {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: emitted block size %d != cut block size %d", observedBlockSize, cut.BlockSize)
	}
	if count != cut.BlockCount || dataBytes != cut.DataBytes {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: source counters do not reconcile emitted=(%d,%d) cut=(%d,%d)", count, dataBytes, cut.BlockCount, cut.DataBytes)
	}
	if havePrevious && previousLBA >= cut.NumBlocks {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: emitted LBA %d outside %d blocks", previousLBA, cut.NumBlocks)
	}
	header := encodeArchiveHeader(cut)
	if _, err := f.WriteAt(header, 0); err != nil {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: finalize archive header: %w", err)
	}
	if err := f.Sync(); err != nil {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: fsync archive: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: stat archive: %w", err)
	}
	if err := f.Close(); err != nil {
		return storage.SnapshotCut{}, 0, "", fmt.Errorf("snapshot: close archive: %w", err)
	}
	closed = true
	digest, err := digestFile(path)
	if err != nil {
		return storage.SnapshotCut{}, 0, "", err
	}
	return cut, info.Size(), digest, nil
}

func encodeArchiveHeader(cut storage.SnapshotCut) []byte {
	header := make([]byte, archiveHeaderSize)
	copy(header[0:8], archiveMagic)
	binary.LittleEndian.PutUint32(header[8:12], archiveVersion)
	binary.LittleEndian.PutUint32(header[12:16], uint32(cut.BlockSize))
	binary.LittleEndian.PutUint32(header[16:20], cut.NumBlocks)
	binary.LittleEndian.PutUint64(header[24:32], cut.Frontier)
	binary.LittleEndian.PutUint64(header[32:40], cut.BlockCount)
	binary.LittleEndian.PutUint64(header[40:48], cut.DataBytes)
	return header
}

func readArchive(ctx context.Context, path, expectedDigest string, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	digest, err := digestFile(path)
	if err != nil {
		return storage.SnapshotCut{}, fmt.Errorf("%w: %v", ErrArchiveCorrupt, err)
	}
	if digest != expectedDigest {
		return storage.SnapshotCut{}, fmt.Errorf("%w: digest got %s want %s", ErrArchiveCorrupt, digest, expectedDigest)
	}
	f, err := os.Open(path)
	if err != nil {
		return storage.SnapshotCut{}, fmt.Errorf("snapshot: open archive: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return storage.SnapshotCut{}, fmt.Errorf("snapshot: stat archive: %w", err)
	}
	cut, err := readArchiveRecords(ctx, f, sink)
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	expectedSize, err := archiveSize(cut)
	if err != nil || expectedSize != info.Size() {
		return storage.SnapshotCut{}, fmt.Errorf("%w: size got %d want %d", ErrArchiveCorrupt, info.Size(), expectedSize)
	}
	return cut, nil
}

// ApplyArchiveStream verifies one exact immutable archive while applying its
// records to an unpublished target. Callers must keep the target non-ready
// until this method and the target durability fence both succeed.
func ApplyArchiveStream(ctx context.Context, r io.Reader, rec Record, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	if r == nil || sink == nil || rec.ArchiveBytes <= 0 || validateRecord(rec) != nil {
		return storage.SnapshotCut{}, fmt.Errorf("%w: invalid streamed archive contract", ErrInvalidRequest)
	}
	h := sha256.New()
	tee := io.TeeReader(io.LimitReader(r, rec.ArchiveBytes+1), h)
	cut, err := readArchiveRecordsExpected(ctx, tee, sink, &rec)
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	var extra [1]byte
	if n, err := tee.Read(extra[:]); n != 0 || err != io.EOF {
		return storage.SnapshotCut{}, fmt.Errorf("%w: streamed archive has trailing or unread bytes", ErrArchiveCorrupt)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != rec.ArchiveSHA256 {
		return storage.SnapshotCut{}, fmt.Errorf("%w: digest got %s want %s", ErrArchiveCorrupt, got, rec.ArchiveSHA256)
	}
	expectedSize, err := archiveSize(cut)
	if err != nil || expectedSize != rec.ArchiveBytes {
		return storage.SnapshotCut{}, fmt.Errorf("%w: size got %d want %d", ErrArchiveCorrupt, rec.ArchiveBytes, expectedSize)
	}
	if cut.Frontier != rec.Frontier || cut.NumBlocks != rec.NumBlocks || cut.BlockSize != rec.BlockSize || cut.BlockCount != rec.RecordCount || cut.DataBytes != rec.DataBytes || rec.SizeBytes != uint64(cut.NumBlocks)*uint64(cut.BlockSize) {
		return storage.SnapshotCut{}, fmt.Errorf("%w: streamed archive does not match catalog", ErrArchiveCorrupt)
	}
	return cut, nil
}

func readArchiveRecords(ctx context.Context, r io.Reader, sink storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	return readArchiveRecordsExpected(ctx, r, sink, nil)
}

func readArchiveRecordsExpected(ctx context.Context, r io.Reader, sink storage.SnapshotBlockSink, expected *Record) (storage.SnapshotCut, error) {
	header := make([]byte, archiveHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return storage.SnapshotCut{}, fmt.Errorf("%w: read header: %v", ErrArchiveCorrupt, err)
	}
	cut, err := decodeArchiveHeader(header)
	if err != nil {
		return storage.SnapshotCut{}, err
	}
	if expected != nil && (cut.Frontier != expected.Frontier || cut.NumBlocks != expected.NumBlocks || cut.BlockSize != expected.BlockSize || cut.BlockCount != expected.RecordCount || cut.DataBytes != expected.DataBytes) {
		return storage.SnapshotCut{}, fmt.Errorf("%w: streamed archive header does not match catalog", ErrArchiveCorrupt)
	}

	var previousLBA uint32
	for i := uint64(0); i < cut.BlockCount; i++ {
		if err := ctx.Err(); err != nil {
			return storage.SnapshotCut{}, err
		}
		var recordHeader [recordHeaderSize]byte
		if _, err := io.ReadFull(r, recordHeader[:]); err != nil {
			return storage.SnapshotCut{}, fmt.Errorf("%w: read record %d header: %v", ErrArchiveCorrupt, i, err)
		}
		lba := binary.LittleEndian.Uint32(recordHeader[0:4])
		wantCRC := binary.LittleEndian.Uint32(recordHeader[4:8])
		if lba >= cut.NumBlocks || (i > 0 && lba <= previousLBA) {
			return storage.SnapshotCut{}, fmt.Errorf("%w: invalid LBA order at record %d", ErrArchiveCorrupt, i)
		}
		data := make([]byte, cut.BlockSize)
		if _, err := io.ReadFull(r, data); err != nil {
			return storage.SnapshotCut{}, fmt.Errorf("%w: read record %d data: %v", ErrArchiveCorrupt, i, err)
		}
		if got := crc32.ChecksumIEEE(data); got != wantCRC {
			return storage.SnapshotCut{}, fmt.Errorf("%w: LBA %d CRC got %08x want %08x", ErrArchiveCorrupt, lba, got, wantCRC)
		}
		if sink != nil {
			if err := sink(lba, data); err != nil {
				return storage.SnapshotCut{}, err
			}
		}
		previousLBA = lba
	}
	return cut, nil
}

func decodeArchiveHeader(header []byte) (storage.SnapshotCut, error) {
	if len(header) != archiveHeaderSize || string(header[0:8]) != archiveMagic {
		return storage.SnapshotCut{}, fmt.Errorf("%w: invalid archive magic", ErrArchiveCorrupt)
	}
	if version := binary.LittleEndian.Uint32(header[8:12]); version != archiveVersion {
		return storage.SnapshotCut{}, fmt.Errorf("%w: unsupported archive version %d", ErrArchiveCorrupt, version)
	}
	cut := storage.SnapshotCut{
		BlockSize:  int(binary.LittleEndian.Uint32(header[12:16])),
		NumBlocks:  binary.LittleEndian.Uint32(header[16:20]),
		Frontier:   binary.LittleEndian.Uint64(header[24:32]),
		BlockCount: binary.LittleEndian.Uint64(header[32:40]),
		DataBytes:  binary.LittleEndian.Uint64(header[40:48]),
	}
	if cut.BlockSize <= 0 || cut.BlockSize > maxArchiveBlockSize || cut.NumBlocks == 0 || cut.BlockCount > uint64(cut.NumBlocks) || cut.DataBytes != cut.BlockCount*uint64(cut.BlockSize) {
		return storage.SnapshotCut{}, fmt.Errorf("%w: invalid archive geometry", ErrArchiveCorrupt)
	}
	return cut, nil
}

func archiveSize(cut storage.SnapshotCut) (int64, error) {
	perRecord := uint64(recordHeaderSize) + uint64(cut.BlockSize)
	if cut.BlockCount > uint64(^uint64(0)-archiveHeaderSize)/perRecord {
		return 0, fmt.Errorf("%w: archive size overflow", ErrArchiveCorrupt)
	}
	size := uint64(archiveHeaderSize) + cut.BlockCount*perRecord
	if size > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("%w: archive too large", ErrArchiveCorrupt)
	}
	return int64(size), nil
}

func digestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("snapshot: open archive for digest: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("snapshot: digest archive: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
