package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

// The impl-3 codec is deliberately separate from the 38-byte impl-1 codec.
// No production writer, reader or recovery path selects it before activation.
const (
	walEntryPrefixSizeV3 = 62
	walEntryHeaderSizeV3 = 70 // prefix plus CRC32C and EntrySize
)

var walEntryCRC32CV3 = crc32.MakeTable(crc32.Castagnoli)

type walEntryV3 struct {
	LSN          uint64
	BatchCount   uint64
	Type         uint8
	Flags        uint8
	LBA          uint64
	Length       uint32
	DurableLSN   uint64
	LogicalStart uint64
	Nonce        [16]byte
	Data         []byte
}

// payloadSizeV3 validates header arithmetic before reading or allocating the
// payload. The reader passes geometry from the selected, validated descriptor.
// Nonce equality, exact expected cursor and lap admission belong to the scanner;
// a valid CRC here alone never makes a record trustworthy for replay.
func (e walEntryV3) payloadSizeV3(blockSize uint32, numBlocks uint64) (uint64, error) {
	invalid := func(why string) (uint64, error) { return 0, fmt.Errorf("%w: v3 %s", errInvalidEntry, why) }
	if blockSize == 0 || numBlocks == 0 || numBlocks > counterMaxV3/uint64(blockSize) {
		return invalid("invalid volume geometry")
	}
	if e.Nonce == ([16]byte{}) || e.Flags != 0 {
		return invalid("nonce or flags")
	}
	if e.LSN > counterMaxV3 || e.DurableLSN > counterMaxV3 || e.LogicalStart > counterMaxV3 || e.LBA > counterMaxV3 || e.BatchCount > counterMaxV3 {
		return 0, ErrCounterExhausted
	}
	if e.Type != walEntryPadding && e.LSN == 0 {
		return invalid("zero record LSN")
	}
	var payload, blocks uint64
	switch e.Type {
	case walEntryWrite:
		if e.BatchCount != 0 || e.Length != blockSize {
			return invalid("single write geometry")
		}
		blocks = 1
		payload = uint64(e.Length)
	case walEntryWriteBatch:
		if e.BatchCount == 0 {
			return invalid("empty batch")
		}
		if e.BatchCount > counterMaxV3/uint64(blockSize) || e.BatchCount-1 > counterMaxV3-e.LSN {
			return 0, ErrCounterExhausted
		}
		blocks = e.BatchCount
		payload = blocks * uint64(blockSize)
		if payload != uint64(e.Length) {
			return invalid("batch payload length")
		}
	case walEntryTrim:
		if e.BatchCount != 0 || e.Length == 0 || e.Length%blockSize != 0 {
			return invalid("trim geometry")
		}
		blocks = uint64(e.Length / blockSize)
	case walEntryBarrier:
		if e.BatchCount != 0 || e.Length != 0 {
			return invalid("barrier geometry")
		}
	case walEntryPadding:
		if e.BatchCount != 0 || e.LSN != 0 || e.LBA != 0 || e.DurableLSN != 0 {
			return invalid("padding header")
		}
		payload = uint64(e.Length)
	default:
		return invalid("unknown record type")
	}
	if blocks > 0 && (e.LBA >= numBlocks || blocks > numBlocks-e.LBA) {
		return invalid("LBA span outside volume")
	}
	// EntrySize is a 32-bit wire field. Never truncate it or an allocation size.
	if payload > math.MaxUint32-walEntryHeaderSizeV3 || payload > uint64(int(^uint(0)>>1))-walEntryHeaderSizeV3 {
		return invalid("encoded record too large")
	}
	if uint64(walEntryHeaderSizeV3)+payload > counterMaxV3-e.LogicalStart {
		return 0, ErrCounterExhausted
	}
	return payload, nil
}

func encodeWALEntryV3(e walEntryV3, blockSize uint32, numBlocks uint64) ([]byte, error) {
	return encodeWALEntryV3Instrumented(e, blockSize, numBlocks, nil)
}

func encodeWALEntryV3Instrumented(e walEntryV3, blockSize uint32, numBlocks uint64, instr *writeInstrumentation) ([]byte, error) {
	start := time.Now()
	n, err := e.payloadSizeV3(blockSize, numBlocks)
	if err != nil {
		return nil, err
	}
	if uint64(len(e.Data)) != n {
		return nil, fmt.Errorf("%w: v3 payload size", errInvalidEntry)
	}
	if e.Type == walEntryPadding {
		for _, b := range e.Data {
			if b != 0 {
				return nil, fmt.Errorf("%w: v3 nonzero padding", errInvalidEntry)
			}
		}
	}
	buf := make([]byte, walEntryHeaderSizeV3+int(n))
	le := binary.LittleEndian
	le.PutUint64(buf[0:8], e.LSN)
	le.PutUint64(buf[8:16], e.BatchCount)
	buf[16] = e.Type
	buf[17] = e.Flags
	le.PutUint64(buf[18:26], e.LBA)
	le.PutUint32(buf[26:30], e.Length)
	le.PutUint64(buf[30:38], e.DurableLSN)
	le.PutUint64(buf[38:46], e.LogicalStart)
	copy(buf[46:62], e.Nonce[:])
	end := walEntryPrefixSizeV3 + int(n)
	copyStart := time.Now()
	copy(buf[walEntryPrefixSizeV3:end], e.Data)
	if instr != nil {
		instr.recordWALCopy(len(e.Data), time.Since(copyStart))
	}
	checksumStart := time.Now()
	le.PutUint32(buf[end:end+4], crc32.Checksum(buf[:end], walEntryCRC32CV3))
	if instr != nil {
		instr.recordWALChecksum(end, time.Since(checksumStart))
	}
	le.PutUint32(buf[end+4:end+8], uint32(len(buf)))
	if instr != nil {
		instr.recordWALEncode(len(buf), time.Since(start))
	}
	return buf, nil
}

func decodeWALEntryV3(buf []byte, blockSize uint32, numBlocks uint64) (walEntryV3, error) {
	if len(buf) < walEntryHeaderSizeV3 {
		return walEntryV3{}, errEntryTruncated
	}
	le := binary.LittleEndian
	e := entryPrefixV3(buf)
	n, err := e.payloadSizeV3(blockSize, numBlocks)
	if err != nil {
		return walEntryV3{}, err
	}
	size := uint64(walEntryHeaderSizeV3) + n
	if size > uint64(len(buf)) {
		return walEntryV3{}, errEntryTruncated
	}
	end := walEntryPrefixSizeV3 + int(n)
	if le.Uint32(buf[end+4:end+8]) != uint32(size) {
		return walEntryV3{}, fmt.Errorf("%w: v3 EntrySize", errInvalidEntry)
	}
	if le.Uint32(buf[end:end+4]) != crc32.Checksum(buf[:end], walEntryCRC32CV3) {
		return walEntryV3{}, errCRCMismatch
	}
	if e.Type == walEntryPadding {
		for _, b := range buf[walEntryPrefixSizeV3:end] {
			if b != 0 {
				return walEntryV3{}, fmt.Errorf("%w: v3 nonzero padding", errInvalidEntry)
			}
		}
	}
	// Only copy after geometry, source bounds, CRC and footer all validate.
	if n > 0 {
		e.Data = append([]byte(nil), buf[walEntryPrefixSizeV3:end]...)
	}
	return e, nil
}

// entryPrefixV3 decodes only the fixed prefix; payloadSizeV3 validates all
// geometry before either a whole-record allocation or a bounded block scan.
func entryPrefixV3(buf []byte) walEntryV3 {
	le := binary.LittleEndian
	e := walEntryV3{LSN: le.Uint64(buf[0:8]), BatchCount: le.Uint64(buf[8:16]), Type: buf[16], Flags: buf[17], LBA: le.Uint64(buf[18:26]), Length: le.Uint32(buf[26:30]), DurableLSN: le.Uint64(buf[30:38]), LogicalStart: le.Uint64(buf[38:46])}
	copy(e.Nonce[:], buf[46:62])
	return e
}
