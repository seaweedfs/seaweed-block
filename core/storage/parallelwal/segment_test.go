package parallelwal

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func segmentTestRecord(lsn uint64, lba uint32, fill byte, blockSize uint32) walRecord {
	return walRecord{
		LSN:   lsn,
		LBA:   lba,
		Flags: flagWrite,
		Data:  testBlock(fill, int(blockSize)),
	}
}

func resealTestSegment(buf []byte) {
	binary.LittleEndian.PutUint32(buf[segmentEntriesCRCOffset:52],
		crc32.ChecksumIEEE(buf[segmentHeaderSize:]))
	binary.LittleEndian.PutUint32(buf[segmentHeaderCRCOffset:64],
		crc32.ChecksumIEEE(buf[:segmentHeaderCRCOffset]))
}

func resealTestSegmentEntry(buf []byte, index int, blockSize uint32) {
	count := int(binary.LittleEndian.Uint32(buf[12:16]))
	entryOffset := segmentHeaderSize + index*segmentEntryHeaderSize
	payloadOffset := segmentHeaderSize + count*segmentEntryHeaderSize + index*int(blockSize)
	entry := buf[entryOffset : entryOffset+segmentEntryHeaderSize]
	payload := buf[payloadOffset : payloadOffset+int(blockSize)]
	entryCRC := crc32.NewIEEE()
	_, _ = entryCRC.Write(entry[:28])
	_, _ = entryCRC.Write(payload)
	binary.LittleEndian.PutUint32(entry[28:32], entryCRC.Sum32())
	resealTestSegment(buf)
}

func TestSegmentEncodeDecodeAllowsOrderedSameLBAWrites(t *testing.T) {
	const (
		blockSize = uint32(512)
		numBlocks = uint32(16)
	)
	encoded, err := encodeSegment(7, []walRecord{
		segmentTestRecord(11, 3, 0x11, blockSize),
		segmentTestRecord(12, 3, 0x22, blockSize),
		segmentTestRecord(13, 8, 0x33, blockSize),
	}, blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeSegment(encoded, blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Sequence != 7 || len(decoded.Records) != 3 {
		t.Fatalf("decoded sequence/records=%d/%d", decoded.Sequence, len(decoded.Records))
	}
	for i, rec := range decoded.Records {
		if rec.LSN != uint64(11+i) {
			t.Fatalf("record %d LSN=%d", i, rec.LSN)
		}
	}
	if decoded.Records[0].LBA != 3 || decoded.Records[1].LBA != 3 ||
		decoded.Records[0].Data[0] != 0x11 || decoded.Records[1].Data[0] != 0x22 {
		t.Fatal("same-LBA writes lost order or payload")
	}
}

func TestSegmentFormatGoldenVector(t *testing.T) {
	const goldenHex = "505753470100400064000000010000000400000000000000050000000000000005000000000000000300000000000000966002fb0000000000000000bc06d2c205000000000000000200000001000000600000000400000000b98ae092979dfc10203040"
	golden, err := hex.DecodeString(goldenHex)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeSegment(3, []walRecord{{
		LSN: 5, LBA: 2, Flags: flagWrite, Data: []byte{0x10, 0x20, 0x30, 0x40},
	}}, 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(golden) {
		t.Fatalf("encoded segment changed:\ngot  %x\nwant %x", encoded, golden)
	}
	decoded, err := decodeSegment(golden, 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Sequence != 3 || len(decoded.Records) != 1 ||
		decoded.Records[0].LSN != 5 || decoded.Records[0].LBA != 2 ||
		string(decoded.Records[0].Data) != string([]byte{0x10, 0x20, 0x30, 0x40}) {
		t.Fatalf("decoded golden=%+v", decoded)
	}
}

func TestSegmentEncodeRejectsInvalidGeometryAndOrder(t *testing.T) {
	const blockSize = uint32(512)
	tests := []struct {
		name     string
		sequence uint64
		records  []walRecord
	}{
		{name: "zero sequence", records: []walRecord{segmentTestRecord(1, 0, 1, blockSize)}},
		{name: "empty", sequence: 1},
		{
			name:     "non-contiguous LSN",
			sequence: 1,
			records: []walRecord{
				segmentTestRecord(1, 0, 1, blockSize),
				segmentTestRecord(3, 1, 2, blockSize),
			},
		},
		{
			name:     "invalid LBA",
			sequence: 1,
			records:  []walRecord{segmentTestRecord(1, 8, 1, blockSize)},
		},
		{
			name:     "invalid flags",
			sequence: 1,
			records: []walRecord{{
				LSN: 1, LBA: 0, Flags: 9, Data: testBlock(1, int(blockSize)),
			}},
		},
		{
			name:     "invalid payload",
			sequence: 1,
			records: []walRecord{{
				LSN: 1, LBA: 0, Flags: flagWrite, Data: []byte{1},
			}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := encodeSegment(tc.sequence, tc.records, blockSize, 8); err == nil {
				t.Fatal("encode unexpectedly succeeded")
			}
		})
	}
	if _, err := segmentEncodedSize(maxSegmentEntries+1, blockSize); !errors.Is(err, errSegmentBounds) {
		t.Fatalf("entry bound error=%v", err)
	}
	if _, err := segmentEncodedSize(2, maxSegmentPayloadBytes); !errors.Is(err, errSegmentBounds) {
		t.Fatalf("payload bound error=%v", err)
	}
}

func TestSegmentDecodeRejectsCorruptionAndBounds(t *testing.T) {
	const (
		blockSize = uint32(512)
		numBlocks = uint32(8)
	)
	base, err := encodeSegment(1, []walRecord{
		segmentTestRecord(1, 0, 0x11, blockSize),
		segmentTestRecord(2, 1, 0x22, blockSize),
	}, blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	clone := func() []byte { return append([]byte(nil), base...) }
	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   string
	}{
		{
			name: "truncated header",
			mutate: func(buf []byte) []byte {
				return buf[:segmentHeaderSize-1]
			},
		},
		{
			name: "bad header CRC",
			mutate: func(buf []byte) []byte {
				buf[24] ^= 0xff
				return buf
			},
			want: "header CRC",
		},
		{
			name: "bad entries CRC",
			mutate: func(buf []byte) []byte {
				buf[segmentHeaderSize+14] ^= 0xff
				return buf
			},
			want: "entries CRC",
		},
		{
			name: "bad entry CRC",
			mutate: func(buf []byte) []byte {
				buf[segmentHeaderSize+28] ^= 0xff
				resealTestSegment(buf)
				return buf
			},
			want: "entry 0 CRC",
		},
		{
			name: "duplicate LSN",
			mutate: func(buf []byte) []byte {
				second := segmentHeaderSize + segmentEntryHeaderSize
				binary.LittleEndian.PutUint64(buf[second:second+8], 1)
				resealTestSegmentEntry(buf, 1, blockSize)
				return buf
			},
		},
		{
			name: "invalid LBA",
			mutate: func(buf []byte) []byte {
				binary.LittleEndian.PutUint32(buf[segmentHeaderSize+8:segmentHeaderSize+12], numBlocks)
				resealTestSegmentEntry(buf, 0, blockSize)
				return buf
			},
		},
		{
			name: "non-canonical payload offset",
			mutate: func(buf []byte) []byte {
				binary.LittleEndian.PutUint32(buf[segmentHeaderSize+16:segmentHeaderSize+20], 0)
				resealTestSegmentEntry(buf, 0, blockSize)
				return buf
			},
		},
		{
			name: "bad data CRC",
			mutate: func(buf []byte) []byte {
				payloadStart := segmentHeaderSize + 2*segmentEntryHeaderSize
				buf[payloadStart] ^= 0xff
				resealTestSegmentEntry(buf, 0, blockSize)
				return buf
			},
			want: "entry 0 data CRC",
		},
		{
			name: "truncated payload",
			mutate: func(buf []byte) []byte {
				return buf[:len(buf)-1]
			},
		},
		{
			name: "oversized count before allocation",
			mutate: func(buf []byte) []byte {
				binary.LittleEndian.PutUint32(buf[12:16], ^uint32(0))
				binary.LittleEndian.PutUint32(buf[segmentHeaderCRCOffset:64],
					crc32.ChecksumIEEE(buf[:segmentHeaderCRCOffset]))
				return buf
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeSegment(tc.mutate(clone()), blockSize, numBlocks)
			if err == nil {
				t.Fatal("decode unexpectedly succeeded")
			}
			if !errors.Is(err, errBadSegment) && !errors.Is(err, errSegmentBounds) {
				t.Fatalf("decode error=%v", err)
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestRecoverCommittedSegmentsIgnoresOnlyUncommittedTail(t *testing.T) {
	const (
		blockSize = uint32(512)
		numBlocks = uint32(8)
	)
	first, err := encodeSegment(9,
		[]walRecord{segmentTestRecord(10, 0, 0x10, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeSegment(10, []walRecord{
		segmentTestRecord(11, 1, 0x11, blockSize),
		segmentTestRecord(12, 1, 0x12, blockSize),
	}, blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	third, err := encodeSegment(11,
		[]walRecord{segmentTestRecord(13, 2, 0x13, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "segments.bin")
	physical := append(append(append([]byte(nil), first...), second...), third[:len(third)/2]...)
	if err := os.WriteFile(path, physical, 0o600); err != nil {
		t.Fatal(err)
	}
	committedBytes := int64(len(first) + len(second))
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var segments []walSegment
	window := segmentRecoveryWindow{
		CommittedBytes: committedBytes,
		SegmentCount:   2,
		FirstSequence:  9,
		FirstLSN:       10,
		LastLSN:        12,
	}
	err = scanCommittedSegments(f, window, blockSize, numBlocks, func(segment walSegment) error {
		segments = append(segments, segment)
		return nil
	})
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 {
		t.Fatalf("recovered segment count=%d", len(segments))
	}
	if len(segments[0].Records) != 1 || len(segments[1].Records) != 2 {
		t.Fatalf("recovered segment shape=%v", []int{len(segments[0].Records), len(segments[1].Records)})
	}
	if got := segments[1].Records[1]; got.LSN != 12 || got.LBA != 1 || got.Data[0] != 0x12 {
		t.Fatalf("last recovered record=%+v", got)
	}

	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tornWindow := segmentRecoveryWindow{
		CommittedBytes: int64(len(physical)),
		SegmentCount:   3,
		FirstSequence:  9,
		FirstLSN:       10,
		LastLSN:        13,
	}
	err = scanCommittedSegments(f, tornWindow, blockSize, numBlocks, nil)
	_ = f.Close()
	if !errors.Is(err, errBadSegment) {
		t.Fatalf("committed torn tail error=%v", err)
	}
}

func TestRecoverCommittedSegmentsFailsClosedOnCommittedCorruption(t *testing.T) {
	const (
		blockSize = uint32(512)
		numBlocks = uint32(8)
	)
	first, err := encodeSegment(1,
		[]walRecord{segmentTestRecord(1, 0, 0x10, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeSegment(2,
		[]walRecord{segmentTestRecord(2, 1, 0x20, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	all := append(append([]byte(nil), first...), second...)
	all[len(first)+segmentHeaderSize+segmentEntryHeaderSize] ^= 0xff
	path := filepath.Join(t.TempDir(), "segments.bin")
	if err := os.WriteFile(path, all, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	window := segmentRecoveryWindow{
		CommittedBytes: int64(len(all)),
		SegmentCount:   2,
		FirstSequence:  1,
		FirstLSN:       1,
		LastLSN:        2,
	}
	err = scanCommittedSegments(f, window, blockSize, numBlocks, nil)
	_ = f.Close()
	if !errors.Is(err, errBadSegment) {
		t.Fatalf("committed corruption error=%v", err)
	}
}

func TestRecoverCommittedSegmentsRejectsSequenceAndLSNGaps(t *testing.T) {
	const (
		blockSize = uint32(512)
		numBlocks = uint32(8)
	)
	first, err := encodeSegment(1,
		[]walRecord{segmentTestRecord(1, 0, 0x10, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		sequence uint64
		firstLSN uint64
		want     string
	}{
		{name: "sequence gap", sequence: 3, firstLSN: 2, want: "segment sequence"},
		{name: "LSN gap", sequence: 2, firstLSN: 3, want: "segment first LSN"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			second, err := encodeSegment(tc.sequence,
				[]walRecord{segmentTestRecord(tc.firstLSN, 1, 0x20, blockSize)},
				blockSize, numBlocks)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "segments.bin")
			all := append(append([]byte(nil), first...), second...)
			if err := os.WriteFile(path, all, 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			window := segmentRecoveryWindow{
				CommittedBytes: int64(len(all)),
				SegmentCount:   2,
				FirstSequence:  1,
				FirstLSN:       1,
				LastLSN:        tc.firstLSN,
			}
			err = scanCommittedSegments(f, window, blockSize, numBlocks, nil)
			_ = f.Close()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("recovery error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestScanCommittedSegmentsRequiresManifestAnchors(t *testing.T) {
	const (
		blockSize = uint32(512)
		numBlocks = uint32(8)
	)
	first, err := encodeSegment(1,
		[]walRecord{segmentTestRecord(1, 0, 0x10, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeSegment(2,
		[]walRecord{segmentTestRecord(2, 1, 0x20, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		data   []byte
		window segmentRecoveryWindow
		want   string
	}{
		{
			name: "missing first segment",
			data: second,
			window: segmentRecoveryWindow{
				CommittedBytes: int64(len(second)),
				SegmentCount:   1,
				FirstSequence:  1,
				FirstLSN:       1,
				LastLSN:        1,
			},
			want: "segment sequence=2 want=1",
		},
		{
			name: "durable frontier not reached",
			data: first,
			window: segmentRecoveryWindow{
				CommittedBytes: int64(len(first)),
				SegmentCount:   1,
				FirstSequence:  1,
				FirstLSN:       1,
				LastLSN:        2,
			},
			want: "segments/lastLSN=1/1 want=1/2",
		},
		{
			name: "segment count not reached",
			data: first,
			window: segmentRecoveryWindow{
				CommittedBytes: int64(len(first)),
				SegmentCount:   2,
				FirstSequence:  1,
				FirstLSN:       1,
				LastLSN:        2,
			},
			want: "segments/lastLSN=1/1 want=2/2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "segments.bin")
			if err := os.WriteFile(path, tc.data, 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			err = scanCommittedSegments(f, tc.window, blockSize, numBlocks, nil)
			_ = f.Close()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("scan error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestScanCommittedSegmentsReplaysSameLBAInLSNOrder(t *testing.T) {
	const (
		blockSize = uint32(512)
		numBlocks = uint32(8)
	)
	first, err := encodeSegment(4,
		[]walRecord{segmentTestRecord(20, 3, 0x20, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeSegment(5,
		[]walRecord{segmentTestRecord(21, 3, 0x21, blockSize)},
		blockSize, numBlocks)
	if err != nil {
		t.Fatal(err)
	}
	all := append(append([]byte(nil), first...), second...)
	path := filepath.Join(t.TempDir(), "segments.bin")
	if err := os.WriteFile(path, all, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	latest := make(map[uint32]walRecord)
	window := segmentRecoveryWindow{
		CommittedBytes: int64(len(all)),
		SegmentCount:   2,
		FirstSequence:  4,
		FirstLSN:       20,
		LastLSN:        21,
	}
	err = scanCommittedSegments(f, window, blockSize, numBlocks, func(segment walSegment) error {
		for _, record := range segment.Records {
			latest[record.LBA] = record
		}
		return nil
	})
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got := latest[3]; got.LSN != 21 || got.Data[0] != 0x21 {
		t.Fatalf("latest same-LBA record=%+v", got)
	}
}
