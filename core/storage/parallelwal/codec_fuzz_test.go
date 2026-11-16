package parallelwal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"testing"
)

var fuzzBlockSizes = []int{1, 16, 512, 4096}

func fuzzBlockSize(sel uint8) int { return fuzzBlockSizes[int(sel)%len(fuzzBlockSizes)] }

// FuzzParallelWALDecodeRecord: decodeRecord never panics; a rejected record
// is the zero value; an accepted one re-encodes to exactly its bytes (every
// header byte is a defined field).
func FuzzParallelWALDecodeRecord(f *testing.F) {
	for _, bs := range fuzzBlockSizes {
		rec := walRecord{LSN: 9, LBA: 2, Flags: flagWrite, Data: bytes.Repeat([]byte{0xA5}, bs)}
		buf, err := encodeRecord(rec, bs)
		if err != nil {
			f.Fatal(err)
		}
		sel := uint8(0)
		for i, v := range fuzzBlockSizes {
			if v == bs {
				sel = uint8(i)
			}
		}
		f.Add(buf, sel)
		torn := append([]byte(nil), buf...)
		torn[len(torn)-1] ^= 1
		f.Add(torn, sel)
	}
	f.Fuzz(func(t *testing.T, buf []byte, sel uint8) {
		bs := fuzzBlockSize(sel)
		rec, err := decodeRecord(buf, bs)
		if err != nil {
			if !reflect.DeepEqual(rec, walRecord{}) {
				t.Fatalf("error %v with a partial record %+v", err, rec)
			}
			return
		}
		re, err := encodeRecord(rec, bs)
		if err != nil {
			t.Fatalf("re-encode of an accepted record: %v", err)
		}
		if !bytes.Equal(re, buf) {
			t.Fatalf("accepted record does not re-encode to its bytes")
		}
	})
}

// FuzzParallelWALRecordRoundTrip: every record the encoder accepts decodes
// back to itself.
func FuzzParallelWALRecordRoundTrip(f *testing.F) {
	f.Add(uint64(9), uint32(2), uint16(flagWrite), []byte("payload!"), uint8(1))
	f.Fuzz(func(t *testing.T, lsn uint64, lba uint32, flags uint16, data []byte, sel uint8) {
		bs := fuzzBlockSize(sel)
		block := make([]byte, bs)
		copy(block, data)
		rec := walRecord{LSN: lsn, LBA: lba, Flags: flags, Data: block}
		buf, err := encodeRecord(rec, bs)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		got, err := decodeRecord(buf, bs)
		if err != nil || !reflect.DeepEqual(got, rec) {
			t.Fatalf("round trip: %+v err %v, want %+v", got, err, rec)
		}
	})
}

func seedHeader() fileHeader {
	h := fileHeader{Generation: 3, CreatedAt: 1, BlockSize: 4096, NumBlocks: 64, LaneCount: 2, StripeBlocks: 8,
		RecordSize: recordHeaderSize + 4096, SlotsPerLane: 32, DurableLSN: 10, CheckpointLSN: 4, WALTail: 2,
		RetainPerLane: 4, ActiveExtent: 1}
	h.LaneHeads[0], h.LaneTails[0] = 5, 1
	h.LaneHeads[1], h.LaneTails[1] = 6, 2
	return h
}

// headerFromFuzz builds a 4096-byte header slot from fuzz bytes with a valid
// magic, version, size and CRC, so the fuzzer exercises field validation.
func headerFromFuzz(data []byte) []byte {
	buf := make([]byte, headerSize)
	copy(buf, data)
	copy(buf[0:4], headerMagic)
	binary.LittleEndian.PutUint16(buf[4:6], headerVersion)
	binary.LittleEndian.PutUint16(buf[6:8], headerSize)
	binary.LittleEndian.PutUint32(buf[headerCRCOffset:], crc32.ChecksumIEEE(buf[:headerCRCOffset]))
	return buf
}

// FuzzParallelWALDecodeHeader: decodeHeader never panics on any bytes; on a
// CRC-valid slot a rejection is the zero header, and an accepted header is
// valid and survives an encode/decode round trip unchanged.
func FuzzParallelWALDecodeHeader(f *testing.F) {
	good, err := encodeHeader(seedHeader())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good[:], false)
	f.Add(good[:100], true)
	f.Add([]byte{}, true)
	f.Fuzz(func(t *testing.T, data []byte, fixup bool) {
		buf := data
		if fixup {
			buf = headerFromFuzz(data)
		}
		h, err := decodeHeader(buf)
		if err != nil {
			if !reflect.DeepEqual(h, fileHeader{}) {
				t.Fatalf("error %v with a partial header", err)
			}
			return
		}
		if err := h.validate(); err != nil {
			t.Fatalf("accepted header is invalid: %v", err)
		}
		re, err := encodeHeader(h)
		if err != nil {
			t.Fatalf("re-encode of an accepted header: %v", err)
		}
		got, err := decodeHeader(re[:])
		if err != nil || !reflect.DeepEqual(got, h) {
			t.Fatalf("header round trip: %+v err %v", got, err)
		}
	})
}
