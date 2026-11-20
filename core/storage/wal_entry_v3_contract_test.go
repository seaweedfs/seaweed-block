package storage

// Row 2 NEW-API contract coverage, prepared before the codec exists.
// Missing walEntryV3/encodeWALEntryV3/decodeWALEntryV3 is NOT a behavioral
// RED. These names are internal codec seams owned by row 2. Slot nonce and
// geometry are passed as values; this test does not invent a slot codec.

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"reflect"
	"testing"
)

const v3TestBlockSize uint32 = 4096
const v3TestNumBlocks uint64 = 1024

func v3TestBarrier() walEntryV3 {
	var nonce [16]byte
	for i := range nonce {
		nonce[i] = byte(i)
	}
	return walEntryV3{LSN: 7, Type: walEntryBarrier, DurableLSN: 6, LogicalStart: 128, Nonce: nonce}
}

func TestWALEntryV3_CanonicalBarrierBytes(t *testing.T) {
	want, err := os.ReadFile("testdata/format_v3/barrier.bin")
	if err != nil {
		t.Fatal(err)
	}
	entry := v3TestBarrier()
	got, err := encodeWALEntryV3(entry, v3TestBlockSize, v3TestNumBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded barrier differs from independent signed-layout vector: %x", got)
	}
	decoded, err := decodeWALEntryV3(want, v3TestBlockSize, v3TestNumBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, entry) {
		t.Fatalf("decoded barrier=%+v want %+v", decoded, entry)
	}
}

func TestWALEntryV3_TypeFootprints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   uint8
		count  uint64
		length uint32
		data   []byte
		size   int
	}{
		{"single block", walEntryWrite, 0, 4096, bytes.Repeat([]byte{0x91}, 4096), 4166},
		{"batch", walEntryWriteBatch, 2, 8192, bytes.Repeat([]byte{0x72}, 8192), 8262},
		{"large trim no payload", walEntryTrim, 0, 65536, nil, 70},
		{"barrier", walEntryBarrier, 0, 0, nil, 70},
		{"padding", walEntryPadding, 0, 12, make([]byte, 12), 82},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := v3TestBarrier()
			e.Type = tc.kind
			e.BatchCount = tc.count
			e.Length = tc.length
			e.Data = tc.data
			if tc.kind == walEntryPadding {
				e.LSN = 0
				e.DurableLSN = 0
			}
			b, err := encodeWALEntryV3(e, v3TestBlockSize, v3TestNumBlocks)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) != tc.size {
				t.Fatalf("footprint=%d want %d", len(b), tc.size)
			}
			if binary.LittleEndian.Uint32(b[len(b)-4:]) != uint32(tc.size) {
				t.Fatal("incorrect EntrySize footer")
			}
			// Return every logical field, not merely an equal CRC or payload.
			got, err := decodeWALEntryV3(b, v3TestBlockSize, v3TestNumBlocks)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, e) {
				t.Fatalf("decoded=%+v want %+v", got, e)
			}
		})
	}
}

func TestWALEntryV3_RefusesInvalidEncode(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*walEntryV3)
	}{
		{"unknown type", func(e *walEntryV3) { e.Type = 0x55 }},
		{"flags", func(e *walEntryV3) { e.Flags = 1 }},
		{"zero nonce", func(e *walEntryV3) { e.Nonce = [16]byte{} }},
		{"barrier payload", func(e *walEntryV3) { e.Data = []byte{1}; e.Length = 1 }},
		{"nonbatch count", func(e *walEntryV3) { e.BatchCount = 1 }},
		{"write short", func(e *walEntryV3) { e.Type = walEntryWrite; e.Length = 1; e.Data = []byte{1} }},
		{"write long", func(e *walEntryV3) { e.Type = walEntryWrite; e.Length = 8192; e.Data = make([]byte, 8192) }},
		{"batch zero count", func(e *walEntryV3) { e.Type = walEntryWriteBatch; e.Length = 4096; e.Data = make([]byte, 4096) }},
		{"batch length mismatch", func(e *walEntryV3) {
			e.Type = walEntryWriteBatch
			e.BatchCount = 2
			e.Length = 4096
			e.Data = make([]byte, 4096)
		}},
		{"batch count overflow", func(e *walEntryV3) {
			e.Type = walEntryWriteBatch
			e.BatchCount = ^uint64(0)
			e.Length = 4096
			e.Data = make([]byte, 4096)
		}},
		{"batch LSN overflow", func(e *walEntryV3) {
			e.Type = walEntryWriteBatch
			e.LSN = ^uint64(0) - 1
			e.BatchCount = 2
			e.Length = 8192
			e.Data = make([]byte, 8192)
		}},
		{"batch LBA span", func(e *walEntryV3) {
			e.Type = walEntryWriteBatch
			e.LBA = v3TestNumBlocks - 1
			e.BatchCount = 2
			e.Length = 8192
			e.Data = make([]byte, 8192)
		}},
		{"trim outside volume", func(e *walEntryV3) { e.Type = walEntryTrim; e.LBA = v3TestNumBlocks - 1; e.Length = 8192 }},
		{"trim payload", func(e *walEntryV3) { e.Type = walEntryTrim; e.Length = 4096; e.Data = []byte{1} }},
		{"padding data nonzero", func(e *walEntryV3) {
			e.Type = walEntryPadding
			e.LSN = 0
			e.DurableLSN = 0
			e.Length = 1
			e.Data = []byte{1}
		}},
		{"padding LSN", func(e *walEntryV3) { e.Type = walEntryPadding; e.DurableLSN = 0 }},
		{"padding DurableLSN", func(e *walEntryV3) { e.Type = walEntryPadding; e.LSN = 0 }},
		{"padding LBA", func(e *walEntryV3) { e.Type = walEntryPadding; e.LSN = 0; e.DurableLSN = 0; e.LBA = 1 }},
		{"zero normal LSN", func(e *walEntryV3) { e.LSN = 0 }},
		{"LSN exhausted", func(e *walEntryV3) { e.LSN = ^uint64(0) }},
		{"durable exhausted", func(e *walEntryV3) { e.DurableLSN = ^uint64(0) }},
		{"cursor size overflow", func(e *walEntryV3) { e.LogicalStart = ^uint64(0) - 30 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := v3TestBarrier()
			tc.edit(&e)
			if _, err := encodeWALEntryV3(e, v3TestBlockSize, v3TestNumBlocks); err == nil {
				t.Fatal("invalid record admitted")
			}
		})
	}
}

func TestWALEntryV3_RefusesMalformedWire(t *testing.T) {
	original, err := os.ReadFile("testdata/format_v3/barrier.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		edit      func([]byte)
		repairCRC bool
	}{
		{"CRC", func(b []byte) { b[62] ^= 1 }, false},
		{"footer", func(b []byte) { b[66] ^= 1 }, false},
		{"unknown type valid CRC", func(b []byte) { b[16] = 0x55 }, true},
		{"flags valid CRC", func(b []byte) { b[17] = 1 }, true},
		{"count valid CRC", func(b []byte) { b[8] = 1 }, true},
		{"zero nonce valid CRC", func(b []byte) { clear(b[46:62]) }, true},
		{"huge length valid CRC", func(b []byte) { binary.LittleEndian.PutUint32(b[26:30], ^uint32(0)) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := bytes.Clone(original)
			tc.edit(b)
			if tc.repairCRC {
				binary.LittleEndian.PutUint32(b[62:66], crc32.Checksum(b[:62], crc32.MakeTable(crc32.Castagnoli)))
			}
			if _, err := decodeWALEntryV3(b, v3TestBlockSize, v3TestNumBlocks); err == nil {
				t.Fatal("malformed wire record admitted")
			}
		})
	}
	for n := 0; n < len(original); n++ {
		if _, err := decodeWALEntryV3(original[:n], v3TestBlockSize, v3TestNumBlocks); err == nil {
			t.Fatalf("truncated size%d admitted", n)
		}
	}
}

// Native fuzzing runs later against the actual decoder. Seed execution belongs
// to the row-2 package tests; allocation bounds must precede copying payload.
func FuzzWALEntryV3_DecodeNeverPanics(f *testing.F) {
	b, err := os.ReadFile("testdata/format_v3/barrier.bin")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 70))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 16384 {
			t.Skip()
		}
		_, _ = decodeWALEntryV3(b, v3TestBlockSize, v3TestNumBlocks)
	})
}

// Nonzero count/LBA/Length vectors keep a mutually-wrong encoder and decoder
// from passing solely by roundtrip. Expected bytes use the independent fixture.
func TestWALEntryV3_CanonicalBatchAndTrimBytes(t *testing.T) {
	for _, name := range []string{"batch", "trim"} {
		t.Run(name, func(t *testing.T) {
			e := v3TestBarrier()
			if name == "batch" {
				e.Type = walEntryWriteBatch
				e.BatchCount = 2
				e.LBA = 3
				e.Length = 8192
				e.Data = bytes.Repeat([]byte{0x72}, 8192)
			} else {
				e.Type = walEntryTrim
				e.LBA = 5
				e.Length = 65536
			}
			want, err := os.ReadFile("testdata/format_v3/" + name + ".bin")
			if err != nil {
				t.Fatal(err)
			}
			got, err := encodeWALEntryV3(e, v3TestBlockSize, v3TestNumBlocks)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("record differs from independent count/LBA/length vector")
			}
			decoded, err := decodeWALEntryV3(want, v3TestBlockSize, v3TestNumBlocks)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, e) {
				t.Fatalf("decoded vector differs: %+v", decoded)
			}
		})
	}
}
