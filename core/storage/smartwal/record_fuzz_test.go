package smartwal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// FuzzSmartWALDecode: decode never panics; a rejected slot yields the zero
// record; an accepted slot re-encodes to the same record and to the same
// bytes outside the reserved fields.
func FuzzSmartWALDecode(f *testing.F) {
	good := encode(record{LSN: 7, LBA: 3, Flags: flagWrite, DataCRC32: 0xDEADBEEF})
	f.Add(good[:])
	f.Add(make([]byte, recordSize))
	f.Add(good[:recordSize-1])
	torn := good
	torn[9] ^= 1
	f.Add(torn[:])
	f.Fuzz(func(t *testing.T, buf []byte) {
		r, ok := decode(buf)
		if !ok {
			if r != (record{}) {
				t.Fatalf("rejected slot with a partial record %+v", r)
			}
			return
		}
		re := encode(r)
		if got, ok := decode(re[:]); !ok || got != r {
			t.Fatalf("re-encode of %+v decodes to %+v (ok %t)", r, got, ok)
		}
		if gap := knownSmartWALDecodeGap(buf); gap != "" {
			return // pinned below
		}
		if !bytes.Equal(re[:], buf[:recordSize]) {
			t.Fatalf("accepted slot does not re-encode to its bytes: %x vs %x", re, buf[:recordSize])
		}
	})
}

// FuzzSmartWALRecordFields: slots with a valid magic and record CRC but
// arbitrary fields, including the reserved bytes.
func FuzzSmartWALRecordFields(f *testing.F) {
	f.Add(uint8(flagWrite), uint32(3), uint64(7), uint32(0xDEADBEEF), uint16(0), uint64(0))
	f.Add(uint8(flagTrim), uint32(0), uint64(1), uint32(0), uint16(1), uint64(9))
	f.Fuzz(func(t *testing.T, flags uint8, lba uint32, lsn uint64, dataCRC uint32, res1 uint16, res2 uint64) {
		var buf [recordSize]byte
		buf[0] = recordMagic
		buf[1] = flags
		binary.LittleEndian.PutUint16(buf[2:4], res1)
		binary.LittleEndian.PutUint32(buf[4:8], lba)
		binary.LittleEndian.PutUint64(buf[8:16], lsn)
		binary.LittleEndian.PutUint64(buf[16:24], res2)
		binary.LittleEndian.PutUint32(buf[24:28], dataCRC)
		binary.LittleEndian.PutUint32(buf[28:32], crc32.ChecksumIEEE(buf[:28]))
		r, ok := decode(buf[:])
		if !ok {
			t.Fatalf("a slot with a valid magic and CRC was rejected")
		}
		if r != (record{LSN: lsn, LBA: lba, Flags: flags, DataCRC32: dataCRC}) {
			t.Fatalf("decoded %+v", r)
		}
		if gap := knownSmartWALDecodeGap(buf[:]); gap == "" {
			re := encode(r)
			if re != buf {
				t.Fatalf("canonical slot does not re-encode to its bytes")
			}
		}
	})
}

// FuzzSmartWALRoundTrip: every record encodes and decodes back to itself.
func FuzzSmartWALRoundTrip(f *testing.F) {
	f.Add(uint64(7), uint32(3), uint8(flagWrite), uint32(0xDEADBEEF))
	f.Fuzz(func(t *testing.T, lsn uint64, lba uint32, flags uint8, dataCRC uint32) {
		r := record{LSN: lsn, LBA: lba, Flags: flags, DataCRC32: dataCRC}
		buf := encode(r)
		got, ok := decode(buf[:])
		if !ok || got != r {
			t.Fatalf("round trip of %+v: %+v (ok %t)", r, got, ok)
		}
	})
}

// knownSmartWALDecodeGap names accepted slots that are not canonical:
// non-zero reserved bytes (2:4, 16:24) are covered by the record CRC but
// ignored, so such a slot is accepted although encode never writes it.
// Found by FuzzSmartWALRecordFields; reported, not changed here. Any flag
// value is accepted too (decode does not check flags).
func knownSmartWALDecodeGap(buf []byte) string {
	if binary.LittleEndian.Uint16(buf[2:4]) != 0 || binary.LittleEndian.Uint64(buf[16:24]) != 0 {
		return "non-zero reserved bytes"
	}
	return ""
}
