package storage

import (
	"bytes"
	"testing"
)

func FuzzDecodeWALEntryV3(f *testing.F) {
	for _, e := range []walEntryV3{
		{LSN: 1, Type: walEntryWrite, LBA: 2, Length: 4096, Data: bytes.Repeat([]byte{0x41}, 4096)},
		{LSN: 1, Type: walEntryWriteBatch, LBA: 2, BatchCount: 2, Length: 8192, Data: bytes.Repeat([]byte{0x42}, 8192)},
		{LSN: 1, Type: walEntryTrim, LBA: 2, Length: 8192},
		{LSN: 1, Type: walEntryBarrier},
		{Type: walEntryPadding, Length: 32, Data: make([]byte, 32)},
	} {
		e.Nonce[0] = 1
		b, err := encodeWALEntryV3(e, 4096, 64)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
		f.Add(append(append([]byte(nil), b...), 0xA5))
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			t.Skip()
		}
		e, err := decodeWALEntryV3(b, 4096, 64)
		if err != nil {
			return
		}
		encoded, err := encodeWALEntryV3(e, 4096, 64)
		if err != nil || len(encoded) > len(b) || !bytes.Equal(encoded, b[:len(encoded)]) {
			t.Fatalf("accepted native record does not roundtrip: %v", err)
		}
	})
}
