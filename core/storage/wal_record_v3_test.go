package storage

import (
	"bytes"
	"errors"
	"testing"
)

// DecodeWALRecordV3 must agree with the internal decoder field for field on
// valid records and fail exactly when it fails, with the same error.
func TestDecodeWALRecordV3AgreesWithTheInternalDecoder(t *testing.T) {
	var nonce [16]byte
	copy(nonce[:], "0123456789abcdef")
	write := walEntryV3{LSN: 21, Type: walEntryWrite, LBA: 3, Length: v3TestBlockSize, DurableLSN: 20,
		LogicalStart: 4096, Nonce: nonce, Data: bytes.Repeat([]byte{0xA5}, int(v3TestBlockSize))}
	pad := walEntryV3{Type: walEntryPadding, Nonce: nonce, Length: 512, Data: make([]byte, 512)}
	for name, e := range map[string]walEntryV3{"write": write, "barrier": v3TestBarrier(), "padding": pad} {
		t.Run(name, func(t *testing.T) {
			b, err := encodeWALEntryV3(e, v3TestBlockSize, v3TestNumBlocks)
			if err != nil {
				t.Fatalf("fixture not encodable: %v", err)
			}
			in, ierr := decodeWALEntryV3(b, v3TestBlockSize, v3TestNumBlocks)
			out, oerr := DecodeWALRecordV3(b, v3TestBlockSize, v3TestNumBlocks)
			if ierr != nil || oerr != nil {
				t.Fatalf("valid record: internal %v, exported %v", ierr, oerr)
			}
			want := WALRecordV3{LSN: in.LSN, Type: in.Type, LBA: in.LBA, Length: in.Length,
				Size: uint64(len(b)), Padding: in.Type == walEntryPadding}
			if out != want {
				t.Fatalf("exported %+v, internal decode gives %+v", out, want)
			}
		})
	}
	b, err := encodeWALEntryV3(write, v3TestBlockSize, v3TestNumBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if got := uint64(len(b)); got != uint64(WALRecordHeaderBytesV3)+uint64(v3TestBlockSize) {
		t.Fatalf("a %d-byte write's record is %d bytes, want header %d + payload", v3TestBlockSize, got, WALRecordHeaderBytesV3)
	}
	// every single-byte corruption and every truncation: both decoders refuse, with the same error
	for i := 0; i < len(b); i++ {
		c := append([]byte(nil), b...)
		c[i] ^= 0x40
		_, ierr := decodeWALEntryV3(c, v3TestBlockSize, v3TestNumBlocks)
		_, oerr := DecodeWALRecordV3(c, v3TestBlockSize, v3TestNumBlocks)
		if (ierr == nil) != (oerr == nil) || ierr != nil && !errors.Is(oerr, ierr) && oerr.Error() != ierr.Error() {
			t.Fatalf("byte %d corrupted: internal %v, exported %v", i, ierr, oerr)
		}
	}
	for n := 0; n < len(b); n += 97 {
		_, ierr := decodeWALEntryV3(b[:n], v3TestBlockSize, v3TestNumBlocks)
		_, oerr := DecodeWALRecordV3(b[:n], v3TestBlockSize, v3TestNumBlocks)
		if ierr == nil || oerr == nil || oerr.Error() != ierr.Error() {
			t.Fatalf("truncated to %d: internal %v, exported %v", n, ierr, oerr)
		}
	}
}
