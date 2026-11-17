package storage

import (
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"os"
	"testing"
)

// Fixture custody only. Until code GO there is no product v3 codec call here.
func TestFormatV3_BarrierFixtureCustody(t *testing.T) {
	b, err := os.ReadFile("testdata/format_v3/barrier.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 70 {
		t.Fatalf("fixture size=%d want 70", len(b))
	}
	if binary.LittleEndian.Uint64(b[0:8]) != 7 || binary.LittleEndian.Uint64(b[8:16]) != 0 || b[16] != 3 || b[17] != 0 || binary.LittleEndian.Uint64(b[18:26]) != 0 || binary.LittleEndian.Uint32(b[26:30]) != 0 {
		t.Fatal("barrier type/count/geometry fixture differs from worked example")
	}
	if binary.LittleEndian.Uint64(b[30:38]) != 6 || binary.LittleEndian.Uint64(b[38:46]) != 128 || hex.EncodeToString(b[46:62]) != "000102030405060708090a0b0c0d0e0f" {
		t.Fatal("barrier durability/cursor/nonce fixture differs from worked example")
	}
	if binary.LittleEndian.Uint32(b[66:70]) != 70 {
		t.Fatal("barrier footer differs from fixed overhead")
	}
	if got, want := crc32.Checksum(b[:62], crc32.MakeTable(crc32.Castagnoli)), binary.LittleEndian.Uint32(b[62:66]); got != want {
		t.Fatalf("fixture CRC32C=%08x stored=%08x", got, want)
	}
}
