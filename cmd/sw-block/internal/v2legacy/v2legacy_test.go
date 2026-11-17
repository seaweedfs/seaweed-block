package v2legacy

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Plan row 13a controls (DESIGN r17.4 section 10.1 precondition 3; owner
// claude03): the frozen v2 decoder and the exhaustive, refusal-biased scan.

// encode is a test-only encoder of the frozen v2 framing.
func encode(lsn, reserved uint64, typ uint8, lba uint64, length uint32, data []byte) []byte {
	b := make([]byte, entryPrefix+len(data)+8)
	le := binary.LittleEndian
	le.PutUint64(b[0:], lsn)
	le.PutUint64(b[8:], reserved)
	b[16] = typ
	le.PutUint64(b[18:], lba)
	le.PutUint32(b[26:], length)
	copy(b[entryPrefix:], data)
	end := entryPrefix + len(data)
	le.PutUint32(b[end:], crc32.ChecksumIEEE(b[:end]))
	le.PutUint32(b[end+4:], uint32(entryHeader+len(data)))
	return b
}

func region(n int, at int, recs ...[]byte) []byte {
	w := make([]byte, n)
	for _, r := range recs {
		for i := range r {
			w[(at+i)%n] = r[i]
		}
		at += len(r)
	}
	return w
}

func write(lsn uint64) []byte { return encode(lsn, 0, typeWrite, 1, 64, make([]byte, 64)) }

func TestScanCleanFindsEveryUnflushedV2RecordShape(t *testing.T) {
	cases := []struct {
		name       string
		wal        []byte
		checkpoint uint64
		refuse     bool
	}{
		{"an empty region", make([]byte, 4096), 0, false},
		{"a write above the checkpoint", region(4096, 100, write(5)), 4, true},
		{"a write at the checkpoint is in the extent", region(4096, 100, write(5)), 5, false},
		{"a record wrapping across the region end", region(4096, 4096-50, write(9)), 0, true},
		{"a trim whose affected range exceeds the region (footprint 38)", region(4096, 0, encode(7, 0, typeTrim, 0, 1<<30, nil)), 6, true},
		{"a barrier above the checkpoint", region(4096, 0, encode(7, 0, typeBarrier, 0, 0, nil)), 6, true},
		{"a batch counts by its last LSN", region(4096, 0, encode(3, 3, typeBatch, 0, 64, make([]byte, 64))), 4, true},
		{"a batch fully below the checkpoint", region(4096, 0, encode(3, 3, typeBatch, 0, 64, make([]byte, 64))), 5, false},
		{"a CRC-valid batch with a zero count is unclassifiable", region(4096, 0, encode(3, 0, typeBatch, 0, 64, make([]byte, 64))), 9, true},
		{"a record embedded in another record's payload", region(4096, 0, encode(2, 0, typeWrite, 1, uint32(len(write(9))), write(9))), 5, true},
		{"a corrupt CRC is no record", func() []byte { w := region(4096, 0, write(9)); w[40] ^= 0xFF; return w }(), 0, false},
		{"a record larger than the region cannot be one", encode(9, 0, typeWrite, 1, 100, make([]byte, 100))[:64], 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ScanClean(c.wal, c.checkpoint)
			if (len(got) > 0) != c.refuse {
				t.Fatalf("findings %+v, want refuse = %v", got, c.refuse)
			}
		})
	}
}

func TestReadSuperblockRefusesOtherFormats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v2")
	s, err := storage.CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := ReadSuperblock(raw[:SuperblockSize])
	if err != nil || sb.WALOffset != SuperblockSize || sb.BlockSize != 4096 {
		t.Fatalf("real v2 superblock: %+v, %v", sb, err)
	}
	impl2 := append([]byte(nil), raw[:SuperblockSize]...)
	binary.LittleEndian.PutUint32(impl2[92:], 2)
	if _, err := ReadSuperblock(impl2); !errors.Is(err, ErrImplUnsupported) {
		t.Fatalf("impl version 2: err = %v, want ErrImplUnsupported", err)
	}
	v3 := append([]byte(nil), raw[:SuperblockSize]...)
	binary.LittleEndian.PutUint16(v3[4:], 3)
	if _, err := ReadSuperblock(v3); !errors.Is(err, ErrNotV2) {
		t.Fatalf("version 3: err = %v, want ErrNotV2", err)
	}
}

// Real v2 stores: a cleanly closed, flushed store scans clean; a store with
// synced records that were never flushed (a crash) refuses, even though
// its superblock may say head = tail (the #156 shape: bounds not persisted
// by an ordinary Sync).
func TestScanCleanOnRealV2Stores(t *testing.T) {
	scan := func(t *testing.T, path string) []Finding {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sb, err := ReadSuperblock(raw[:SuperblockSize])
		if err != nil {
			t.Fatal(err)
		}
		return ScanClean(raw[SuperblockSize:SuperblockSize+sb.WALSize], sb.WALCheckpointLSN)
	}
	blk := make([]byte, 4096)
	t.Run("cleanly closed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "clean")
		s, err := storage.CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for lba := uint32(0); lba < 4; lba++ {
			if _, err := s.Write(lba, blk); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if f := scan(t, path); len(f) != 0 {
			t.Fatalf("a cleanly closed v2 store refused: %+v", f[0])
		}
	})
	t.Run("synced, never flushed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dirty")
		s, err := storage.CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		for lba := uint32(0); lba < 2; lba++ {
			if _, err := s.Write(lba, blk); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if f := scan(t, path); len(f) == 0 {
			t.Fatal("synced unflushed v2 records scanned clean")
		}
		_ = s.Close()
	})
}
