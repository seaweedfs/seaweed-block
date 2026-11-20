//go:build swblock_format_v3_test

package v2legacy

import (
	"encoding/binary"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"os"
	"path/filepath"
	"testing"
)

// TODO(format-v3 row 14a): retain raw-codec controls in the default suite;
// these real v2-engine fixtures remain explicit in the tagged migration suite.
func TestReadSuperblockRefusesOtherFormats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v2")
	s, err := storage.CreateLegacyWALStoreTest(path, 16, 4096)
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
		s, err := storage.CreateLegacyWALStoreTest(path, 16, 4096)
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
		s, err := storage.CreateLegacyWALStoreTest(path, 16, 4096)
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
