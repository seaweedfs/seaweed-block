package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWALStoreProductionSelectorCreatesMappedV3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	s, err := CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if !s.FormatV3() {
		t.Fatal("production Create must select mapped v3/impl3")
	}
	d := s.v3.pub.current()
	if d.ImplVersion != 3 || d.WMode != WModeLagged || d.MapRootHash == [32]byte{} {
		t.Fatalf("production descriptor: impl=%d mode=%d map=%x", d.ImplVersion, d.WMode, d.MapRootHash)
	}
	if _, err := s.Write(3, bytes.Repeat([]byte{0x73}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Recover(); err != nil {
		_ = r.Close()
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if data, err := ro.Read(3); err != nil || !bytes.Equal(data, bytes.Repeat([]byte{0x73}, 4096)) {
		t.Fatalf("production readonly after reopen: %v", err)
	}
}

func TestWALStoreProductionFallsBackWhenDamagedSlotOnlyLooksLegacy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(3, blk(0x73)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{2, 0}, 4) // corrupt A's discriminator, not a valid v2 header
	ce := f.Close()
	if err != nil || ce != nil {
		t.Fatal(err, ce)
	}
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("valid B was masked by a damaged v3 A: %v", err)
	}
	defer r.Close()
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Read(3); err != nil || !bytes.Equal(got, blk(0x73)) {
		t.Fatalf("fallback bytes: %v", err)
	}
}

func TestWALStoreProductionRefusesLegacyWithoutMutation(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy")
			opts := WALStoreOptions{}
			if version == 2 {
				opts = recordingOptions(&recordingMetadata{}, 2)
			}
			s, err := createLegacyWALStoreWithOptions(path, 16, 4096, opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if r, err := OpenWALStore(path); !errors.Is(err, ErrUnsupportedLegacyFormat) {
				if r != nil {
					_ = r.Close()
				}
				t.Fatalf("legacy writable open: %v", err)
			}
			if r, err := OpenReadOnly(path); !errors.Is(err, ErrUnsupportedLegacyFormat) {
				if r != nil {
					_ = r.Close()
				}
				t.Fatalf("legacy readonly open: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("legacy refusal changed bytes", err)
			}
		})
	}
}

func TestWALStoreProductionCreateOptions(t *testing.T) {
	s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), 64, 4096, WALStoreOptions{ChunkBlocks: 4, WMode: WModeStrict})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.v3.pub.current().WMode != WModeStrict || s.v3.ownedMap.chunkBlocks != 4 || len(s.v3.mapEntries) != 16 {
		t.Fatal("production create lost the strict mode or map geometry option")
	}
}

func TestWALStoreV3InstrumentationIncludesBatch(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if _, err := s.Write(0, blk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(2, [][]byte{blk(2), blk(3)}); err != nil {
		t.Fatal(err)
	}
	i := s.WriteInstrumentation()
	// A batch first aggregates its blocks, then copies that payload into
	// the encoded frame: both real copies belong in the cost profile.
	if i.WALCopyOps != 3 || i.WALCopyBytes != 5*4096 || i.WALEncodeOps != 2 || i.WALChecksumOps != 2 ||
		i.WALAppendOps != 2 || i.WALAppendWriteAtCalls != 2 || i.DirtyMapUpdateOps != 3 || s.DurabilityLatency().WALAppend.Count != 2 {
		t.Fatalf("v3 single+8KiB batch instrumentation incomplete: %+v latency=%+v", i, s.DurabilityLatency())
	}
}
