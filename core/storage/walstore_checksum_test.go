package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// A fully checkpointed extent no longer has a retained WAL record to mask
// corruption. Neither the serving reader nor the offline verifier may return
// its wrong bytes as a successful read.
func TestWALStoreC12_ExtentBitFlipReturnsEIO(t *testing.T) {
	for _, readonly := range []bool{false, true} {
		name := "serving"
		if readonly {
			name = "read-only-verifier"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			s, err := CreateWALStore(path, 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			if err = s.flusher.Stop(); err != nil {
				t.Fatal(err)
			}
			want := makeBlock(4096, 0x7c)
			lsn, err := s.Write(3, want)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if s.CheckpointLSN() < lsn {
				t.Fatal("extent not checkpointed")
			}
			if _, _, _, _, dirty := s.dm.get(3); dirty {
				t.Fatal("fixture still reads WAL")
			}
			off := int64(s.extentBase + 3*4096 + 17)
			if _, err = s.fd.WriteAt([]byte{0x7d}, off); err != nil {
				t.Fatal(err)
			}
			if err = s.fd.Sync(); err != nil {
				t.Fatal(err)
			}
			var got []byte
			if readonly {
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				r, e := OpenReadOnly(path)
				if e != nil {
					t.Fatal(e)
				}
				defer r.Close()
				got, err = r.Read(3)
			} else {
				got, err = s.Read(3)
			}
			if !errors.Is(err, syscall.EIO) || len(got) != 0 {
				t.Fatalf("corrupted extent was not refused: err=%v returned=%d bytes", err, len(got))
			}
		})
	}
}

func createC12Legacy(t *testing.T, path string) {
	t.Helper()
	sb, err := newSuperblock(16*4096, createOptions{BlockSize: 4096, ExtentSize: 4096, ImplKind: ImplKindWALStore, ImplVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	size, err := walstoreFileSize(&sb)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if _, err = sb.writeTo(f); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
}
func TestWALStoreC12_V1ExplicitCompatibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy")
	createC12Legacy(t, path)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if s, err := OpenWALStoreWithChecksums(path); !errors.Is(err, ErrDataChecksumsRequired) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("required C12 accepted v1: %v", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("refusal modified/migrated v1")
	}
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.FormatVersion() != 1 || s.DataChecksums() {
		t.Fatal("v1 falsely reported protected")
	}
	if _, err = s.Write(2, makeBlock(4096, 0x4a)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(2)
	if err != nil || !bytes.Equal(got, makeBlock(4096, 0x4a)) {
		t.Fatalf("v1 no-C12 path broken: %v", err)
	}
}
func TestWALStoreC12_ZeroAndChecksumCorruption(t *testing.T) {
	for _, checksum := range []bool{false, true} {
		t.Run(fmt.Sprint(checksum), func(t *testing.T) {
			s, err := CreateWALStore(filepath.Join(t.TempDir(), "store"), 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.flusher.Stop()
			got, err := s.Read(4)
			if err != nil || !bytes.Equal(got, make([]byte, 4096)) {
				t.Fatalf("sparse zeros: %v", err)
			}
			off := s.extentBase + 4*4096
			if checksum {
				off = checksumBase(s.sb) + 4*4
			}
			if _, err = s.fd.WriteAt([]byte{1}, int64(off)); err != nil {
				t.Fatal(err)
			}
			got, err = s.Read(4)
			if !errors.Is(err, ErrDataChecksum) || !errors.Is(err, syscall.EIO) || len(got) != 0 {
				t.Fatalf("corruption accepted: %v bytes=%d", err, len(got))
			}
		})
	}
}
func TestWALStoreC12_TruncatedRegionRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if err = os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenWALStore(path); !errors.Is(err, io.ErrUnexpectedEOF) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("truncated CRC region opened: %v", err)
	}
}
func TestWALStoreC12_FailedChecksumWriteRetainsWAL(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.flusher.Stop()
	want := makeBlock(4096, 0x53)
	if _, err = s.Write(3, want); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	cp, tail := s.CheckpointLSN(), walLogicalTail(s)
	injected := errors.New("checksum write failed")
	s.testAfterExtentDataWrite = func() error { return injected }
	if err = s.flusher.flushOnce(); !errors.Is(err, injected) {
		t.Fatalf("flush did not refuse CRC gap: %v", err)
	}
	if s.CheckpointLSN() != cp || walLogicalTail(s) != tail {
		t.Fatal("CRC failure recycled WAL")
	}
	got, err := s.Read(3)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("retained WAL lost: %v", err)
	}
	s.testAfterExtentDataWrite = nil
	if err = s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if err = verifyExtentChecksum(s.fd, s.sb, 3, want); err != nil {
		t.Fatal(err)
	}
}
func TestWALStoreC12_BitmapAndCRCBeforeTail(t *testing.T) {
	s, _, _ := crashableStore(t)
	remote := newFakeChunkRemote()
	cfg := ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	mustAttach(t, s, remote, cfg)
	want := makeBlock(ctBlockSize, 0x63)
	if _, err := s.Write(12, want); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	cp, tail := s.CheckpointLSN(), walLogicalTail(s)
	bm.mu.Lock()
	bm.onSave = func(dirty []uint64) {
		if s.checkpointLSN != cp || s.wal.logicalTail != tail {
			t.Error("tail/checkpoint moved before bitmap save")
		}
		got := extentBytes(t, s, 12)
		if !bytes.Equal(got, want) {
			t.Error("bitmap saved before extent data")
		}
		if err := verifyExtentChecksum(s.fd, s.sb, 12, got); err != nil {
			t.Errorf("bitmap saved before checksum: %v", err)
		}
	}
	bm.failSave = errors.New("bitmap fsync failure")
	bm.mu.Unlock()
	if err := s.flusher.flushOnce(); err == nil {
		t.Fatal("bitmap failure allowed tail advance")
	}
	if s.CheckpointLSN() != cp || walLogicalTail(s) != tail {
		t.Fatal("bitmap failure recycled WAL")
	}
	bm.mu.Lock()
	bm.failSave = nil
	bm.mu.Unlock()
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() <= cp || walLogicalTail(s) <= tail || !bm.persisted(3) {
		t.Fatal("successful shared durability did not advance")
	}
}
