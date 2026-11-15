package storage

import (
	"errors"
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
