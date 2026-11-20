//go:build linux

package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// External dm-log-writes harness calls prepare/write, replays only through
// the post-sync ACK mark onto a fresh device, then calls verify. Without that
// block-layer harness this helper is not a power-loss test.
func TestDataSyncPowerLossReplayHelper(t *testing.T) {
	root := os.Getenv("SWBLOCK_REPLAY_ROOT")
	if root == "" {
		t.Skip("requires isolated dm-log-writes replay harness")
	}
	phase, kind := os.Getenv("SWBLOCK_REPLAY_PHASE"), os.Getenv("SWBLOCK_REPLAY_CASE")
	path := filepath.Join(root, "probe")
	payload := bytes.Repeat([]byte{0x7d}, 4096)
	if kind == "wal" {
		switch phase {
		case "prepare":
			s, e := CreateWALStore(path, 32, 4096)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
		case "write":
			s, e := OpenWALStore(path)
			if e != nil {
				t.Fatal(e)
			}
			s.DisableAutoFlushForRecoveryTest()
			if _, e = s.Recover(); e != nil {
				t.Fatal(e)
			}
			if _, e = s.Write(13, payload); e != nil {
				t.Fatal(e)
			}
			if os.Getenv("SWBLOCK_REPLAY_DROP_SYNC") != "1" {
				if _, e = s.Sync(); e != nil {
					t.Fatal(e)
				}
			}
			// Deliberately no Close: only the explicit Sync precedes the ACK mark.
		case "verify":
			s, e := OpenWALStore(path)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			if _, e = s.Recover(); e != nil {
				t.Fatal(e)
			}
			b, e := s.Read(13)
			if e != nil || !bytes.Equal(b, payload) {
				t.Fatalf("REPLAY_BYTES_MISMATCH WAL: %v", e)
			}
		default:
			t.Fatalf("unknown replay phase %q", phase)
		}
	} else {
		switch phase {
		case "prepare":
			f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			switch kind {
			case "written":
				if _, e = f.WriteAt(make([]byte, 1<<20), 0); e != nil {
					t.Fatal(e)
				}
			case "unwritten":
				if e = fallocateFile(f, 1<<20); e != nil {
					t.Fatal(e)
				}
			case "hole":
				if e = f.Truncate(1 << 20); e != nil {
					t.Fatal(e)
				}
			case "growth":
				if _, e = f.WriteAt(make([]byte, 4096), 0); e != nil {
					t.Fatal(e)
				}
			default:
				t.Fatalf("unknown replay case %q", kind)
			}
			if e = f.Sync(); e != nil {
				t.Fatal(e)
			}
		case "write":
			f, e := os.OpenFile(path, os.O_RDWR, 0)
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			if _, e = f.WriteAt(payload, 128<<10); e != nil {
				t.Fatal(e)
			}
			if os.Getenv("SWBLOCK_REPLAY_DROP_SYNC") != "1" {
				if kind == "growth" {
					e = f.Sync()
				} else {
					var sync func() error
					sync, e = newDataFileSync(f)
					if e == nil {
						e = sync()
					}
				}
				if e != nil {
					t.Fatal(e)
				}
			}
		case "verify":
			f, e := os.Open(path)
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			b := make([]byte, 4096)
			n, e := f.ReadAt(b, 128<<10)
			if e != nil || n != len(b) || !bytes.Equal(b, payload) {
				t.Fatalf("REPLAY_BYTES_MISMATCH %s: n=%d err=%v", kind, n, e)
			}
		default:
			t.Fatalf("unknown replay phase %q", phase)
		}
	}
	if phase == "prepare" {
		d, e := os.Open(root)
		if e != nil {
			t.Fatal(e)
		}
		defer d.Close()
		if e = d.Sync(); e != nil {
			t.Fatal(e)
		}
	}
	fmt.Printf("REPLAY_%s_OK %s\n", phase, kind)
}
