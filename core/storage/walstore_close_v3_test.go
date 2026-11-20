package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// codex01 row 7 slice 4 P1: WALStore.Close serialized the legacy superblock
// and pwrote it at offset 0, which on a v3 file is descriptor slot A. The
// schedule: strict create (A gen 1), Recover (B gen 2), Write+Sync (A gen 3,
// W 1), Close (slot A destroyed), reopen (B gen 2, W 0). A v3 Close writes
// nothing; reopen must select the newest slot A and its W.
func TestWALStoreV3CloseKeepsTheNewestSlotAAndItsW(t *testing.T) {
	for name, mode := range map[string]WMode{"strict": WModeStrict, "lagged": WModeLagged} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "close.v3")
			s, err := createMappedWALStoreV3ForTest(path, 64, v3TestBlock, v3TestWAL, mode, 4)
			if err != nil {
				t.Fatal(err)
			}
			s.DisableAutoFlushForRecoveryTest()
			writeSynced(t, s, 1, 1)
			if mode == WModeLagged {
				if err := s.refreshW(); err != nil {
					t.Fatal(err)
				}
			}
			if s.v3.pub.current().SlotGen%2 == 0 {
				// Land the newest publication in slot A, the slot Close hit.
				if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			before := s.v3.pub.current()
			if before.W != 1 || before.SlotGen%2 != 1 {
				t.Fatalf("setup: newest slot gen %d W %d, want an odd (slot A) generation with W 1", before.SlotGen, before.W)
			}
			rawA := readSlotRawV3(t, path, slotAOffsetV3)
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := readSlotRawV3(t, path, slotAOffsetV3); !bytes.Equal(got, rawA) {
				t.Fatal("Close rewrote descriptor slot A")
			}
			r, err := openWALStoreV3ForTest(path)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { r.Close() })
			if got := r.v3.pub.current(); got.SlotGen != before.SlotGen || got.W != 1 {
				t.Fatalf("reopen selected gen %d W %d, want gen %d W 1", got.SlotGen, got.W, before.SlotGen)
			}
			if _, err := r.Recover(); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			readAllV3(t, r, 1, 1)
		})
	}
}

// The other legacy offset-0 writers (the checkpoint/recovery superblock
// writer and the direct-frontier writer behind SeedChunkFrontier and Sync's
// direct frontier) refuse on a v3 store and leave slot A untouched.
func TestWALStoreV3LegacySuperblockWritersRefuse(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 1)
	rawA := readSlotRawV3(t, path, slotAOffsetV3)
	junk := bytes.Repeat([]byte{0xEE}, 4096)
	if err := s.writeSuperblockMetadata(junk); !errors.Is(err, errV3LegacySuperblockWrite) {
		t.Fatalf("writeSuperblockMetadata: err = %v, want errV3LegacySuperblockWrite", err)
	}
	if err := s.syncDirectFrontierMetadata(junk); !errors.Is(err, errV3LegacySuperblockWrite) {
		t.Fatalf("syncDirectFrontierMetadata: err = %v, want errV3LegacySuperblockWrite", err)
	}
	if got := readSlotRawV3(t, path, slotAOffsetV3); !bytes.Equal(got, rawA) {
		t.Fatal("a refused legacy writer changed descriptor slot A")
	}
}

func readSlotRawV3(t *testing.T, path string, off int64) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, slotSizeV3)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	return b
}
