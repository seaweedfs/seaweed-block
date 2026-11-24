package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

// A guest that never flushes: below the WAL pressure threshold its writes
// are not synced, so the flusher writes none of their extents
// (INV-WAL-V3-WRITE-AHEAD-001) and the checkpoint stays; once the WAL is
// past the threshold, the flusher's pressure sync makes them durable and the
// same cycle writes every block's newest bytes to the extent and advances
// the checkpoint. The data never waits for a guest flush that does not come.
func TestFlusherWriteAheadNeverFlushingGuestReachesExtentAtPressure(t *testing.T) {
	for _, mode := range []WMode{WModeStrict, WModeLagged} {
		t.Run(map[WMode]string{WModeStrict: "A", WModeLagged: "C"}[mode], func(t *testing.T) {
			s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "pressure"), 64, 4096, 64*1024, mode, 4)
			if err != nil {
				t.Fatal(err)
			}
			s.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = s.Close() })
			expected := map[uint32][]byte{}
			write := func(lba uint32) {
				t.Helper()
				data := bytes.Repeat([]byte{byte(s.NextLSN())}, 4096)
				if _, err := s.Write(lba, data); err != nil {
					t.Fatal(err)
				}
				expected[lba] = data
			}
			extent := func(lba uint32) []byte {
				t.Helper()
				b, err := s.readFromExtent(lba)
				if err != nil {
					t.Fatal(err)
				}
				return b
			}

			write(0)
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(extent(0), expected[0]) || s.CheckpointLSN() != 0 {
				t.Fatalf("below pressure: an unsynced write reached the extent (checkpoint %d)", s.CheckpointLSN())
			}

			for lba := uint32(1); s.walUsedFraction() < 0.72; lba = (lba + 1) % 64 {
				write(lba)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			for lba, want := range expected {
				if !bytes.Equal(extent(lba), want) {
					t.Fatalf("after the pressure sync: block %d's extent does not hold its newest write", lba)
				}
			}
			if s.CheckpointLSN() == 0 {
				t.Fatal("after the pressure sync: the checkpoint did not advance")
			}
		})
	}
}
