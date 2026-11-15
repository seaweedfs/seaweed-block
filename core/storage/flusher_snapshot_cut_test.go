package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

// Force a Write+Sync between scans of shard 0 and shard 5. The older
// snapshot entry is also superseded, so it cannot cover the omitted write.
func TestFlusherSnapshotCutPreservesSyncedWrites(t *testing.T) {
	for _, finishFlush := range []bool{false, true} {
		name := "crash-after-first-cycle"
		if finishFlush {
			name = "next-cycle-makes-progress"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.bin")
			s, err := CreateWALStore(path, 128, 4096)
			if err != nil {
				t.Fatal(err)
			}
			s.DisableAutoFlushForRecoveryTest()
			crashed := false
			t.Cleanup(func() {
				if !crashed {
					_ = s.Close()
				}
			})
			if _, err := s.Write(0, bytes.Repeat([]byte{0x11}, 4096)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			a := bytes.Repeat([]byte{0xAA}, 4096)
			b := bytes.Repeat([]byte{0xBB}, 4096)
			fired := false
			var low, high uint64
			s.dm.afterSnapshotShard = func(shard int) {
				if shard != 0 || fired {
					return
				}
				fired = true
				var err error
				low, err = s.Write(0, a) // Already scanned: omitted lower LSN.
				if err != nil {
					t.Fatal(err)
				}
				high, err = s.Write(5, b) // Not yet scanned: visible higher LSN.
				if err != nil {
					t.Fatal(err)
				}
				acked, err := s.Sync()
				if err != nil || acked < high {
					t.Fatalf("Sync frontier=%d high=%d: %v", acked, high, err)
				}
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			s.dm.afterSnapshotShard = nil
			if !fired || low != 2 || high != 3 {
				t.Fatalf("interleaving not reached: fired=%v LSNs=%d/%d", fired, low, high)
			}
			t.Logf("first-cycle checkpoint=%d omitted-synced-LSN=%d tail=%d", s.CheckpointLSN(), low, s.wal.logicalTailValue())
			if finishFlush {
				if err := s.flusher.flushOnce(); err != nil {
					t.Fatal(err)
				}
				if s.CheckpointLSN() != high || s.dm.len() != 0 {
					t.Fatalf("next flush did not drain: checkpoint=%d dirty=%d", s.CheckpointLSN(), s.dm.len())
				}
			}
			// Close only the committer and descriptor: normal Close would flush.
			s.committer.Stop()
			if err := s.fd.Close(); err != nil {
				t.Fatal(err)
			}
			crashed = true
			reopened, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if _, err := reopened.Recover(); err != nil {
				t.Fatal(err)
			}
			for lba, want := range map[uint32][]byte{0: a, 5: b} {
				got, err := reopened.Read(lba)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Errorf("Sync-acked LBA %d lost after crash: got first byte %#x, want %#x", lba, got[0], want[0])
				}
			}
		})
	}
}
