package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Force a Write+Sync between scans of shard 0 and shard 5. The older
// snapshot entry is also superseded, so it cannot cover the omitted write.
func TestFlusherSnapshotCutPreservesSyncedWrites(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		mode := "write"
		if replicated {
			mode = "supplied-lsn"
		}
		t.Run(mode, func(t *testing.T) {
			for _, finishFlush := range []bool{false, true} {
				name := "crash-after-first-cycle"
				if finishFlush {
					name = "next-cycle-makes-progress"
				}
				t.Run(name, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "store.bin")
					s, err := createLegacyWALStoreForTest(path, 128, 4096)
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
					done := make(chan error, 1)
					waitWriter := func() {
						t.Helper()
						select {
						case err := <-done:
							if err != nil {
								t.Fatal(err)
							}
						case <-time.After(5 * time.Second):
							t.Fatal("writer blocked after snapshot")
						}
					}
					waited := false
					s.dm.afterSnapshotShard = func(shard int) {
						if shard != 0 || fired {
							return
						}
						fired = true
						// No timing race: if publication is excluded by the snapshot,
						// let the real writer finish after it. Otherwise force completion
						// before shard 5 is copied (the broken implementation).
						canPublish := s.mu.TryLock()
						if canPublish {
							s.mu.Unlock()
						}
						go func() {
							var err error
							if replicated {
								low, high = 20, 30
								err = s.ApplyEntry(0, a, low)
								if err == nil {
									err = s.ApplyEntry(5, b, high)
								}
							} else {
								low, err = s.Write(0, a)
								if err == nil {
									high, err = s.Write(5, b)
								}
							}
							if err == nil {
								var acked uint64
								acked, err = s.Sync()
								if err == nil && acked < high {
									err = fmt.Errorf("Sync=%d below %d", acked, high)
								}
							}
							done <- err
						}()
						if canPublish {
							waitWriter()
							waited = true
						}
					}
					if err := s.flusher.flushOnce(); err != nil {
						t.Fatal(err)
					}
					s.dm.afterSnapshotShard = nil
					if !waited {
						waitWriter()
					}
					wantLow, wantHigh := uint64(2), uint64(3)
					if replicated {
						wantLow, wantHigh = 20, 30
					}
					if !fired || low != wantLow || high != wantHigh {
						t.Fatalf("interleaving not reached: fired=%v LSNs=%d/%d", fired, low, high)
					}
					t.Logf("first-cycle checkpoint=%d omitted-synced-LSN=%d tail=%d", s.CheckpointLSN(), low, s.wal.logicalTailValue())
					if finishFlush {
						if err := s.flusher.flushOnce(); err != nil {
							t.Fatal(err)
						}
						if s.CheckpointLSN() < high || s.dm.len() != 0 {
							t.Fatalf("next flush did not drain: checkpoint=%d dirty=%d", s.CheckpointLSN(), s.dm.len())
						}
					}
					// Close only the committer and descriptor: normal Close would flush.
					s.committer.Stop()
					if err := s.fd.Close(); err != nil {
						t.Fatal(err)
					}
					crashed = true
					reopened, err := openLegacyWALStoreForTest(path)
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
		})
	}
}
