package storage

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
)

func TestWALStoreV3_FlushedProgressRetriesAfterLaterSync(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		t.Run(map[WMode]string{WModeLagged: "lagged", WModeStrict: "strict"}[mode], func(t *testing.T) {
			s, path := newStoreV3(t, mode)
			var once sync.Once
			crash := func() { once.Do(func() { crashRecoveryHardeningStore(t, s) }) }
			t.Cleanup(crash) // own the file/committer even when setup or an assertion fails
			d := s.v3.pub.current()
			m, err := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := m.Close(); err != nil {
					t.Error(err)
				}
			})
			// Real durable COW map candidates, with conservative coverage of all chunks.
			// Automatic map attach and the bound-proof callback seam are separate row7 work.
			es := make([]chunkLSNEntryV3, s.NumBlocks())
			for i := range es {
				es[i].Unknown = true
			}
			generation := d.MapGeneration
			var prior *chunkMapCandidateV3
			s.v3.mapCandidate = func(cp uint64) (func(*descriptorV3) error, error) {
				generation++
				c, err := m.Prepare(generation, cp, es, prior)
				if err != nil {
					return nil, err
				}
				prior = c
				return func(n *descriptorV3) error { *n = c.descriptor(*n); return nil }, nil
			}
			writeSynced(t, s, 1, 1)
			if _, err := s.Write(2, blk(2)); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			first := s.v3.pub.current()
			if first.CheckpointLSN != 1 || first.MapCoveredCheckpoint != 1 {
				t.Fatalf("first clamp: cp=%d coverage=%d", first.CheckpointLSN, first.MapCoveredCheckpoint)
			}
			if _, _, lsn, _, ok := s.dm.get(2); !ok || lsn != 2 {
				t.Error("flushed LSN2 lost its checkpoint retry ownership")
			}
			// No new write: formerly an empty cycle refreshed W only and stranded cp/coverage1.
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			last := s.v3.pub.current()
			if last.CheckpointLSN != 2 || last.MapCoveredCheckpoint != 2 || last.W < 2 {
				t.Fatalf("later Sync plus cycle stranded flushed progress: cp=%d coverage=%d W=%d", last.CheckpointLSN, last.MapCoveredCheckpoint, last.W)
			}
			if s.dm.len() != 0 {
				t.Fatal("covered flushed entries did not retire from the dirty map")
			}
			if _, _, err := readChunkLSNMapV3(m.fd, last, 1); err != nil {
				t.Fatalf("published coverage has no valid durable map: %v", err)
			}
			crash()
			r := mustReopenV3(t, path)
			t.Cleanup(func() {
				if err := r.Close(); err != nil {
					t.Error(err)
				}
			})
			for lba := uint32(1); lba <= 2; lba++ {
				if !bytes.Equal(readV3(t, r, lba), blk(byte(lba))) {
					t.Fatalf("reopened checkpoint lost LBA%d", lba)
				}
			}
		})
	}
}
