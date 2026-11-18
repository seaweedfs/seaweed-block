package storage

import (
	"errors"
	"testing"
)

// Plan row 9c controls (owner claude03): SeedChunkFrontier on a dark v3
// store uses the checkpoint's map/metadata barrier and the sole descriptor
// publication (publishCheckpointV3), never the legacy superblock writer.

// A seed publishes CheckpointLSN = W = map coverage = lsn in exactly one
// slot publication with a fresh map root; the allocator continues at lsn+1
// and a crash and mapped reopen keep the frontier and a later write.
// Mutant: a seed that does not raise W refuses (no candidate above W) and
// fails here.
func TestWALStoreV3SeedPublishesTheCheckpointThroughTheMap(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	before := s.v3.pub.current()
	writes := countSlotWritesV3(s)
	if err := s.SeedChunkFrontier(7); err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if writes.Load() != 1 {
		t.Fatalf("seed wrote %d slots, want one publication", writes.Load())
	}
	if d.CheckpointLSN != 7 || d.W != 7 || d.MapCoveredCheckpoint != 7 || d.MapGeneration <= before.MapGeneration {
		t.Fatalf("seed published checkpoint %d W %d coverage %d map gen %d (was %d)",
			d.CheckpointLSN, d.W, d.MapCoveredCheckpoint, d.MapGeneration, before.MapGeneration)
	}
	if s.NextLSN() != 8 {
		t.Fatalf("NextLSN %d, want 8", s.NextLSN())
	}
	if err := s.SeedChunkFrontier(7); err != nil || writes.Load() != 1 {
		t.Fatalf("repeated seed: err %v, slot writes %d, want an idempotent no-op", err, writes.Load())
	}
	s.v3.testBeforeSlotWrite = nil
	if lsn, err := s.Write(3, blk(3)); err != nil || lsn != 8 {
		t.Fatalf("write after seed: LSN %d err %v, want 8", lsn, err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := r.v3.pub.current(); got.CheckpointLSN != 7 || got.W < 8 {
		t.Fatalf("reopen: checkpoint %d W %d, want 7 and >= 8", got.CheckpointLSN, got.W)
	}
	readAllV3(t, r, 3, 3)
}

// The barrier precedes the publication: when the map candidate is
// persisted, no slot has been published yet. A failure after it (here a
// recovery lifetime change) publishes nothing and moves no volatile
// frontier; a retry then succeeds. Mutant: moving the volatile frontier
// before the publication fails here.
func TestWALStoreV3SeedFailurePublishesNothingAndRetries(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	before := s.v3.pub.current()
	fired := false
	s.v3.testMapPrepared = func() {
		if fired {
			return
		}
		fired = true
		if s.v3.pub.current() != before {
			t.Error("a slot was published before the map candidate was persisted")
		}
		s.v3.recoverGen.Add(2) // a recovery ran between preparation and publication
	}
	if err := s.SeedChunkFrontier(7); !errors.Is(err, ErrMapCoverageStale) {
		t.Fatalf("seed across a lifetime change: err = %v, want ErrMapCoverageStale", err)
	}
	if s.v3.pub.current() != before || s.NextLSN() != 1 || s.v3.wack.Load() != 0 {
		t.Fatal("a failed seed published or moved the volatile frontier")
	}
	if err := s.SeedChunkFrontier(7); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if d := s.v3.pub.current(); d.CheckpointLSN != 7 || s.NextLSN() != 8 {
		t.Fatalf("retry: checkpoint %d NextLSN %d", d.CheckpointLSN, s.NextLSN())
	}
}

// Refusals, writing nothing: local writes beyond the checkpoint, an
// unmapped store (no map candidate for the new checkpoint) and an attached
// chunk layer.
func TestWALStoreV3SeedRefusals(t *testing.T) {
	t.Run("local writes", func(t *testing.T) {
		s, _ := newMappedCheckpointStoreV3(t)
		writeSynced(t, s, 1, 1)
		before := s.v3.pub.current()
		if err := s.SeedChunkFrontier(7); err == nil || s.v3.pub.current() != before {
			t.Fatalf("seed over a local write: err %v", err)
		}
	})
	t.Run("unmapped", func(t *testing.T) {
		s, _ := newStoreV3(t, WModeStrict)
		t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
		before := s.v3.pub.current()
		if err := s.SeedChunkFrontier(7); !errors.Is(err, errV3NotYet) || s.v3.pub.current() != before {
			t.Fatalf("unmapped seed: err %v", err)
		}
	})
	t.Run("attached chunk layer", func(t *testing.T) {
		s, _ := newMappedCheckpointStoreV3(t)
		cfg := ctConfig(1)
		cfg.Bitmap = nil
		cfg.BarrierBound = 1
		mustAttach(t, s, newFakeChunkRemote(), cfg)
		before := s.v3.pub.current()
		if err := s.SeedChunkFrontier(7); err == nil || s.v3.pub.current() != before {
			t.Fatalf("seed with a layer attached: err %v", err)
		}
	})
}
