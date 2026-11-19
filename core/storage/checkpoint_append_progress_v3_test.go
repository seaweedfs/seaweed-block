package storage

import (
	"bytes"
	"errors"
	"testing"
)

func TestChunkLayerV3_CheckpointProgressUnderContinuousAppends(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	remote := newFakeChunkRemote()
	mustAttach(t, s, remote, ctConfig(1))
	if _, err := s.Write(0, blk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	realSync := s.v3.ownedMap.syncFile
	t.Cleanup(func() { s.v3.ownedMap.syncFile = realSync })
	refusals := 0
	for round := uint32(0); round < 8; round++ {
		injected := false
		s.v3.ownedMap.syncFile = func() error {
			if !injected {
				injected = true
				if _, err := s.Write(round+1, blk(byte(round+2))); err != nil {
					return err
				}
			}
			return realSync()
		}
		err := s.flusher.flushOnce()
		s.v3.ownedMap.syncFile = realSync
		if err != nil && !errors.Is(err, ErrMapCoverageStale) {
			t.Fatal(err)
		}
		if errors.Is(err, ErrMapCoverageStale) {
			refusals++
		}
		if !injected {
			t.Fatal("cycle did not overlap a write")
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	cp := s.CheckpointLSN()
	t.Logf("continuous writes: cycles8 refusals%d cp%d wack%d dirty%d", refusals, cp, s.v3.wack.Load(), s.dm.len())
	if cp == 0 {
		t.Fatal("checkpoint made no progress across eight completed extent/map persistence cycles while newer appends continued")
	}
}

func TestCheckpointV3_NewerBatchReplaysAfterPreparedPrefix(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	remote := newFakeChunkRemote()
	layer := mustAttach(t, s, remote, ctConfig(1))
	if _, err := s.Write(1, blk(0x11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.v3.testMapPrepared = func() {
		if _, err := s.WriteBatch(1, [][]byte{blk(0xB2), blk(0xC3)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	s.v3.testMapPrepared = nil
	d := s.v3.pub.current()
	if d.CheckpointLSN != 1 || d.MapCoveredCheckpoint != 1 || s.dm.len() != 2 {
		t.Fatalf("prefix cp=%d C=%d dirty=%d", d.CheckpointLSN, d.MapCoveredCheckpoint, s.dm.len())
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err = r.Recover(); err != nil {
		t.Fatal(err)
	}
	for lba, want := range map[uint32][]byte{1: blk(0xB2), 2: blk(0xC3)} {
		if !bytes.Equal(readV3(t, r, lba), want) {
			t.Fatalf("replayed LBA%d lost", lba)
		}
	}
	restored := mustAttach(t, r, remote, ctConfig(1))
	cut, err := restored.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	image := remote.imageOf(t, cut.Head)
	if !bytes.Equal(image[4096:8192], blk(0xB2)) || !bytes.Equal(image[8192:12288], blk(0xC3)) {
		t.Fatal("capture omitted newer replayed batch")
	}
}

func TestCheckpointV3_EffectiveBaseWriteStillInvalidatesProof(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	h := beginV3(t, s, 0, idA)
	cp := s.CheckpointLSN()
	prepared, err := s.prepareOwnedMapCheckpointV3(cp)
	if err != nil {
		t.Fatal(err)
	}
	before := s.v3.pub.current()
	if err = h.WriteBase(1, blk(0xA7)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); !errors.Is(err, ErrMapCoverageStale) {
		t.Fatalf("effective BASE mutation proof accepted: %v", err)
	}
	if s.v3.pub.current() != before {
		t.Fatal("stale BASE candidate changed slots")
	}
}

// Removing append-driven increments must not bypass terminal exhaustion.
func TestCheckpointV3_ExhaustedMutationCounterStillRefusesAppend(t *testing.T) {
	for _, batch := range []bool{false, true} {
		s, _ := newMappedCheckpointStoreV3(t)
		before := s.NextLSN()
		s.v3.mapProof.mutation.Store(counterMaxV3)
		var err error
		if batch {
			_, err = s.WriteBatch(1, [][]byte{blk(1), blk(2)})
		} else {
			_, err = s.Write(1, blk(1))
		}
		if !errors.Is(err, ErrCounterExhausted) || s.NextLSN() != before || s.dm.len() != 0 {
			t.Fatalf("exhaustion bypassed: batch=%t err=%v next=%d dirty=%d", batch, err, s.NextLSN(), s.dm.len())
		}
	}
}
