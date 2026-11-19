package storage

import (
	"bytes"
	"errors"
	"path/filepath"
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
	previousCP := uint64(0)
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
		if cp := s.CheckpointLSN(); cp <= previousCP {
			t.Fatalf("cycle %d: checkpoint %d did not advance beyond %d", round, cp, previousCP)
		} else {
			previousCP = cp
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

// A write's real fallback rollover can run after a map candidate was prepared.
// It preserves the older prefix and the later append remains available to replay.
func TestCheckpointV3_AppendRolloverAfterPrepareKeepsRetainedSuffix(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	s.SetRecoveryRetentionLSNs(1)
	writeSynced(t, s, 1, 2)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	prepared, err := s.prepareOwnedMapCheckpointV3(2)
	if err != nil {
		t.Fatal(err)
	}
	tail := s.v3.w.reclaimThroughV3(2)
	before := s.v3.pub.current()
	if tail <= before.RetainedTail {
		t.Fatal("setup: no releasable retained record")
	}
	// One real slot publication advances the current tail only. The next Write
	// must bring the fallback forward before it can reuse that released range.
	if _, err := s.v3.pub.publish(nil, nil, func(d *descriptorV3) error {
		d.RetainedTail = tail
		d.RetainedHead = max(d.RetainedHead, tail)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	current, floor, ok := s.v3.pub.walSnapshotV3()
	if !ok || floor >= tail {
		t.Fatal("setup: fallback did not pin rollover")
	}
	if lsn, err := s.Write(3, blk(0xD3)); err != nil || lsn != 3 {
		t.Fatalf("write: lsn %d err %v", lsn, err)
	}
	after := s.v3.pub.current()
	if after.SlotGen != current.SlotGen+1 {
		t.Fatal("Write did not execute the fallback rollover")
	}
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err != nil {
		t.Fatalf("prefix publication after rollover: %v", err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	d, floor, ok := s.v3.pub.walSnapshotV3()
	if !ok || d.CheckpointLSN != 2 || d.MapCoveredCheckpoint != 2 || d.W < 3 || floor != tail || d.RetainedTail != tail {
		t.Fatalf("prefix/suffix boundary: cp%d C%d W%d floor%d tail%d want%d", d.CheckpointLSN, d.MapCoveredCheckpoint, d.W, floor, d.RetainedTail, tail)
	}
	s.SetRecoveryRetentionLSNs(1)
	var scanned []RecoveryEntry
	if err := s.ScanLBAs(3, func(e RecoveryEntry) error { scanned = append(scanned, e); return nil }); err != nil || len(scanned) != 1 || scanned[0].LSN != 3 || !bytes.Equal(scanned[0].Data, blk(0xD3)) {
		t.Fatalf("retained suffix: %+v err %v", scanned, err)
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
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readV3(t, r, 3), blk(0xD3)) {
		t.Fatal("retained suffix lost after reopen")
	}
}

func TestCheckpointV3_LazyFillDoesNotInvalidatePreparedPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hydrating")
	s, err := createMappedWALStoreV3ForTest(path, ctNumBlocks, ctBlockSize, v3TestWAL, WModeStrict, ctChunkBlocks)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	image := make([]byte, ctNumBlocks*ctBlockSize)
	copy(image[9*ctBlockSize:], blk(0xA9))
	remote := newFakeChunkRemote()
	remote.seedHead(t, image, 10, 1)
	if err := s.SeedChunkFrontier(10); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, remote, cfg).(*chunkLayerImpl)
	prepared, err := s.prepareOwnedMapCheckpointV3(10)
	if err != nil {
		t.Fatal(err)
	}
	layer.stMu.Lock()
	absent := layer.absent[2]
	layer.stMu.Unlock()
	if !absent {
		t.Fatal("setup: chunk is not absent")
	}
	if got, err := s.Read(9); err != nil || !bytes.Equal(got, blk(0xA9)) {
		t.Fatalf("lazy read: %v", err)
	}
	layer.stMu.Lock()
	absent = layer.absent[2]
	layer.stMu.Unlock()
	if absent {
		t.Fatal("read did not hydrate chunk")
	}
	if !prepared.proof.validV3(s.v3.mapProof) {
		t.Fatal("lazy fill invalidated the checkpoint prefix")
	}
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err != nil {
		t.Fatalf("publish after lazy fill: %v", err)
	}
}

// At every cycle the ring reaches the real admission soft-pressure range.
// An append overlaps map persistence, yet each completed cycle must reclaim;
// none may require a cut or new remote HEAD to escape repeated proof refusal.
func TestChunkLayerV3_CheckpointProgressAtWALPressure(t *testing.T) {
	for _, mode := range []WMode{WModeStrict, WModeLagged} {
		t.Run(map[WMode]string{WModeStrict: "A", WModeLagged: "C"}[mode], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pressure")
			s, err := createMappedWALStoreV3ForTest(path, 64, 4096, 64*1024, mode, 4)
			if err != nil {
				t.Fatal(err)
			}
			s.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = s.Close() })
			mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
			realSync := s.v3.ownedMap.syncFile
			t.Cleanup(func() { s.v3.ownedMap.syncFile = realSync })
			previousCP := uint64(0)
			for cycle := 0; cycle < 8; cycle++ {
				for s.walUsedFraction() < 0.72 {
					if _, err := s.Write(uint32(s.NextLSN()%64), blk(byte(s.NextLSN()))); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
				injected := false
				s.v3.ownedMap.syncFile = func() error {
					if !injected {
						injected = true
						if _, err := s.Write(uint32(s.NextLSN()%64), blk(byte(s.NextLSN()))); err != nil {
							return err
						}
					}
					return realSync()
				}
				err := s.flusher.flushOnce()
				s.v3.ownedMap.syncFile = realSync
				if err != nil {
					t.Fatalf("pressure cycle%d: %v", cycle, err)
				}
				cp := s.CheckpointLSN()
				if !injected || cp <= previousCP {
					t.Fatalf("no progress cycle%d cp%d previous%d", cycle, cp, previousCP)
				}
				previousCP = cp
			}
			floor, ok := s.v3.pub.reuseFloor()
			if !ok || floor < 5*64*1024 {
				t.Fatalf("no sustained ring reuse: floor%d", floor)
			}
			t.Logf("cycles8 checkpoint%d floor%d busy0", previousCP, floor)
		})
	}
}
