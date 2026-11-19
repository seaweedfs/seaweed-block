package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"
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
			expected := make(map[uint32][]byte)
			write := func() error {
				lsn := s.NextLSN()
				lba := uint32(lsn % 64)
				data := checkpointProgressBlock(lsn)
				if _, err := s.Write(lba, data); err != nil {
					return err
				}
				expected[lba] = data
				return nil
			}
			for cycle := 0; cycle < 8; cycle++ {
				for s.walUsedFraction() < 0.72 {
					if err := write(); err != nil {
						t.Fatal(err)
					}
				}
				injected := false
				s.v3.ownedMap.syncFile = func() error {
					if !injected {
						injected = true
						if err := write(); err != nil {
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
			for lba, want := range expected {
				if got, err := s.Read(lba); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("no-Flush readback LBA%d: %v", lba, err)
				}
			}
			t.Logf("no client Sync: cycles8 checkpoint%d floor%d busy0", previousCP, floor)
		})
	}
}

func checkpointProgressBlock(lsn uint64) []byte {
	data := make([]byte, 4096)
	for i := 0; i < len(data); i += 8 {
		binary.LittleEndian.PutUint64(data[i:i+8], lsn*4096+uint64(i))
	}
	return data
}

// An active history pin gives one bounded, typed refusal; local flusher work
// after releasing it restores progress without a client Sync or new remote cut.
func TestCheckpointV3_NoFlushPressureWithRealPin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pinned")
	s, err := createMappedWALStoreV3ForTest(path, 64, 4096, 64*1024, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
	s.SetRecycleFloorSource(&fakeRecycleFloorSource{floor: 0, anyActive: true})
	expected := make(map[uint32][]byte)
	for s.walUsedFraction() < 0.90 {
		lsn := s.NextLSN()
		lba := uint32(lsn)
		data := checkpointProgressBlock(lsn)
		if _, err := s.Write(lba, data); err != nil {
			t.Fatal(err)
		}
		expected[lba] = data
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() != 0 || s.v3.wack.Load() == 0 {
		t.Fatal("real retention pin did not constrain an internally synced checkpoint")
	}
	next := s.NextLSN()
	start := time.Now()
	_, err = s.Write(uint32(next), checkpointProgressBlock(next))
	elapsed := time.Since(start)
	if !errors.Is(err, ErrWALPressureTimeout) || elapsed >= 5*time.Second || s.NextLSN() != next {
		t.Fatalf("pinned busy: err%v elapsed%v next%d want%d", err, elapsed, s.NextLSN(), next)
	}
	count := 0
	if err := s.ScanLBAs(1, func(e RecoveryEntry) error {
		count++
		if e.LSN != uint64(count) || !bytes.Equal(e.Data, expected[e.LBA]) {
			return errors.New("pinned retained prefix differs from submitted bytes")
		}
		return nil
	}); err != nil || count != len(expected) {
		t.Fatalf("pinned exact prefix count%d want%d: %v", count, len(expected), err)
	}
	s.SetRecycleFloorSource(nil)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	} // no Write/Sync wakes it
	if s.CheckpointLSN() != next-1 {
		t.Fatal("released pin did not permit passive local checkpoint progress")
	}
	if lsn, err := s.Write(uint32(next), checkpointProgressBlock(next)); err != nil || lsn != next {
		t.Fatalf("retry: %d %v", lsn, err)
	}
	expected[uint32(next)] = checkpointProgressBlock(next)
	for lba, want := range expected {
		if got, err := s.Read(lba); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("pinned readback LBA%d: %v", lba, err)
		}
	}
	t.Logf("busy1 elapsed%v retained%d checkpoint%d", elapsed, count, s.CheckpointLSN())
}

func TestCheckpointV3_AppendRolloverFaultRefusesHeldCandidate(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
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
	if _, err := s.v3.pub.publish(nil, nil, func(d *descriptorV3) error {
		d.RetainedTail = tail
		d.RetainedHead = max(d.RetainedHead, tail)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := s.v3.pub.current()
	_, floor, ok := s.v3.pub.walSnapshotV3()
	if !ok || floor >= tail {
		t.Fatal("setup: no pinned fallback")
	}
	injected := errors.New("append rollover slot fault")
	s.v3.testBeforeSlotWrite = func() error { return injected }
	_, err = s.Write(3, blk(0xD3))
	s.v3.testBeforeSlotWrite = nil
	if !errors.Is(err, injected) || s.stickyIOFault() == nil || s.NextLSN() != 3 || s.dm.len() != 0 {
		t.Fatalf("rollover fault: %v next%d dirty%d", err, s.NextLSN(), s.dm.len())
	}
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err == nil {
		t.Fatal("held candidate published through sticky rollover failure")
	}
	if s.v3.pub.current() != before {
		t.Fatal("rollover fault changed authoritative tuple")
	}
	if nextFloor, valid := s.v3.pub.reuseFloor(); valid || nextFloor != 0 {
		t.Fatal("uncertain rollover must disable reuse until reopen")
	}
}

// Max-LSN map entries may include unsynced writes above both cp and W. A
// reused LSN can then set UNKNOWN without changing the prefix proof token.
func TestCheckpointV3_SpeculativeMaximumAfterDroppedSuffix(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	remote := newFakeChunkRemote()
	layer := mustAttach(t, s, remote, ctConfig(1))
	if _, err := s.Write(1, blk(0xA1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if s.v3.mapEntries[0].Unknown {
		t.Fatal("setup: initial definite cut did not clear UNKNOWN")
	}
	if _, err := s.Write(1, blk(0xA2)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, blk(0xA3)); err != nil {
		t.Fatal(err)
	}
	prepared, err := s.prepareOwnedMapCheckpointV3(1)
	if err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	e := prepared.candidate.entries[0]
	if e.LSN != 3 || e.Unknown || d.CheckpointLSN != 1 || d.W != 1 {
		t.Fatalf("speculative maximum: entry%+v cp%d W%d", e, d.CheckpointLSN, d.W)
	}
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, prepared, nil); err != nil {
		t.Fatal(err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	damageRecord(t, path, 2) // unacknowledged record2 tears; the following3 is dropped
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if r.NextLSN() != 2 || r.v3.mapEntries[0].LSN != 3 {
		t.Fatal("dropped suffix did not leave the conservative persisted maximum")
	}
	held, err := r.prepareOwnedMapCheckpointV3(1)
	if err != nil {
		t.Fatal(err)
	}
	if held.candidate.entries[0].Unknown {
		t.Fatal("setup: held candidate already UNKNOWN")
	}
	if lsn, err := r.Write(1, blk(0xB2)); err != nil || lsn != 2 {
		t.Fatalf("reused LSN: %d %v", lsn, err)
	}
	if !r.v3.mapEntries[0].Unknown || r.v3.mapEntries[0].LSN != 3 {
		t.Fatal("reused LSN did not conservatively set UNKNOWN beneath the high maximum")
	}
	if !held.proof.validV3(r.v3.mapProof) {
		t.Fatal("reused ordered append invalidated the older prefix")
	}
	if _, err := r.v3.pub.publishMapV3(&r.mu, nil, held, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, r)
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r2, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r2.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r2.Close() })
	if _, err := r2.Recover(); err != nil {
		t.Fatal(err)
	}
	if !r2.v3.mapEntries[0].Unknown || !bytes.Equal(readV3(t, r2, 1), blk(0xB2)) {
		t.Fatal("replay did not restore UNKNOWN and exact reused-LSN bytes")
	}
	restored := mustAttach(t, r2, remote, ctConfig(2))
	cut, err := restored.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	image := remote.imageOf(t, cut.Head)
	if !bytes.Equal(image[4096:8192], blk(0xB2)) {
		t.Fatal("capture treated the speculative maximum as remotely clean")
	}
}

func TestCheckpointV3_HeldHydrationWithLocalWriteAndFlush(t *testing.T) {
	for _, flush := range []bool{false, true} {
		t.Run(map[bool]string{false: "wal", true: "extent"}[flush], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "hydrating")
			s, err := createMappedWALStoreV3ForTest(path, ctNumBlocks, ctBlockSize, v3TestWAL, WModeStrict, ctChunkBlocks)
			if err != nil {
				t.Fatal(err)
			}
			s.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = s.Close() })
			image := make([]byte, ctNumBlocks*ctBlockSize)
			copy(image[8*ctBlockSize:], blk(0xA8))
			copy(image[9*ctBlockSize:], blk(0xA9))
			remote := newFakeChunkRemote()
			remote.seedHead(t, image, 10, 1)
			if err := s.SeedChunkFrontier(10); err != nil {
				t.Fatal(err)
			}
			cfg := ctConfig(2)
			cfg.FreshFromHead = true
			layer := mustAttach(t, s, remote, cfg)
			held, err := s.prepareOwnedMapCheckpointV3(10)
			if err != nil {
				t.Fatal(err)
			}
			entered, release := holdNextGet(t, remote)
			done := make(chan error, 1)
			joined := make(chan struct{})
			// Release the fetch before joining, including failures before release().
			// Register before launch so store/layer cleanup never precedes the join.
			t.Cleanup(func() {
				release()
				select {
				case <-joined:
				case <-time.After(ctLocalBound):
					t.Errorf("held hydration reader did not settle within %v after release", ctLocalBound)
				}
			})
			go func() {
				defer close(joined)
				got, err := s.Read(9)
				if err == nil && !bytes.Equal(got, blk(0xB9)) {
					err = errors.New("held read did not return newer local bytes")
				}
				done <- err
			}()
			waitKey(t, entered, "cold fill before a competing local write")
			within(t, "local write and optional flush while hydration is held", func() error {
				if _, err := s.Write(9, blk(0xB9)); err != nil {
					return err
				}
				if flush {
					if _, err := s.Sync(); err != nil {
						return err
					}
					return s.flusher.flushOnce()
				}
				return nil
			})
			dirty := s.dm.len()
			release()
			if err := waitErr(t, done, "fill and post-miss WAL recheck"); err != nil {
				t.Fatal(err)
			}
			if s.dm.len() != dirty || !held.proof.validV3(s.v3.mapProof) {
				t.Fatal("hydration manufactured logical dirt or invalidated the prefix proof")
			}
			before := s.v3.pub.current()
			_, err = s.v3.pub.publishMapV3(&s.mu, nil, held, nil)
			if flush {
				if !errors.Is(err, ErrMapCoverageStale) || s.v3.pub.current() != before {
					t.Fatalf("superseded candidate after flush: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			// Cover the hydrated extent bytes too before the reopen oracle.
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readV3(t, s, 8), blk(0xA8)) || !bytes.Equal(readV3(t, s, 9), blk(0xB9)) {
				t.Fatal("HEAD fill or local write was overwritten")
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
			if _, err := r.Recover(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readV3(t, r, 8), blk(0xA8)) || !bytes.Equal(readV3(t, r, 9), blk(0xB9)) {
				t.Fatal("reopen lost exact hydrated/local bytes")
			}
		})
	}
}

func TestCheckpointV3_DefiniteCutClearInvalidatesHeldProof(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	layer := mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
	writeSynced(t, s, 1, 1)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	held, err := s.prepareOwnedMapCheckpointV3(1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.v3.mapEntries[0].Unknown {
		t.Fatal("setup: no UNKNOWN to clear")
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if s.v3.mapEntries[0].Unknown {
		t.Fatal("definite cut did not clear UNKNOWN")
	}
	// Inspect the proof itself so the newer map generation cannot mask a missing
	// clear-invalidation guard in the publication refusal below.
	if held.proof.validV3(s.v3.mapProof) {
		t.Fatal("capture UNKNOWN clearing left the old proof valid")
	}
	before := s.v3.pub.current()
	if _, err := s.v3.pub.publishMapV3(&s.mu, nil, held, nil); !errors.Is(err, ErrMapCoverageStale) {
		t.Fatalf("old capture proof: %v", err)
	}
	if s.v3.pub.current() != before {
		t.Fatal("capture-stale candidate changed a slot")
	}
}
