package storage

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

// Row 8 store controls for the reset transaction (DESIGN 8.6). The reset is
// dark: only these tests call it.

// slotsV3 decodes both descriptor slots of the file at path.
func slotsV3(t *testing.T, path string) (a, b descriptorV3) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i, off := range []int64{slotAOffsetV3, slotBOffsetV3} {
		buf := make([]byte, slotSizeV3)
		if _, err := f.ReadAt(buf, off); err != nil {
			t.Fatal(err)
		}
		d, err := decodeSlotV3(buf, nil)
		if err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
		if i == 0 {
			a = d
		} else {
			b = d
		}
	}
	return a, b
}

// checkpointedStoreV3 returns a store whose LSNs 1..n are acknowledged and
// checkpointed, with an empty dirty map: the reset precondition holds.
func checkpointedStoreV3(t *testing.T, n uint32) (*WALStore, string, func()) {
	t.Helper()
	s, path := newStoreV3(t, WModeLagged)
	var once sync.Once
	crash := func() { once.Do(func() { crashRecoveryHardeningStore(t, s) }) }
	t.Cleanup(crash)
	attachMapV3(t, s, path)
	writeSynced(t, s, 1, n)
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if d := s.v3.pub.current(); d.CheckpointLSN != uint64(n) {
		t.Fatalf("setup: checkpoint %d, want %d", d.CheckpointLSN, n)
	}
	return s, path, crash
}

// attachMapV3 gives an unmapped fixture a real sidecar map and a candidate
// source for the flusher (the pattern of the row 5f clamp control), so a
// checkpoint can advance and a reopen binds the map.
func attachMapV3(t *testing.T, s *WALStore, path string) {
	t.Helper()
	m, err := createChunkLSNMapV3(path+".chunkmap", s.v3.pub.current(), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	entries := make([]chunkLSNEntryV3, s.NumBlocks())
	for i := range entries {
		entries[i].Unknown = true
	}
	initial, err := m.Prepare(s.v3.pub.current().MapGeneration+1, 0, entries, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.v3.pub.publish(&s.mu, nil, func(d *descriptorV3) error { *d = initial.descriptor(*d); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	s.v3.mapCandidate = func(cp uint64) (func(*descriptorV3) error, error) {
		c, e := m.Prepare(s.v3.pub.current().MapGeneration+1, cp, entries, nil)
		if e != nil {
			return nil, e
		}
		return func(d *descriptorV3) error { *d = c.descriptor(*d); return nil }, nil
	}
}

func countSlotWritesV3(s *WALStore) *atomic.Int64 {
	var n atomic.Int64
	s.v3.testBeforeSlotWrite = func() error { n.Add(1); return nil }
	return &n
}

func readAllV3(t *testing.T, s *WALStore, from, to uint32) {
	t.Helper()
	for lba := from; lba <= to; lba++ {
		if got := readV3(t, s, lba); !bytes.Equal(got, blk(byte(lba))) {
			t.Fatalf("LBA %d: got %x..., want %x...", lba, got[0], byte(lba))
		}
	}
}

func TestWALStoreV3ResetRefusesUncheckpointedOrUnacknowledgedData(t *testing.T) {
	t.Run("acknowledged but not checkpointed", func(t *testing.T) {
		s, _ := newStoreV3(t, WModeLagged)
		t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
		writeSynced(t, s, 1, 2)
		old := s.v3.pub.current()
		writes := countSlotWritesV3(s)
		if err := s.resetIncarnationV3(); !errors.Is(err, ErrResetNotPreserved) {
			t.Fatalf("err = %v, want ErrResetNotPreserved", err)
		}
		if writes.Load() != 0 || s.v3.pub.current() != old {
			t.Fatal("a refused reset wrote or published")
		}
	})
	t.Run("appended but not acknowledged", func(t *testing.T) {
		s, _, _ := checkpointedStoreV3(t, 2)
		if _, err := s.Write(3, blk(3)); err != nil {
			t.Fatal(err)
		}
		old := s.v3.pub.current()
		writes := countSlotWritesV3(s)
		if err := s.resetIncarnationV3(); !errors.Is(err, ErrResetNotPreserved) {
			t.Fatalf("err = %v, want ErrResetNotPreserved", err)
		}
		if writes.Load() != 0 || s.v3.pub.current() != old {
			t.Fatal("a refused reset wrote or published")
		}
	})
	t.Run("checkpointed but the dirty map not yet released", func(t *testing.T) {
		// The window between a checkpoint publication and the flusher's
		// release of the covered dirty entries: an entry still names an
		// old-incarnation WAL position a read would follow.
		s, _, _ := checkpointedStoreV3(t, 2)
		_, _, lsn, _, _ := s.dm.get(1)
		s.dm.put(1, 0, max(lsn, 1), v3TestBlock)
		old := s.v3.pub.current()
		writes := countSlotWritesV3(s)
		if err := s.resetIncarnationV3(); !errors.Is(err, ErrResetNotPreserved) {
			t.Fatalf("err = %v, want ErrResetNotPreserved", err)
		}
		if writes.Load() != 0 || s.v3.pub.current() != old {
			t.Fatal("a refused reset wrote or published")
		}
	})
}

func TestWALStoreV3ResetPublishesBothSlotsAndWritesRecoverAcrossACrash(t *testing.T) {
	s, path, crash := checkpointedStoreV3(t, 3)
	old := s.v3.pub.current()
	if err := s.resetIncarnationV3(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	a, b := slotsV3(t, path)
	if a.Nonce != b.Nonce || a.Nonce == old.Nonce {
		t.Fatalf("slots after reset: nonces %x / %x, old %x: want both on one new nonce", a.Nonce, b.Nonce, old.Nonce)
	}
	d := s.v3.pub.current()
	if d.RetainedTail != 0 || d.RetainedHead != 0 || d.W != old.W || d.CheckpointLSN != old.CheckpointLSN || d.MapRootHash != old.MapRootHash {
		t.Fatalf("after reset %+v (old %+v)", d, old)
	}
	readAllV3(t, s, 1, 3)
	writeSynced(t, s, 4, 6)
	if got := s.v3.pub.current(); got.Nonce != d.Nonce {
		t.Fatalf("writes after the reset did not land in the new incarnation: %+v", got)
	}
	crash()
	r := mustReopenV3(t, path)
	defer r.Close()
	readAllV3(t, r, 1, 6)
}

func TestWALStoreV3ResetCrashOutcomes(t *testing.T) {
	boom := errors.New("injected slot write error")
	failNth := func(s *WALStore, nth int64) {
		var n atomic.Int64
		s.v3.testBeforeSlotWrite = func() error {
			if n.Add(1) == nth {
				return boom
			}
			return nil
		}
	}
	t.Run("step 2 fails: the old image", func(t *testing.T) {
		s, path, crash := checkpointedStoreV3(t, 3)
		old := s.v3.pub.current()
		failNth(s, 1)
		if err := s.resetIncarnationV3(); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the write error", err)
		}
		if _, err := s.Write(4, blk(4)); err == nil {
			t.Fatal("a write was admitted after a failed reset")
		}
		crash()
		r := mustReopenV3(t, path)
		defer r.Close()
		if got := r.v3.pub.current(); got.Nonce != old.Nonce {
			t.Fatal("a failed step 2 changed the selected incarnation")
		}
		readAllV3(t, r, 1, 3)
		writeSynced(t, r, 4, 5)
		readAllV3(t, r, 4, 5)
	})
	t.Run("step 3 fails: the new image, never a successful reset", func(t *testing.T) {
		s, path, crash := checkpointedStoreV3(t, 3)
		old := s.v3.pub.current()
		failNth(s, 2)
		if err := s.resetIncarnationV3(); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the step-3 write error", err)
		}
		if _, err := s.Write(4, blk(4)); err == nil {
			t.Fatal("a write was admitted after a half-published reset")
		}
		crash()
		a, b := slotsV3(t, path)
		if a.Nonce == b.Nonce {
			t.Fatal("setup: the slots should carry the old and the new nonce")
		}
		r := mustReopenV3(t, path)
		defer r.Close()
		if got := r.v3.pub.current(); got.Nonce == old.Nonce {
			t.Fatal("reopen after step 2 did not select the new incarnation")
		}
		readAllV3(t, r, 1, 3)
		writeSynced(t, r, 4, 5)
		readAllV3(t, r, 4, 5)
		a, b = slotsV3(t, path)
		if a.Nonce != b.Nonce {
			t.Fatal("the half-published reset is still mixed after a recovered reopen and writes")
		}
	})
}

// After a half-published reset, an opened but not yet recovered store binds
// no writer across the two nonces: a write is refused until Recover has
// completed the new incarnation in the other slot.
func TestWALStoreV3HalfResetRefusesWritesUntilRecover(t *testing.T) {
	s, path, crash := checkpointedStoreV3(t, 2)
	var n atomic.Int64
	s.v3.testBeforeSlotWrite = func() error {
		if n.Add(1) == 2 {
			return errors.New("injected step-3 write error")
		}
		return nil
	}
	if err := s.resetIncarnationV3(); err == nil {
		t.Fatal("setup: the reset should fail at step 3")
	}
	crash()
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatalf("open after a half-published reset: %v", err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	if r.v3.pub.mapDomain == nil {
		t.Fatal("an open across a half-published reset did not bind its map")
	}
	if _, err := r.Write(3, blk(3)); !errors.Is(err, errWALBindingV3) {
		t.Fatalf("write before Recover across a half-published reset: err = %v, want errWALBindingV3", err)
	}
	if _, err := r.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	writeSynced(t, r, 3, 3)
	readAllV3(t, r, 1, 3)
}

// codex01 row 8 P1: a Read that resolved LBA 1's WAL position (cursor 0)
// parks before its pread; the real flusher checkpoints and releases the
// entry, the reset starts the new incarnation at cursor 0, and Write(2, B)
// reuses that position. The resumed Read must not return B for LBA 1: it
// validates the whole record against the resolving entry and re-resolves,
// reading A from the extent.
func TestWALStoreV3ReadParkedAcrossAResetAndOverwriteReturnsItsOwnBlock(t *testing.T) {
	s, _, _ := checkpointedStoreV3(t, 0)
	h := newHeldV3(t)
	writeSynced(t, s, 1, 1) // LBA 1 = A at cursor 0
	if off, _, _, _, ok := s.dm.get(1); !ok || off != 0 {
		t.Fatalf("setup: LBA 1 dirty at %d (%v), want cursor 0", off, ok)
	}
	s.v3.testReadResolved = h.hold
	rd := make(chan []byte, 1)
	res := h.start(func() error {
		b, err := s.Read(1)
		rd <- b
		return err
	})
	waitV3(t, h.parked, "Read after resolving cursor 0")
	s.v3.testReadResolved = nil
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if s.dm.len() != 0 {
		t.Fatal("setup: the flusher did not release the entry")
	}
	if err := s.resetIncarnationV3(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	if off, _, _, _, ok := s.dm.get(2); !ok || off != 0 {
		t.Fatalf("setup: LBA 2 at %d (%v), want the reused cursor 0", off, ok)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	h.rel()
	if err := h.result(res, "Read"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := <-rd; !bytes.Equal(got, blk(1)) {
		t.Fatalf("Read(1) across a reset and overwrite returned %x..., want LBA 1's own block %x...", got[0], byte(1))
	}
}

// The same schedule through the post-OnReadMiss recheck: the read missed the
// dirty map, A was written while the miss was held, the recheck resolved
// cursor 0 and parks there across the flush, reset and overwrite.
func TestWALStoreV3ReadMissRecheckParkedAcrossAResetReturnsItsOwnBlock(t *testing.T) {
	s, _, _ := checkpointedStoreV3(t, 0)
	h := newHeldV3(t)
	miss := &heldWALReadMiss{entered: make(chan struct{}), release: make(chan struct{})}
	releaseMiss := sync.OnceFunc(func() { close(miss.release) })
	t.Cleanup(releaseMiss) // registered after heldV3's join: runs first
	s.chunk.Store(&chunkHooks{sink: miss, closer: func() error { return nil }})
	rd := make(chan []byte, 1)
	res := h.start(func() error {
		b, err := s.Read(1)
		rd <- b
		return err
	})
	waitV3(t, miss.entered, "Read in OnReadMiss")
	writeSynced(t, s, 1, 1) // LBA 1 = A at cursor 0, while the miss is held
	s.v3.testReadResolved = h.hold
	releaseMiss()
	waitV3(t, h.parked, "the post-miss recheck after resolving cursor 0")
	s.v3.testReadResolved = nil
	s.chunk.Store(nil)
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if err := s.resetIncarnationV3(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	writeSynced(t, s, 2, 2) // reuses cursor 0
	if off, _, _, _, ok := s.dm.get(2); !ok || off != 0 {
		t.Fatalf("setup: LBA 2 at %d (%v), want the reused cursor 0", off, ok)
	}
	h.rel()
	if err := h.result(res, "Read"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := <-rd; !bytes.Equal(got, blk(1)) {
		t.Fatalf("post-miss Read(1) across a reset and overwrite returned %x..., want %x...", got[0], byte(1))
	}
}

// A live dirty entry whose record no longer validates (the entry unchanged
// on re-resolution) is WAL damage: the read fails with a WAL integrity
// fault instead of returning the damaged bytes or retrying forever.
func TestWALStoreV3ReadOfADamagedLiveRecordIsAnIntegrityFault(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 1)
	damageRecord(t, path, 1)
	if _, err := s.Read(1); !isWALCorrupt(err) {
		t.Fatalf("Read of a damaged live record: err = %v, want a WAL integrity failure", err)
	}
}
