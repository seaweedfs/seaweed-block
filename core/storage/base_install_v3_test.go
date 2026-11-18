package storage

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Plan row 9a controls (DESIGN r17.3.1 sections 8.1, 8.2, 8.4, 8.5, 8.7):
// the v3 BASE begin, the install handle, complete, no abort and retirement.
// Owner: claude03. Each control names the mutant it qualifies.

var (
	idA = [16]byte{0xA}
	idB = [16]byte{0xB}
)

func beginV3(t *testing.T, s *WALStore, baseLSN uint64, id [16]byte) *installHandleV3 {
	t.Helper()
	h, err := s.beginBaseInstallV3(baseLSN, id)
	if err != nil {
		t.Fatalf("begin BaseLSN %d: %v", baseLSN, err)
	}
	return h
}

// Begin publishes installing (BaseLSN, BaseCursor = the writer head, the
// ID) into BOTH slots before it returns the handle (8.1 steps 3 and 4).
// Mutant: a one-slot begin (no step 4) fails here.
func TestBaseInstallV3BeginPublishesInstallingIntoBothSlots(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 3)
	P := s.v3.w.logicalHeadV3()
	beginV3(t, s, 3, idA)
	a, b := slotsV3(t, path)
	for i, d := range []descriptorV3{a, b} {
		if d.BaseState != BaseInstalling || d.BaseInstallID != idA || d.BaseLSN != 3 || d.BaseCursor != P {
			t.Fatalf("slot %d: state %d id %x BaseLSN %d BaseCursor %d, want installing %x 3 %d",
				i, d.BaseState, d.BaseInstallID[0], d.BaseLSN, d.BaseCursor, idA[0], P)
		}
	}
}

// The coverage rule (8.1): a begin from none or a superseding begin with
// BaseLSN below the acknowledged W is refused before any descriptor write,
// with the begin-specific error. Mutant: dropping the W check fails here.
func TestBaseInstallV3BeginRefusesBelowW(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 3)
	writes := countSlotWritesV3(s)
	a0, b0 := slotsV3(t, path)
	if _, err := s.beginBaseInstallV3(2, idA); !errors.Is(err, ErrInstallBaseUncovered) {
		t.Fatalf("begin from none below W: err = %v, want ErrInstallBaseUncovered", err)
	}
	if a, b := slotsV3(t, path); writes.Load() != 0 || a != a0 || b != b0 {
		t.Fatal("a refused begin wrote a descriptor slot")
	}
	s.v3.testBeforeSlotWrite = nil
	beginV3(t, s, 3, idA)
	writes = countSlotWritesV3(s)
	a0, b0 = slotsV3(t, path)
	if _, err := s.beginBaseInstallV3(2, idB); !errors.Is(err, ErrInstallSupersedeUncovered) {
		t.Fatalf("superseding begin below W: err = %v, want ErrInstallSupersedeUncovered", err)
	}
	if a, b := slotsV3(t, path); writes.Load() != 0 || a != a0 || b != b0 {
		t.Fatal("a refused superseding begin wrote a descriptor slot")
	}
}

// Mode C: W includes an acknowledged LSN the lagged slot W does not show
// yet. A begin at the published W but below the acknowledged one is
// refused. Mutant: checking only the published W fails here.
func TestBaseInstallV3BeginCoversTheAcknowledgedNotYetPublishedW(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 3)
	d := s.v3.pub.current()
	if ack := s.v3.wack.Load(); d.W >= ack {
		t.Fatalf("setup: published W %d is not below the acknowledged %d", d.W, ack)
	}
	if _, err := s.beginBaseInstallV3(d.W, idA); !errors.Is(err, ErrInstallBaseUncovered) {
		t.Fatalf("begin at the lagged W %d: err = %v, want ErrInstallBaseUncovered", d.W, err)
	}
}

func TestBaseInstallV3BeginRefusals(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	if _, err := s.beginBaseInstallV3(0, [16]byte{}); !errors.Is(err, errDescriptorTransition) {
		t.Fatalf("zero ID: err = %v", err)
	}
	beginV3(t, s, 0, idA)
	if _, err := s.beginBaseInstallV3(0, idA); !errors.Is(err, errDescriptorTransition) {
		t.Fatalf("begin with the installing ID: err = %v", err)
	}
}

// Complete (8.2): a stale ID is refused with no slot write; the real ID
// makes BASE bytes and the install-lane suffix durable, publishes installed
// with retainedTail >= BaseCursor, and moves the frontier to BaseLSN. A
// crash and reopen serve the installed image and the suffix. The pre-BASE
// guest records (LSN 1, 2 at LBA 1, 2, behind BaseCursor) are superseded
// by the BASE image: the open store and the recovered one both stop
// serving them (the BASE here does not ship LBA 1 or 2). Mutant: keeping
// the pre-BASE dirty entries at complete serves LBA 1 before the crash and
// not after it.
func TestBaseInstallV3CompleteServesTheImageAcrossACrash(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	writeSynced(t, s, 1, 2)
	h := beginV3(t, s, 10, idA)
	if err := h.WriteBase(5, blk(0x55)); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(6, blk(0x66), 11); err != nil {
		t.Fatal(err)
	}
	writes := countSlotWritesV3(s)
	if err := s.markBaseCompleteV3(idB); !errors.Is(err, ErrInstallSuperseded) || writes.Load() != 0 {
		t.Fatalf("stale-ID complete: err = %v, slot writes %d", err, writes.Load())
	}
	s.v3.testBeforeSlotWrite = nil
	if err := s.markBaseCompleteV3(idA); err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if d.BaseState != BaseInstalled || d.BaseInstallID != idA || d.RetainedTail < d.BaseCursor {
		t.Fatalf("after complete: state %d id %x tail %d cursor %d", d.BaseState, d.BaseInstallID[0], d.RetainedTail, d.BaseCursor)
	}
	if err := h.WriteBase(5, blk(0x77)); !errors.Is(err, ErrInstallSuperseded) {
		t.Fatalf("WriteBase after complete: err = %v, want ErrInstallSuperseded", err)
	}
	served := map[uint32]byte{1: 0, 2: 0, 5: 0x55, 6: 0x66}
	for lba, want := range served {
		if got := readV3(t, s, lba); !bytes.Equal(got, blk(want)) {
			t.Fatalf("after complete LBA %d = %x..., want %x", lba, got[0], want)
		}
	}
	lsn, err := s.Write(7, blk(0x70))
	if err != nil || lsn != 12 {
		t.Fatalf("first guest write after complete: LSN %d err %v, want 12", lsn, err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	served[7] = 0x70
	for lba, want := range served {
		if got := readV3(t, r, lba); !bytes.Equal(got, blk(want)) {
			t.Fatalf("after reopen LBA %d = %x..., want %x", lba, got[0], want)
		}
	}
}

// No abort (r17, 8.5): installing -> none is refused at the publisher with
// no slot write. Mutant: allowing the edge fails here.
func TestBaseInstallV3InstallingToNoneIsRefused(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	beginV3(t, s, 0, idA)
	writes := countSlotWritesV3(s)
	a0, b0 := slotsV3(t, path)
	_, err := s.v3.pub.publish(&s.mu, nil, func(n *descriptorV3) error {
		n.BaseState, n.BaseLSN, n.BaseCursor, n.BaseInstallID = BaseNone, 0, 0, [16]byte{}
		return nil
	})
	if !errors.Is(err, errDescriptorTransition) {
		t.Fatalf("installing -> none: err = %v, want errDescriptorTransition", err)
	}
	if a, b := slotsV3(t, path); writes.Load() != 0 || a != a0 || b != b0 {
		t.Fatal("a refused abort wrote a descriptor slot")
	}
}

// Retirement (8.4, 8.5): installed stays installed through publications
// until the checkpoint covers BaseLSN; then a normal publication zeroes the
// BASE fields. Mutant: retiring without the checkpoint check fails here.
func TestBaseInstallV3RetiresOnlyOnceTheCheckpointCoversBaseLSN(t *testing.T) {
	s, _, _ := checkpointedStoreV3(t, 2)
	h := beginV3(t, s, 4, idA)
	if err := h.WriteBase(3, blk(3)); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(5, blk(5), 5); err != nil {
		t.Fatal(err)
	}
	if err := s.markBaseCompleteV3(idA); err != nil {
		t.Fatal(err)
	}
	if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	if d := s.v3.pub.current(); d.BaseState != BaseInstalled {
		t.Fatalf("retired with checkpoint %d below BaseLSN %d", d.CheckpointLSN, d.BaseLSN)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	if d.CheckpointLSN < 4 || d.BaseState != BaseNone || d.BaseCursor != 0 || d.BaseInstallID != ([16]byte{}) {
		t.Fatalf("checkpoint %d: state %d cursor %d id %x, want retired", d.CheckpointLSN, d.BaseState, d.BaseCursor, d.BaseInstallID[0])
	}
	readAllV3(t, s, 1, 3)
	readAllV3(t, s, 5, 5)
}

// The order and gap rules (8.1, 6.7) for the install lane.
func TestBaseInstallV3ApplyInstallEntryOrderAndGap(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	h := beginV3(t, s, 4, idA)
	if err := h.ApplyInstallEntry(1, blk(1), 7); !errors.Is(err, ErrInstallLSNGap) {
		t.Fatalf("gap above BaseLSN 4: err = %v, want ErrInstallLSNGap", err)
	}
	if err := h.ApplyInstallEntry(1, blk(1), 5); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(2, blk(2), 5); !errors.Is(err, ErrWALLSNOrder) {
		t.Fatalf("repeated LSN on another LBA: err = %v, want ErrWALLSNOrder", err)
	}
	if err := h.ApplyInstallEntry(1, blk(9), 6); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(1, blk(8), 5); err != nil {
		t.Fatalf("a superseded lower LSN for an LBA that holds 6: err = %v, want a no-op", err)
	}
	if got := readV3(t, s, 1); !bytes.Equal(got, blk(9)) {
		t.Fatalf("LBA 1 = %x..., want the LSN 6 bytes", got[0])
	}
}

// holdOnce returns a hook that parks the first call until release runs,
// and reports through parked when it has parked. The test's cleanup always
// releases, so a failing test never leaves a parked goroutine behind
// (codex01 row 9a P2).
func holdOnce(t *testing.T) (hook func(), parked chan struct{}, release func()) {
	parked = make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var armed atomic.Bool
	armed.Store(true)
	return func() {
		if armed.CompareAndSwap(true, false) {
			close(parked)
			<-gate
		}
	}, parked, release
}

// Drain setup, held inside the mutation (plan row 9, codex01 r12 P2):
// A's admitted write is held; B's begin waits for it; B then writes the
// target block and stays installing; a further A attempt is refused with
// B's block byte-identical. Mutants: begin taking installMu shared (A's
// held write lands over B's) and no ID check (the further A write lands).
func TestBaseInstallV3HeldOldWriteDrainsBeforeANewBegin(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	hA := beginV3(t, s, 0, idA)
	hook, parked, release := holdOnce(t)
	s.v3.testInstallAdmitted = hook
	aDone := make(chan error, 1)
	go func() { aDone <- hA.WriteBase(7, blk(0xAA)) }()
	<-parked
	var hB *installHandleV3
	bDone := make(chan error, 1)
	go func() {
		var err error
		if hB, err = s.beginBaseInstallV3(0, idB); err == nil {
			err = hB.WriteBase(7, blk(0xBB))
		}
		bDone <- err
	}()
	// B must not get past its begin while A's write is held; give a wrong
	// (shared) begin time to finish first. The oracle is the bytes below.
	select {
	case err := <-bDone:
		bDone <- err
	case <-time.After(300 * time.Millisecond):
	}
	release()
	if err := <-aDone; err != nil {
		t.Fatalf("A's admitted write: %v", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("B: %v", err)
	}
	if got := readV3(t, s, 7); !bytes.Equal(got, blk(0xBB)) {
		t.Fatalf("LBA 7 = %x..., want B's bytes", got[0])
	}
	if err := hA.WriteBase(7, blk(0xAA)); !errors.Is(err, ErrInstallSuperseded) {
		t.Fatalf("further A write: err = %v, want ErrInstallSuperseded", err)
	}
	if err := hA.ApplyInstallEntry(7, blk(0xAA), 1); !errors.Is(err, ErrInstallSuperseded) {
		t.Fatalf("further A apply: err = %v, want ErrInstallSuperseded", err)
	}
	if got := readV3(t, s, 7); !bytes.Equal(got, blk(0xBB)) {
		t.Fatalf("LBA 7 = %x... after the stale attempts, want B's bytes", got[0])
	}
	if d := s.v3.pub.current(); d.BaseState != BaseInstalling || d.BaseInstallID != idB {
		t.Fatalf("state %d id %x, want installing under B", d.BaseState, d.BaseInstallID[0])
	}
}

// Premature-check setup, parked before admission (codex03 r11, plan row
// 9): A parks before acquiring installMu; B supersedes, writes the target
// block and stays installing; A resumes and is refused under the lock.
// Mutants: checking the ID before acquiring installMu, and no ID check.
func TestBaseInstallV3ParkedOldFrameIsRefusedAfterASupersession(t *testing.T) {
	for _, op := range []string{"WriteBase", "ApplyInstallEntry"} {
		t.Run(op, func(t *testing.T) {
			s, _ := newStoreV3(t, WModeStrict)
			t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
			hA := beginV3(t, s, 0, idA)
			hook, parked, release := holdOnce(t)
			s.v3.testInstallBeforeAdmit = hook
			aDone := make(chan error, 1)
			go func() {
				if op == "WriteBase" {
					aDone <- hA.WriteBase(7, blk(0xAA))
				} else {
					aDone <- hA.ApplyInstallEntry(7, blk(0xAA), 1)
				}
			}()
			<-parked
			hB := beginV3(t, s, 0, idB)
			if err := hB.WriteBase(7, blk(0xBB)); err != nil {
				t.Fatal(err)
			}
			release()
			if err := <-aDone; !errors.Is(err, ErrInstallSuperseded) {
				t.Fatalf("parked A %s: err = %v, want ErrInstallSuperseded", op, err)
			}
			if got := readV3(t, s, 7); !bytes.Equal(got, blk(0xBB)) {
				t.Fatalf("LBA 7 = %x..., want B's bytes", got[0])
			}
			if d := s.v3.pub.current(); d.BaseState != BaseInstalling || d.BaseInstallID != idB {
				t.Fatalf("state %d id %x, want installing under B", d.BaseState, d.BaseInstallID[0])
			}
		})
	}
}

// codex01 row 9a P1-1: W 1 with unacknowledged LSNs 2 and 3 appended. A
// begin at BaseLSN 1 would put 2 and 3 behind BaseCursor while the
// allocator continues at 4: after complete, an acknowledged write 4 and a
// reopen from F = 1 meet the hole (strict: R 1 < W 4; lagged: 4 dropped).
// The begin is refused, writing nothing; a begin covering the appended
// LSNs installs, and the acknowledged write after it survives a crash.
// Mutant: bounding BaseLSN by W and the checkpoint only fails here.
func TestBaseInstallV3BeginRefusesBelowTheHighestAppendedLSN(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	writeSynced(t, s, 1, 1)
	for lba := uint32(2); lba <= 3; lba++ {
		if _, err := s.Write(lba, blk(byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if w, a := s.v3.wack.Load(), s.v3.appended.Load(); w != 1 || a != 3 {
		t.Fatalf("setup: acknowledged %d appended %d, want 1 and 3", w, a)
	}
	writes := countSlotWritesV3(s)
	if _, err := s.beginBaseInstallV3(1, idA); !errors.Is(err, ErrInstallBaseUncovered) {
		t.Fatalf("begin at BaseLSN 1 below appended 3: err = %v, want ErrInstallBaseUncovered", err)
	}
	if writes.Load() != 0 {
		t.Fatal("a refused begin wrote a descriptor slot")
	}
	s.v3.testBeforeSlotWrite = nil
	h := beginV3(t, s, 3, idA)
	for lba := uint32(1); lba <= 3; lba++ {
		if err := h.WriteBase(lba, blk(0x10+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.markBaseCompleteV3(idA); err != nil {
		t.Fatal(err)
	}
	lsn, err := s.Write(4, blk(4))
	if err != nil || lsn != 4 {
		t.Fatalf("write after complete: LSN %d err %v, want 4", lsn, err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	for lba, want := range map[uint32]byte{1: 0x11, 2: 0x12, 3: 0x13, 4: 4} {
		if got := readV3(t, r, lba); !bytes.Equal(got, blk(want)) {
			t.Fatalf("after reopen LBA %d = %x..., want %x", lba, got[0], want)
		}
	}
}

// codex01 row 9a P1-2: a BASE write makes its chunk's coverage UNKNOWN only
// in memory. Complete settles the map like a checkpoint: the installed
// publication carries a fresh root with that UNKNOWN, so a crash and a
// mapped reopen keep it. Setup: chunk 2 is clean in the durable root (as a
// capture would leave it; set white-box, then checkpointed). Mutant: a
// complete that publishes installed without the map candidate reopens with
// chunk 2 clean and fails here.
func TestBaseInstallV3CompletePersistsTheMapUnknownOfBaseWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapped.v3")
	s, err := createMappedWALStoreV3ForTest(path, 64, v3TestBlock, v3TestWAL, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	writeSynced(t, s, 1, 1)
	s.mu.Lock()
	s.v3.mapEntries[2] = chunkLSNEntryV3{}
	s.mu.Unlock()
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if e := s.v3.pub.mapCurrent.entries[2]; e.Unknown {
		t.Fatal("setup: chunk 2 is not clean in the durable root")
	}
	h := beginV3(t, s, 1, idA)
	if err := h.WriteBase(9, blk(9)); err != nil { // LBA 9 is in chunk 2
		t.Fatal(err)
	}
	if err := s.markBaseCompleteV3(idA); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	if st := r.v3.pub.current().BaseState; st != BaseInstalled {
		t.Fatalf("reopen state %d, want installed", st)
	}
	if e := r.v3.pub.mapCurrent.entries[2]; !e.Unknown {
		t.Fatal("chunk 2 reopened clean: the BASE write's UNKNOWN was lost at complete")
	}
}

// An attached layer without the transition protocol still refuses, writing
// nothing. Actual supported layers are covered by the chunk/provider controls.
func TestBaseInstallV3BeginRefusesWithAnUnsupportedAttachedChunkLayer(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	cfg.BarrierBound = 1
	mustAttach(t, s, newFakeChunkRemote(), cfg)
	hooks := *s.chunk.Load()
	hooks.baseTransitionV3 = nil
	s.chunk.Store(&hooks)
	a0, b0 := slotsV3(t, path)
	if _, err := s.beginBaseInstallV3(0, idA); !errors.Is(err, errV3NotYet) {
		t.Fatalf("begin with a chunk layer attached: err = %v, want errV3NotYet", err)
	}
	if a, b := slotsV3(t, path); a != a0 || b != b0 {
		t.Fatal("a refused begin wrote a descriptor slot")
	}
}

// codex01 row 9b P1, store part: a completed handle keeps admitting
// install-lane applies (the session's WAL lane outlives its BaseDone) while
// installed and after its retirement, until the next begin; a BASE write
// after complete is refused; once a later begin has run (here after the
// retirement, so the ID check alone cannot tell), the old handle is
// refused with ErrInstallSuperseded. Mutants: admitting only while
// installing (the apply after complete fails) and dropping the epoch check
// (the stale apply lands in the new install).
func TestBaseInstallV3CompletedHandleAppliesUntilTheNextBegin(t *testing.T) {
	s, _, _ := checkpointedStoreV3(t, 2)
	h := beginV3(t, s, 4, idA)
	if err := h.WriteBase(3, blk(3)); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(5, blk(5), 5); err != nil {
		t.Fatalf("apply after complete (installed): %v", err)
	}
	if err := h.WriteBase(6, blk(6)); !errors.Is(err, ErrInstallSuperseded) {
		t.Fatalf("WriteBase after complete: err = %v, want ErrInstallSuperseded", err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	if st := s.v3.pub.current().BaseState; st != BaseNone {
		t.Fatalf("setup: state %d, want retired", st)
	}
	if err := h.ApplyInstallEntry(6, blk(6), 6); err != nil {
		t.Fatalf("apply after retirement: %v", err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	hB := beginV3(t, s, 6, idB)
	if err := h.ApplyInstallEntry(7, blk(0xAA), 7); !errors.Is(err, ErrInstallSuperseded) {
		t.Fatalf("old handle after a later begin: err = %v, want ErrInstallSuperseded", err)
	}
	for lba := uint32(5); lba <= 6; lba++ { // B's BASE image at 6
		if err := hB.WriteBase(lba, blk(byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if err := hB.ApplyInstallEntry(7, blk(7), 7); err != nil {
		t.Fatal(err)
	}
	// B completes and retires too: the state is none again, which the old
	// completed handle would accept without the epoch check.
	if err := hB.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	if st := s.v3.pub.current().BaseState; st != BaseNone {
		t.Fatalf("setup: state %d after B, want retired", st)
	}
	if err := h.ApplyInstallEntry(8, blk(0xAA), 8); !errors.Is(err, ErrInstallSuperseded) {
		t.Fatalf("old handle after a later install retired: err = %v, want ErrInstallSuperseded", err)
	}
	readAllV3(t, s, 5, 7)
}

// The retirement gate (row 12a retired-BASE ruling): a deferred store keeps
// a completed BASE installed through publications whose 8.4 conditions hold,
// and retires on the first publication after AllowBaseRetirementV3. A store
// that never set the gate retires as before (TestBaseInstallV3Retires...).
// Mutant: a publisher that ignores the gate retires early and fails here.
func TestBaseInstallV3RetirementWaitsForTheGate(t *testing.T) {
	s, _, _ := checkpointedStoreV3(t, 2)
	s.DeferBaseRetirementV3()
	h := beginV3(t, s, 4, idA)
	if err := h.WriteBase(3, blk(3)); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(5, blk(5), 5); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if d := s.v3.pub.current(); d.CheckpointLSN < 4 || d.BaseState != BaseInstalled {
		t.Fatalf("deferred: checkpoint %d state %d, want installed with the 8.4 conditions holding", d.CheckpointLSN, d.BaseState)
	}
	s.AllowBaseRetirementV3()
	if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	if d := s.v3.pub.current(); d.BaseState != BaseNone {
		t.Fatalf("after Allow: state %d, want retired", d.BaseState)
	}
	// A second install in the same open is deferred again: the earlier
	// Allow covered the first image only. Mutant: a begin that does not
	// re-arm the gate retires the second install early and fails here.
	h2 := beginV3(t, s, 6, idB)
	if err := h2.WriteBase(3, blk(0x33)); err != nil {
		t.Fatal(err)
	}
	if err := h2.ApplyInstallEntry(5, blk(0x55), 7); err != nil {
		t.Fatal(err)
	}
	if err := h2.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if d := s.v3.pub.current(); d.CheckpointLSN < 6 || d.BaseState != BaseInstalled || d.BaseInstallID != idB {
		t.Fatalf("second install: checkpoint %d state %d, want installed under B (re-armed)", d.CheckpointLSN, d.BaseState)
	}
}

// CommitChunkAttachV3 (row 12a retirement timing): the provider's commit
// runs under installMu exclusive; a failing commit changes nothing (no
// durable flag, the deferral holds, the volume stays installed); a
// successful one records the durable attach and lifts the deferral of an
// installed volume, which then retires on the next publication; Defer
// resets the flag at the next open. Mutants: lifting on a failed commit,
// not recording the flag, running the commit outside installMu.
func TestBaseInstallV3CommitChunkAttach(t *testing.T) {
	s, _, _ := checkpointedStoreV3(t, 2)
	s.DeferBaseRetirementV3()
	h := beginV3(t, s, 4, idA)
	if err := h.WriteBase(3, blk(3)); err != nil {
		t.Fatal(err)
	}
	if err := h.ApplyInstallEntry(5, blk(5), 5); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	exclusive := false
	err := s.CommitChunkAttachV3(func() error {
		if s.v3.installMu.TryRLock() {
			s.v3.installMu.RUnlock()
		} else {
			exclusive = true
		}
		return errors.New("injected marker fsync failure")
	})
	if err == nil || !exclusive {
		t.Fatalf("failing commit: err %v, ran under installMu exclusive %v", err, exclusive)
	}
	if s.OwnerChunkAttachDurableV3() {
		t.Fatal("a failed commit recorded a durable attach")
	}
	if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	if st := s.v3.pub.current().BaseState; st != BaseInstalled {
		t.Fatalf("state %d after a failed commit, want installed", st)
	}
	if err := s.CommitChunkAttachV3(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !s.OwnerChunkAttachDurableV3() {
		t.Fatal("a successful commit did not record the durable attach")
	}
	if _, err := s.v3.pub.publish(&s.mu, nil, nil); err != nil {
		t.Fatal(err)
	}
	if st := s.v3.pub.current().BaseState; st != BaseNone {
		t.Fatalf("state %d after the commit, want retired", st)
	}
	s.DeferBaseRetirementV3()
	if s.OwnerChunkAttachDurableV3() {
		t.Fatal("Defer (the next open) did not reset the durable attach flag")
	}
}
