package storage

import (
	"bytes"
	"errors"
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

// holdOnce returns a hook that parks the first call until release is
// closed, and reports through parked when it has parked.
func holdOnce() (hook func(), parked, release chan struct{}) {
	parked, release = make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	return func() {
		if armed.CompareAndSwap(true, false) {
			close(parked)
			<-release
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
	hook, parked, release := holdOnce()
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
	close(release)
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
			hook, parked, release := holdOnce()
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
			close(release)
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
