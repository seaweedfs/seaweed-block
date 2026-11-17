package storage

import (
	"errors"
	"testing"
)

// Plan row 10a controls (DESIGN r17.4 section 7; owner claude03): the gated
// logical Sync and the receiver-only install-log ack.

// While installing, Sync returns exactly (0, ErrBaseInstalling) and writes
// no slot; after complete it acknowledges again. Mutant: an ungated Sync
// acknowledges the install lane through the frontend path and fails here.
func TestInstallLogV3SyncIsGatedWhileInstalling(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 1)
	h := beginV3(t, s, 1, idA)
	if err := h.ApplyInstallEntry(2, blk(2), 2); err != nil {
		t.Fatal(err)
	}
	writes := countSlotWritesV3(s)
	if f, err := s.Sync(); f != 0 || !errors.Is(err, ErrBaseInstalling) {
		t.Fatalf("Sync while installing = (%d, %v), want (0, ErrBaseInstalling)", f, err)
	}
	if writes.Load() != 0 || s.v3.pub.current().W != 1 {
		t.Fatalf("gated Sync wrote %d slots, W %d", writes.Load(), s.v3.pub.current().W)
	}
	s.v3.testBeforeSlotWrite = nil
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	if f, err := s.Sync(); err != nil || f < 2 {
		t.Fatalf("Sync after complete = (%d, %v), want >= 2", f, err)
	}
}

// SyncInstallLog refuses outside an install and for another ID, writing
// nothing. Mutant: dropping the ID check acknowledges another install.
func TestInstallLogV3Refusals(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writes := countSlotWritesV3(s)
	if _, err := s.SyncInstallLog(idA); !errors.Is(err, ErrNotInstalling) {
		t.Fatalf("outside an install: err = %v, want ErrNotInstalling", err)
	}
	s.v3.testBeforeSlotWrite = nil
	beginV3(t, s, 0, idA)
	writes = countSlotWritesV3(s)
	if _, err := s.SyncInstallLog(idB); !errors.Is(err, ErrInstallIDMismatch) {
		t.Fatalf("another ID: err = %v, want ErrInstallIDMismatch", err)
	}
	if writes.Load() != 0 {
		t.Fatal("a refused install-log sync wrote a slot")
	}
}

// The ack is mode A whatever the WMode: on a lagged (mode C) store the slot
// carries W = H as soon as SyncInstallLog returns, with BaseState still
// installing, and a crash keeps that W and the install. Mutant: the ack
// without its publication (the lagged mode's deferred W) fails here.
func TestInstallLogV3IsModeAAndSurvivesACrash(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	h := beginV3(t, s, 0, idA)
	for lsn := uint64(1); lsn <= 3; lsn++ {
		if err := h.ApplyInstallEntry(uint32(lsn), blk(byte(lsn)), lsn); err != nil {
			t.Fatal(err)
		}
	}
	H, err := s.SyncInstallLog(idA)
	if err != nil || H != 3 {
		t.Fatalf("SyncInstallLog = (%d, %v), want 3", H, err)
	}
	crashRecoveryHardeningStore(t, s)
	a, b := slotsV3(t, path)
	newest := a
	if b.SlotGen > a.SlotGen {
		newest = b
	}
	if newest.W != 3 || newest.BaseState != BaseInstalling || newest.BaseInstallID != idA {
		t.Fatalf("newest slot after the ack: W %d state %d, want W 3 installing under A", newest.W, newest.BaseState)
	}
	r, err := openUnmappedWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	if _, err := r.Recover(); !errors.Is(err, ErrBaseIncomplete) || errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("reopen: err = %v, want an intact ErrBaseIncomplete", err)
	}
}

// codex01 row 10a P1 (the #172 ack boundary): an append of the same install
// that fails and latches the store after SyncInstallLog's publication and
// before its ack refuses the ack: no H, no volatile acknowledged frontier.
// The held hook runs that append with a failing pwrite (the real latch). Mutant: no
// final locked sticky check returns H and fails here.
func TestInstallLogV3AFaultLatchedBeforeTheAckRefusesIt(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	h := beginV3(t, s, 0, idA)
	if err := h.ApplyInstallEntry(1, blk(1), 1); err != nil {
		t.Fatal(err)
	}
	var appendErr error
	s.v3.testInstallLogBeforeAck = func() {
		// The queued same-ID append runs here, and its pwrite fails.
		s.v3.w.writeAt = func([]byte, int64) (int, error) { return 0, errors.New("injected pwrite failure") }
		appendErr = h.ApplyInstallEntry(2, blk(2), 2)
	}
	wack := s.v3.wack.Load()
	H, err := s.SyncInstallLog(idA)
	if err == nil || H != 0 {
		t.Fatalf("SyncInstallLog across a latched fault = (%d, %v), want (0, the fault)", H, err)
	}
	if !errors.Is(appendErr, errWALPwrite) {
		t.Fatalf("setup: the held append returned %v, want a pwrite failure", appendErr)
	}
	if s.v3.wack.Load() != wack {
		t.Fatalf("acknowledged frontier moved to %d across the fault", s.v3.wack.Load())
	}
}
