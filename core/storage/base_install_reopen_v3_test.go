package storage

import (
	"bytes"
	"errors"
	"testing"
)

// Install-reopen controls:
// a covering superseding begin after an installing reopen.

// installThenCrashV3 begins install A at BaseLSN 10 over two acknowledged
// guest writes, applies install-lane LSNs 11, 12, 13 at LBAs 1, 2, 3
// (one block each), optionally acknowledges them through the install log,
// crashes, damages record 12 and reopens. Recover reports the installing
// outcome.
func installThenCrashV3(t *testing.T, ackLane bool) (*WALStore, string, error) {
	t.Helper()
	s, path := newStoreV3(t, WModeStrict)
	writeSynced(t, s, 1, 2)
	h := beginV3(t, s, 10, idA)
	for lsn := uint64(11); lsn <= 13; lsn++ {
		if err := h.ApplyInstallEntry(uint32(lsn-10), blk(byte(lsn)), lsn); err != nil {
			t.Fatal(err)
		}
	}
	if ackLane {
		if H, err := s.SyncInstallLog(idA); err != nil || H != 13 {
			t.Fatalf("SyncInstallLog = (%d, %v)", H, err)
		}
	}
	crashRecoveryHardeningStore(t, s)
	damageRecord(t, path, 12)
	r, err := openUnmappedWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	_, rerr := r.Recover()
	return r, path, rerr
}

func reopenServingV3(t *testing.T, s *WALStore, path string) *WALStore {
	t.Helper()
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	return r
}

// G3 (DESIGN 6.6): the reopen keeps LSN 11 (E is past it) and drops 12;
// record 13 survives beyond the gap. Superseding B at BaseLSN 11 writes its
// LSN 12 exactly where A's 12 was, so without the fence A's 13 would chain
// on after it and be served after B completes. The fence zeroes [E, ...)
// before B's installing publication. Mutant: a supersession without the
// fence serves A's LBA 3 bytes and fails here.
func TestBaseInstallV3SupersessionAfterReopenFencesTheOldSuffix(t *testing.T) {
	r, path, err := installThenCrashV3(t, false)
	if !errors.Is(err, ErrBaseIncomplete) || errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("reopen: err = %v, want an intact ErrBaseIncomplete (W 2 <= R 11)", err)
	}
	hB := beginV3(t, r, 11, idB)
	if err := hB.WriteBase(1, blk(0xB1)); err != nil {
		t.Fatal(err)
	}
	if err := hB.ApplyInstallEntry(2, blk(0xB2), 12); err != nil {
		t.Fatal(err)
	}
	if err := hB.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	q := reopenServingV3(t, r, path)
	if got := readV3(t, q, 3); bytes.Equal(got, blk(13)) {
		t.Fatal("A's LSN 13 resurrected after the supersession (no fence)")
	}
	for lba, want := range map[uint32]byte{1: 0xB1, 2: 0xB2} {
		if got := readV3(t, q, lba); !bytes.Equal(got, blk(want)) {
			t.Fatalf("LBA %d = %x..., want B's %x", lba, got[0], want)
		}
	}
}

// A covering supersession over a DAMAGED suffix (DESIGN 8.3, r15): A's lane
// was acknowledged through 13 (W 13), record 12 is damaged, so R 11 < W* 13
// and resume is refused. A supersession below W is refused writing
// nothing; one at BaseLSN >= W fences, publishes installing into both
// slots, completes and serves. Mutant: applying the trusted-through-W check
// to the supersession leaves the volume stuck and fails here.
func TestBaseInstallV3CoveringSupersessionOverADamagedSuffix(t *testing.T) {
	r, path, err := installThenCrashV3(t, true)
	if !errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("reopen: err = %v, want ErrInstallSuffixDamaged (R 11 < W* 13)", err)
	}
	a0, b0 := slotsV3(t, path)
	if _, err := r.beginBaseInstallV3(12, idB); !errors.Is(err, ErrInstallSupersedeUncovered) {
		t.Fatalf("supersession below W: err = %v, want ErrInstallSupersedeUncovered", err)
	}
	if a, b := slotsV3(t, path); a != a0 || b != b0 {
		t.Fatal("a refused supersession wrote a slot")
	}
	hB := beginV3(t, r, 13, idB)
	a, b := slotsV3(t, path)
	for i, d := range []descriptorV3{a, b} {
		if d.BaseState != BaseInstalling || d.BaseInstallID != idB || d.BaseLSN != 13 {
			t.Fatalf("slot %d after the supersession: state %d BaseLSN %d", i, d.BaseState, d.BaseLSN)
		}
	}
	for lba := uint32(1); lba <= 3; lba++ {
		if err := hB.WriteBase(lba, blk(0xB0+byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if err := hB.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	q := reopenServingV3(t, r, path)
	for lba := uint32(1); lba <= 3; lba++ {
		if got := readV3(t, q, lba); !bytes.Equal(got, blk(0xB0+byte(lba))) {
			t.Fatalf("LBA %d = %x..., want B's BASE", lba, got[0])
		}
	}
}
