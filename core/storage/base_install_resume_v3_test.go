package storage

import (
	"bytes"
	"errors"
	"testing"
)

// Plan row 11b controls (DESIGN r17.4 sections 6.6 and 8.3; owner claude03):
// the same-ID resume after an installing reopen.

// An intact reopen (W 2 <= R 11; record 12 damaged, 13 beyond the gap)
// resumes the same ID: the recovered LSN 11 is kept and served after
// completion, the resumed session's LSN 12 lands at E, and the old 13 does
// not re-chain behind it (the fence). Mutant: a resume without the fence
// serves the old LSN 13 at LBA 3 and fails here.
func TestBaseInstallV3ResumeAfterReopenFencesAndServesTheSuffix(t *testing.T) {
	r, path, err := installThenCrashV3(t, false)
	if !errors.Is(err, ErrBaseIncomplete) || errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("reopen: err = %v, want an intact ErrBaseIncomplete", err)
	}
	h, err := r.resumeBaseInstallV3(idA)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if d := r.v3.pub.current(); d.BaseState != BaseInstalling || d.BaseInstallID != idA {
		t.Fatalf("after resume: state %d", d.BaseState)
	}
	if err := h.ApplyInstallEntry(2, blk(0x99), 12); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	q := reopenServingV3(t, r, path)
	if got := readV3(t, q, 3); bytes.Equal(got, blk(13)) {
		t.Fatal("the dropped LSN 13 resurrected after the resume (no fence)")
	}
	for lba, want := range map[uint32]byte{1: 11, 2: 0x99} {
		if got := readV3(t, q, lba); !bytes.Equal(got, blk(want)) {
			t.Fatalf("LBA %d = %x..., want %x", lba, got[0], want)
		}
	}
}

// A damaged suffix (W* 13 > R 11) refuses the resume with
// ErrInstallSuffixDamaged, writing nothing; the volume stays installing.
// Mutant: a resume that ignores the damage fails here.
func TestBaseInstallV3ResumeRefusesADamagedSuffix(t *testing.T) {
	r, path, err := installThenCrashV3(t, true)
	if !errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("reopen: err = %v, want ErrInstallSuffixDamaged", err)
	}
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	a0, b0 := slotsV3(t, path)
	if _, err := r.resumeBaseInstallV3(idA); !errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("resume over a damaged suffix: err = %v, want ErrInstallSuffixDamaged", err)
	}
	if a, b := slotsV3(t, path); a != a0 || b != b0 {
		t.Fatal("a refused resume wrote a slot")
	}
	if st := r.v3.pub.current().BaseState; st != BaseInstalling {
		t.Fatalf("state %d, want installing", st)
	}
}

// E below the persisted retainedHead (codex03 r17 check): the head was
// published past LSN 13 (white-box, as an earlier same-open recovery
// would), then 13 is damaged, so the reopen's E ends after 12, below the
// head. The resume is refused (E >= head is part of trusted-through-W). A
// covering supersession anchors at P = the persisted head; a crash right
// after it reopens with E = P = head and resumes. Mutants: a resume
// without the E >= head check, and a supersession anchored at P = E (its
// reopen then has E below the head and cannot resume), fail here.
func TestBaseInstallV3EBelowThePersistedHead(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	writeSynced(t, s, 1, 2)
	h := beginV3(t, s, 10, idA)
	for lsn := uint64(11); lsn <= 13; lsn++ {
		if err := h.ApplyInstallEntry(uint32(lsn-10), blk(byte(lsn)), lsn); err != nil {
			t.Fatal(err)
		}
	}
	head := s.v3.w.logicalHeadV3()
	if _, err := s.v3.pub.publish(&s.mu, nil, func(n *descriptorV3) error { n.RetainedHead = head; return nil }); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	damageRecord(t, path, 13)
	r, err := openUnmappedWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	if _, err := r.Recover(); !errors.Is(err, ErrBaseIncomplete) || errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("reopen: err = %v, want an intact ErrBaseIncomplete (R 12 >= W* 2)", err)
	}
	if ro := r.v3.installReopen; ro == nil || ro.E >= head {
		t.Fatalf("setup: E %v not below the persisted head %d", ro, head)
	}
	if _, err := r.resumeBaseInstallV3(idA); !errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("resume with E below the persisted head: err = %v, want ErrInstallSuffixDamaged", err)
	}
	beginV3(t, r, 12, idB)
	if d := r.v3.pub.current(); d.BaseCursor != head || d.RetainedHead != head {
		t.Fatalf("supersession anchored at %d (head %d), want P = the persisted head %d", d.BaseCursor, d.RetainedHead, head)
	}
	crashRecoveryHardeningStore(t, r)
	q, err := openUnmappedWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	q.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, q) })
	if _, err := q.Recover(); !errors.Is(err, ErrBaseIncomplete) || errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("reopen after the supersession: err = %v, want an intact ErrBaseIncomplete", err)
	}
	if _, err := q.resumeBaseInstallV3(idB); err != nil {
		t.Fatalf("resume of the covering install: %v", err)
	}
}
