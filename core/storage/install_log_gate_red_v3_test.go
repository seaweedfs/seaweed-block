package storage

import (
	"testing"
)

// RED (plan row 10a) on a3eed8a8: the v3 logical Sync is not gated while
// installing. Plan row 10a controls (DESIGN r17.4 section 7; owner claude03): the gated
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
	if f, err := s.Sync(); f != 0 || err == nil { // ErrBaseInstalling from row 10a
		t.Fatalf("Sync while installing = (%d, %v), want (0, an install-gate error)", f, err)
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

