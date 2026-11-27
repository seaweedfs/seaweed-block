package storage

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// Hypothesis (rc.2 write-stall F1/F2): a rebuild whose BASE completed but
// whose session then failed leaves the replica's store with an installed
// BASE that nothing retires while the replica receives no writes, so every
// later rebuild is refused with ErrRebuildRetainedBase until it is wiped.
//
// The store is built the production way (CreateWALStore, its own flusher)
// and driven through the public install API the replica uses
// (transport/replica.go: BeginInstall, WriteBase, ApplyInstallEntry,
// MarkBaseComplete). The failed session is simply nothing after
// MarkBaseComplete: no close, no barrier, no further write.
func TestARebuildAfterAFailedSessionIsRefusedByItsOwnBase(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "r.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	block := func(b byte) []byte {
		d := make([]byte, 4096)
		for i := range d {
			d[i] = b
		}
		return d
	}
	// The first rebuild: BASE at LSN 100, one install-lane entry, complete.
	h, err := BeginInstall(s, 100, [16]byte{1})
	if err != nil {
		t.Fatalf("first rebuild begin: %v", err)
	}
	for lba := uint32(0); lba < 4; lba++ {
		if err := h.WriteBase(lba, block(byte(lba+1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.ApplyInstallEntry(5, block(9), 101); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	// The session fails here. The replica is degraded: the primary retains
	// live writes for recovery and sends nothing. Give the store's own
	// background flusher time to run as it would on an idle replica.
	deadline := time.Now().Add(3 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		h2, err := BeginInstall(s, 120, [16]byte{2})
		if err == nil {
			_ = h2
			t.Logf("a later rebuild was admitted: the BASE retired on an idle store (hypothesis false)")
			return
		}
		last = err
		time.Sleep(250 * time.Millisecond)
	}
	if !errors.Is(last, ErrRebuildRetainedBase) {
		t.Fatalf("later rebuild refused with %v, want ErrRebuildRetainedBase or admission", last)
	}
	d := s.v3.pub.current()
	t.Fatalf("HYPOTHESIS HOLDS: after a completed BASE and a failed session, every later rebuild is refused for %s on an idle store: %v (BaseState=%d BaseLSN=%d CheckpointLSN=%d RetainedTail=%d BaseCursor=%d)",
		3*time.Second, last, d.BaseState, d.BaseLSN, d.CheckpointLSN, d.RetainedTail, d.BaseCursor)
}
