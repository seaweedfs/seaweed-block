package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

// retainedStoreV3 writes and checkpoints n blocks with the given retention
// window on a WAL big enough to hold it (1 MiB: the cap is 64 LSNs).
func retainedStoreV3(t *testing.T, retention uint64, n uint32) *WALStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.v3")
	s, err := createWALStoreV3ForTest(path, 64, v3TestBlock, 1<<20, WModeLagged)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	attachMapV3(t, s, path)
	s.SetRecoveryRetentionLSNs(retention)
	writeSynced(t, s, 1, n)
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if d := s.v3.pub.current(); d.CheckpointLSN != uint64(n) {
		t.Fatalf("setup: checkpoint %d, want %d", d.CheckpointLSN, n)
	}
	return s
}

// scanAccepts reports whether a catch-up from fromLSN ("entries after
// fromLSN") is served, or refused as recycled.
func scanAccepts(t *testing.T, s *WALStore, fromLSN uint64) bool {
	t.Helper()
	err := s.ScanLBAs(fromLSN, func(RecoveryEntry) error { return nil })
	if err == nil {
		return true
	}
	if !errors.Is(err, ErrWALRecycled) {
		t.Fatalf("scan from %d: %v", fromLSN, err)
	}
	return false
}

// The probe's decision and the catch-up scan read one floor: a replica
// whose R is below S is sent to a rebuild exactly when the scan would
// refuse R, and a replica at S is served by the scan. Before this, S was
// checkpoint+1 whatever the retention, so a replica inside the retained
// window was sent to a rebuild while the scan could still serve it.
func TestTheProbeBoundaryIsTheCatchUpScanFloor(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retention uint64
		wantS     uint64
	}{
		{"no retention: the checkpoint tail, as before", 0, 41},
		{"retention 10: ten LSNs below the checkpoint stay catch-up", 10, 31},
		{"retention past the WAL cap: capped at 64", 1 << 40, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := retainedStoreV3(t, tc.retention, 40)
			_, S, H := s.Boundaries()
			if S != tc.wantS {
				t.Fatalf("S = %d (H %d), want %d", S, H, tc.wantS)
			}
			if S > 1 {
				if scanAccepts(t, s, S-1) {
					t.Fatalf("R = S-1 = %d: the probe says rebuild, but the scan serves it", S-1)
				}
			}
			if !scanAccepts(t, s, S) {
				t.Fatalf("R = S = %d: the probe says catch-up, but the scan refuses it", S)
			}
		})
	}
}

// The window never holds more than a quarter of the WAL in block-sized
// records, whatever is configured, so retention cannot fill the WAL.
func TestRetentionIsCappedByTheWAL(t *testing.T) {
	s := retainedStoreV3(t, 1<<40, 40)
	s.mu.RLock()
	got := s.catchUpRetentionLocked(s.v3.pub.current())
	s.mu.RUnlock()
	if want := uint64(1<<20) / (4 * v3TestBlock); got != want {
		t.Fatalf("effective retention %d, want %d", got, want)
	}
}
