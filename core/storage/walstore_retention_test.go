package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

// G6 §1.A α + §2 substrate retention-gate tests:
//
// Pre-G6: walstore's recovery scan rejects fromLSN <= checkpointLSN
// (strict gate; G5-5C scenario D evidence).
//
// G6: WALRetentionLSNs widens the window so fromLSN > (checkpointLSN
// - retention) is still scannable. This buys a slow replica room to
// catch up before the engine has to escalate to rebuild.
//
// Pinned by:
//   - INV-G6-CATCHUP-CONVERGES-WITHIN-RETENTION (positive: within window
//     scan succeeds)
//   - INV-G6-RETENTION-POLICY-OPERATOR-VISIBLE (the knob is a real
//     operator-tunable plumbed from --wal-retention-lsns)

// freshWALStore creates a walstore in a temp dir and returns it.
// Uses small block geometry — the tests exercise scan-gate semantics,
// not block-IO performance.
func freshWALStore(t *testing.T) *WALStore {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "v.bin")
	ws, err := CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatalf("CreateWALStore: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// advanceCheckpointForTest writes data to grow nextLSN, then forces
// the in-memory checkpointLSN to a chosen value so the test can
// exercise the retention gate at a known boundary. Test-only path —
// production advances checkpointLSN via the flusher.
func advanceCheckpointForTest(t *testing.T, ws *WALStore, lsn uint64) {
	t.Helper()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if lsn >= ws.nextLSN {
		ws.nextLSN = lsn + 1
	}
	ws.checkpointLSN = lsn
}

// fakeRecoveryEntry is a tiny consumer for ScanLBAs whose only
// purpose is to count callbacks. The retention-gate tests don't
// care about the entries themselves — only whether the scan
// returns or rejects.
type recoveryEntryCounter struct {
	count int
}

func (c *recoveryEntryCounter) onEntry(RecoveryEntry) error {
	c.count++
	return nil
}

// TestWALStore_RecoveryRetention_Default_StrictRecycle pins pre-G6
// behavior under the new code: when WALRetentionLSNs is 0 (default),
// the gate behaves exactly as before — fromLSN <= checkpointLSN is
// rejected with WALRecycled.
func TestWALStore_RecoveryRetention_Default_StrictRecycle(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 100)

	if got := ws.RecoveryRetentionLSNs(); got != 0 {
		t.Fatalf("default retention = %d, want 0", got)
	}

	// fromLSN = 50 (well under checkpoint=100) must be recycled.
	c := &recoveryEntryCounter{}
	err := ws.ScanLBAs(50, c.onEntry)
	if err == nil {
		t.Fatal("fromLSN=50 with checkpoint=100 + retention=0 should be recycled")
	}
	if !errors.Is(err, ErrWALRecycled) {
		t.Errorf("expected ErrWALRecycled (or unwrap chain match), got %v", err)
	}

	// fromLSN = 100 is at-or-below checkpoint → still recycled
	// (boundary preserved: <= is rejected, > is accepted).
	err = ws.ScanLBAs(100, c.onEntry)
	if !errors.Is(err, ErrWALRecycled) {
		t.Errorf("fromLSN=100 (== checkpoint) with retention=0 should be recycled, got %v", err)
	}
}

// TestWALStore_RecoveryRetention_NonZero_WidensWindow verifies the
// G6 α path: setting retention=N accepts fromLSN as low as
// checkpointLSN - N (exclusive: at floor still rejected; above floor
// accepted).
func TestWALStore_RecoveryRetention_NonZero_WidensWindow(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 100)
	ws.SetRecoveryRetentionLSNs(20)
	if got := ws.RecoveryRetentionLSNs(); got != 20 {
		t.Fatalf("retention after Set: got %d, want 20", got)
	}

	// floor = 100 - 20 = 80; fromLSN > 80 is accepted, fromLSN <= 80
	// is rejected.
	c := &recoveryEntryCounter{}

	// fromLSN = 90: above floor → accepted (no error). The scan
	// itself may produce no entries because we haven't written
	// anything in the window; that's fine — this test only pins the
	// gate decision, not scan content.
	if err := ws.ScanLBAs(90, c.onEntry); err != nil {
		t.Errorf("fromLSN=90 with checkpoint=100 + retention=20 should be accepted, got %v", err)
	}

	// fromLSN = 81: above floor (80) → accepted.
	if err := ws.ScanLBAs(81, c.onEntry); err != nil {
		t.Errorf("fromLSN=81 with checkpoint=100 + retention=20 should be accepted (floor=80), got %v", err)
	}

	// fromLSN = 80: at floor → rejected (<= floor recycles).
	err := ws.ScanLBAs(80, c.onEntry)
	if !errors.Is(err, ErrWALRecycled) {
		t.Errorf("fromLSN=80 (==floor) should be recycled, got %v", err)
	}

	// fromLSN = 50: well below floor → rejected.
	err = ws.ScanLBAs(50, c.onEntry)
	if !errors.Is(err, ErrWALRecycled) {
		t.Errorf("fromLSN=50 (<floor) should be recycled, got %v", err)
	}
}

// TestWALStore_RecoveryRetention_LargerThanCheckpoint_ClampsToZero
// verifies the saturating-subtraction guard: if retention >=
// checkpoint, the floor saturates to 0 and any fromLSN > 0 is
// accepted.
func TestWALStore_RecoveryRetention_LargerThanCheckpoint_ClampsToZero(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 100)
	ws.SetRecoveryRetentionLSNs(1_000_000) // way larger than checkpoint

	c := &recoveryEntryCounter{}
	// fromLSN = 1: floor saturates to 0; 1 > 0 → accepted.
	if err := ws.ScanLBAs(1, c.onEntry); err != nil {
		t.Errorf("fromLSN=1 with retention >> checkpoint should be accepted (floor saturates to 0), got %v", err)
	}
}

// TestWALStore_RecoveryRetention_TypedFailureKind verifies that the
// recycle-rejection error is the typed RecoveryFailure envelope (so
// the transport layer can extract via errors.As). This pins the
// substrate side of INV-G6-WALRECYCLE-DISPATCHES-REBUILD: if the
// substrate returned a bare sentinel here, transport's
// classifyRecoveryFailure would fall through to RecoveryFailureTransport
// and the engine wouldn't escalate to rebuild.
func TestWALStore_RecoveryRetention_TypedFailureKind(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 100)
	ws.SetRecoveryRetentionLSNs(20)

	c := &recoveryEntryCounter{}
	err := ws.ScanLBAs(50, c.onEntry) // below floor 80 → rejected
	if err == nil {
		t.Fatal("expected recycle error")
	}

	var rf *RecoveryFailure
	if !errors.As(err, &rf) {
		t.Fatalf("expected typed *RecoveryFailure (so transport can map kind via errors.As), got %T (%v)", err, err)
	}
	if rf.Kind != StorageRecoveryFailureWALRecycled {
		t.Errorf("RecoveryFailure.Kind = %v, want StorageRecoveryFailureWALRecycled", rf.Kind)
	}
	// errors.Is chain still works for legacy callers.
	if !errors.Is(err, ErrWALRecycled) {
		t.Error("errors.Is(err, ErrWALRecycled) should still hold via Unwrap chain")
	}
}
