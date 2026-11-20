package storage

import (
	"testing"
)

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
