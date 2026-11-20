package storage

import (
	"errors"

	"testing"
)

// A failed fsync is sticky for the open store (the "fsyncgate" pattern: on
// Linux the kernel may already have dropped the dirty pages, so a later
// successful fsync proves nothing about them). After the first failure the
// store acknowledges no further Sync, accepts no further write and advances
// no checkpoint, with a typed SubstrateIO failure; only a reopen clears it.

var errInjectedFsync = errors.New("injected fsync failure")

func assertStickyFsync(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("err %v; want the sticky ErrFsyncFailed", err)
	}
	var rf *RecoveryFailure
	if !errors.As(err, &rf) || rf.Kind != StorageRecoveryFailureSubstrateIO {
		t.Fatalf("err %v; want a SubstrateIO RecoveryFailure (io_error)", err)
	}
}
