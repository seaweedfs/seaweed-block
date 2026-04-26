package storage

import (
	"errors"
	"testing"
)

// T4d-1 substrate-side typed RecoveryFailure tests.
//
// Per architect HIGH v0.1 #1 + v0.3 boundary fix: storage owns
// `StorageRecoveryFailureKind` + `RecoveryFailure`; transport
// extracts via errors.As and maps to engine kind. These tests pin
// the storage-side contract; engine-side tests pin the typed branch.

func TestStorageRecoveryFailure_WALRecycled_ErrorsIsCompatible(t *testing.T) {
	// Pre-T4d-1 callers used errors.Is(err, ErrWALRecycled). The new
	// typed envelope MUST preserve that via Unwrap so the migration
	// window doesn't break existing call sites.
	err := NewWALRecycledFailure(ErrWALRecycled, "fromLSN=5 checkpointLSN=10")
	if !errors.Is(err, ErrWALRecycled) {
		t.Fatal("RecoveryFailure must preserve errors.Is(err, ErrWALRecycled) compatibility")
	}
}

func TestStorageRecoveryFailure_WALRecycled_ErrorsAsExposesKind(t *testing.T) {
	// Transport-side mapping uses errors.As(err, &target) to pull
	// the typed kind out. Pin that path.
	err := NewWALRecycledFailure(ErrWALRecycled, "detail string")
	var rf *RecoveryFailure
	if !errors.As(err, &rf) {
		t.Fatal("errors.As must extract *RecoveryFailure")
	}
	if rf.Kind != StorageRecoveryFailureWALRecycled {
		t.Errorf("Kind = %v, want StorageRecoveryFailureWALRecycled", rf.Kind)
	}
	if rf.Detail != "detail string" {
		t.Errorf("Detail = %q, want 'detail string'", rf.Detail)
	}
}

func TestStorageRecoveryFailure_SubstrateIO_ErrorsAsExposesKind(t *testing.T) {
	cause := errors.New("read failed at offset 1024")
	err := NewSubstrateIOFailure(cause, "ring decode")
	var rf *RecoveryFailure
	if !errors.As(err, &rf) {
		t.Fatal("errors.As must extract *RecoveryFailure for SubstrateIO")
	}
	if rf.Kind != StorageRecoveryFailureSubstrateIO {
		t.Errorf("Kind = %v, want StorageRecoveryFailureSubstrateIO", rf.Kind)
	}
	if !errors.Is(err, cause) {
		t.Error("Unwrap must preserve cause chain")
	}
}

func TestStorageRecoveryFailureKind_StringForms(t *testing.T) {
	cases := []struct {
		k    StorageRecoveryFailureKind
		want string
	}{
		{StorageRecoveryFailureUnknown, "Unknown"},
		{StorageRecoveryFailureWALRecycled, "WALRecycled"},
		{StorageRecoveryFailureSubstrateIO, "SubstrateIO"},
	}
	for _, c := range cases {
		if got := c.k.String(); got != c.want {
			t.Errorf("kind %v String() = %q, want %q", c.k, got, c.want)
		}
	}
}

// --- AppliedLSNs interface contract ---

func TestBlockStore_AppliedLSNs_ReturnsErrAppliedLSNsNotTracked(t *testing.T) {
	// BlockStore is in-memory; explicit not-tracked return per
	// architect MED v0.1 #2 fix. Gate falls back on this sentinel.
	bs := NewBlockStore(64, 4096)
	got, err := bs.AppliedLSNs()
	if got != nil {
		t.Errorf("BlockStore.AppliedLSNs() returned non-nil map: %v", got)
	}
	if !errors.Is(err, ErrAppliedLSNsNotTracked) {
		t.Errorf("err = %v, want errors.Is(_, ErrAppliedLSNsNotTracked)", err)
	}
}
