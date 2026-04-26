package storage

import (
	"path/filepath"
	"testing"
)

// T4d-4 part A — RecoveryMode interface method tests (T4c §I row 6
// closure). Substrates report their tier-1 sub-mode via the
// LogicalStorage interface. Replaces the T4c-2 duck-typed
// CheckpointLSN probe; survives the component framework's
// storage-wrap pattern.

func TestStorageRecoveryMode_BlockStore_StateConvergence(t *testing.T) {
	bs := NewBlockStore(64, 4096)
	if got := bs.RecoveryMode(); got != RecoveryModeStateConvergence {
		t.Errorf("BlockStore.RecoveryMode() = %q, want %q (in-memory; ScanLBAs synthesizes from current state)",
			got, RecoveryModeStateConvergence)
	}
}

func TestStorageRecoveryMode_Walstore_WALReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ws.walstore")
	s, err := CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatalf("CreateWALStore: %v", err)
	}
	defer s.Close()
	if got := s.RecoveryMode(); got != RecoveryModeWALReplay {
		t.Errorf("walstore.RecoveryMode() = %q, want %q (V2-faithful per-LSN ScanLBAs)",
			got, RecoveryModeWALReplay)
	}
}

// TestStorageRecoveryMode_LogicalStorageInterface_HasMethod is a
// compile-time fence pinning that RecoveryMode is on the interface
// (not just on substrate impls). If a future commit removes the
// interface method, this fails to compile.
func TestStorageRecoveryMode_LogicalStorageInterface_HasMethod(t *testing.T) {
	var s LogicalStorage = NewBlockStore(64, 4096)
	mode := s.RecoveryMode() // must compile via interface
	if mode == "" {
		t.Error("RecoveryMode via interface returned empty string")
	}
}
