package storage

import (
	"fmt"
	"io"
	"log"
	"os"
)

// Data-only sync applies to WAL/extent data. Descriptor slots, chunk maps,
// creation and directory operations retain full fsync.
func configuredDataSyncMode() (string, error) {
	mode := os.Getenv("SWBLOCK_SYNC")
	if mode == "" || mode == "auto" {
		return "auto", nil
	}
	switch mode {
	case "fsync":
		return mode, nil
	case "fdatasync":
		return mode, dataSyncSupported()
	default:
		return "", fmt.Errorf("storage: invalid SWBLOCK_SYNC %q", mode)
	}
}

func selectedDataSyncMode(f *os.File) (string, string, error) {
	mode, err := configuredDataSyncMode()
	if err != nil {
		return "", "", err
	}
	reason := "explicit SWBLOCK_SYNC override"
	if mode == "auto" {
		mode, reason = defaultDataSyncMode(f)
	}
	return mode, reason, nil
}

// DataSyncStatus is immutable for the lifetime of an open volume.
type DataSyncStatus struct {
	Mode   string // the data-file sync call in use (fsync or fdatasync)
	Reason string // why it was chosen (an explicit SWBLOCK_SYNC, or the automatic choice)
}

func (s *WALStore) DataSyncPolicy() DataSyncStatus { return s.dataSyncStatus }

func newDataFileSync(f *os.File) (func() error, error) {
	sync, _, err := bindDataFileSync(f)
	return sync, err
}

func bindDataFileSync(f *os.File) (func() error, DataSyncStatus, error) {
	mode, reason, err := selectedDataSyncMode(f)
	if err != nil {
		return nil, DataSyncStatus{}, err
	}
	status := DataSyncStatus{Mode: mode, Reason: reason}
	log.Printf("storage: WAL/extent sync=%s file=%s reason=%s", mode, f.Name(), reason)
	if mode == "fsync" {
		return f.Sync, status, nil
	}
	return func() error { return fdatasyncFile(f) }, status, nil
}

// Configuration errors must not create or initialize a volume file.
func validateNewStorePolicy() error {
	if _, err := configuredDataSyncMode(); err != nil {
		return err
	}
	_, err := configuredAllocationMode()
	return err
}

func configuredAllocationMode() (string, error) {
	mode := os.Getenv("SWBLOCK_ALLOCATION")
	prezero := os.Getenv("SWBLOCK_PREZERO")
	if prezero != "" && prezero != "0" && prezero != "1" {
		return "", fmt.Errorf("storage: invalid SWBLOCK_PREZERO %q", prezero)
	}
	if prezero == "1" {
		if mode != "" && mode != "prezero" {
			return "", fmt.Errorf("storage: SWBLOCK_PREZERO conflicts with SWBLOCK_ALLOCATION %q", mode)
		}
		mode = "prezero"
	}
	switch mode {
	case "", "sparse":
		return "sparse", nil
	case "fallocate":
		return "", fmt.Errorf("storage: fallocate allocation is disabled; use sparse or explicit prezero")
	case "prezero":
		return mode, dataSyncSupported()
	default:
		return "", fmt.Errorf("storage: invalid SWBLOCK_ALLOCATION %q", mode)
	}
}

// Only newly-created files reach this hook; existing bytes are never rewritten.
func prezeroNewStore(f *os.File, size int64) error {
	mode, err := configuredAllocationMode()
	if err != nil {
		return err
	}
	if mode == "sparse" {
		return nil
	}
	zero := make([]byte, 1<<20)
	for off := int64(0); off < size; {
		b := zero[:min(int64(len(zero)), size-off)]
		n, err := f.WriteAt(b, off)
		if err != nil {
			return fmt.Errorf("storage: prezero: %w", err)
		}
		if n != len(b) {
			return io.ErrShortWrite
		}
		off += int64(n)
	}
	// Allocation/size/extent metadata must be fully durable before the check.
	if err := f.Sync(); err != nil {
		return err
	}
	if err := verifyInitializedFile(f); err != nil {
		return fmt.Errorf("storage: prezero verification: %w", err)
	}
	log.Printf("storage: prezero FIEMAP verified initialized file=%s bytes=%d", f.Name(), size)
	return nil
}
