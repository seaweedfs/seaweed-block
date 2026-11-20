package storage

import (
	"fmt"
	"io"
	"log"
	"os"
)

// Data-only sync applies to WAL/extent data on Linux by default. Descriptor slots,
// chunk maps, creation and directory operations retain full fsync.
func newDataFileSync(f *os.File) (func() error, error) {
	mode := os.Getenv("SWBLOCK_SYNC")
	if mode == "" {
		mode = defaultDataFileSyncMode
	}
	switch mode {
	case "fsync":
		return f.Sync, nil
	case "fdatasync":
		if err := dataSyncSupported(); err != nil {
			return nil, err
		}
		log.Printf("storage: WAL/extent sync=fdatasync file=%s", f.Name())
		return func() error { return fdatasyncFile(f) }, nil
	default:
		return nil, fmt.Errorf("storage: invalid SWBLOCK_SYNC %q", mode)
	}
}

// Only newly-created files reach this hook: existing volume bytes are never
// rewritten. Writing zeros (not fallocate) converts unwritten extents too.
func prezeroNewStore(f *os.File, size int64) error {
	mode := os.Getenv("SWBLOCK_ALLOCATION")
	prezero := os.Getenv("SWBLOCK_PREZERO")
	if prezero != "" && prezero != "0" && prezero != "1" {
		return fmt.Errorf("storage: invalid SWBLOCK_PREZERO %q", prezero)
	}
	if prezero == "1" {
		if mode != "" && mode != "prezero" {
			return fmt.Errorf("storage: SWBLOCK_PREZERO conflicts with SWBLOCK_ALLOCATION %q", mode)
		}
		mode = "prezero"
	}
	switch mode {
	case "", "sparse":
		return nil
	case "fallocate":
		return fmt.Errorf("storage: fallocate allocation is disabled; use sparse or explicit prezero")
	case "prezero":
		if err := dataSyncSupported(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("storage: invalid SWBLOCK_ALLOCATION %q", mode)
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
