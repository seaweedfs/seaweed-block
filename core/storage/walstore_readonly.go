// G5-5: read-only walstore opener for hardware-test byte-equal
// verification. Provides Read(lba) only — no Write, no Sync, no
// Recover, no checkpoint advance. Safe to invoke against a walstore
// path while the daemon owning it is stopped (file is opened
// O_RDONLY; the daemon must NOT be writing concurrently).
//
// Used by `cmd/blockvolume/m01_verify_helper.go` (build-tag
// `m01verify`) to read the replica's stored bytes for a target LBA
// range and compare against the primary's known write payload.
//
// NOT a substrate semantic change — exposes the same Read(lba)
// merge logic the daemon uses (WAL-replay then extent fallback)
// through a read-only file handle.

package storage

import (
	"errors"
	"fmt"
	"os"
)

// WALStoreReader is the read-only handle returned by OpenReadOnly.
// Surface is intentionally narrow: Read(lba), Close. No mutating
// methods (Write, Sync, Recover, Trim) are exposed; callers that
// need them must use OpenWALStore + Recover.
type WALStoreReader struct {
	inner *WALStore
}

// OpenReadOnly opens an existing walstore file for read-only access
// and replays the WAL into an in-memory dirty map so subsequent
// Read(lba) calls return the most-recent value (per-LBA WAL-then-
// extent merge — same semantic as the daemon's Read).
//
// Caller MUST ensure no writer (i.e., no daemon) is currently open
// against the same path; concurrent writes from another process
// would race the dirty-map snapshot. Typical use: stop the daemon
// (or pause writes) before invoking OpenReadOnly.
//
// The returned reader holds an open file descriptor; caller MUST
// call Close. Forward-carry: if mid-test we discover a need for
// shared-read semantics under concurrent writes (advisory locks,
// reader-snapshot variant), that is a separate scope expansion
// flagged at G5-5 close.
func OpenReadOnly(path string) (*WALStoreReader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("storage: OpenReadOnly %s: %w", path, err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: seek superblock: %w", err)
	}
	sb, err := readSuperblock(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := sb.validate(); err != nil {
		_ = f.Close()
		return nil, err
	}
	// Reuse openInitialized so the WAL writer + dirty map are
	// constructed identically to the R/W path. We never CALL the
	// writer (no Write surface exposed); the dirty map gets populated
	// by Recover below.
	inner, err := openInitialized(path, f, &sb)
	if err != nil {
		return nil, err
	}
	// Replay WAL into dirty map so Read merges WAL-recent writes
	// over the extent. Recover scans the WAL — it does NOT issue
	// writes against the file (the recover path's writes happen
	// only when promoting WAL entries to the extent at flush time,
	// which our caller never triggers).
	if _, err := inner.Recover(); err != nil {
		_ = inner.Close()
		return nil, fmt.Errorf("storage: OpenReadOnly recover: %w", err)
	}
	return &WALStoreReader{inner: inner}, nil
}

// Read returns the current bytes at lba — same merge logic the
// daemon uses (WAL-recent over extent).
func (r *WALStoreReader) Read(lba uint32) ([]byte, error) {
	if r.inner == nil {
		return nil, errors.New("storage: WALStoreReader.Read after Close")
	}
	return r.inner.Read(lba)
}

// Close releases the file descriptor.
func (r *WALStoreReader) Close() error {
	if r.inner == nil {
		return nil
	}
	err := r.inner.Close()
	r.inner = nil
	return err
}
