// G5-5: read-only walstore opener for hardware-test byte-equal
// verification. Provides Read(lba) only — no Write, no Sync, no
// Recover, no checkpoint advance, no background goroutines, no
// superblock writes.
//
// Used by `cmd/m01verify/main.go` (build-tag `m01verify`) to read
// the replica's stored bytes for a target LBA range and compare
// against the primary's known write payload.
//
// NOT a substrate semantic change — exposes the same Read(lba)
// merge logic the daemon uses (WAL-replay then extent fallback)
// through a read-only file handle, but constructed without the
// writer machinery (committer / flusher / admission goroutines)
// that the R/W path needs.
//
// Architectural fix per architect review (round 52, MEDIUM):
// the original implementation reused `openInitialized`, which
// started flusher + committer + admission goroutines and persisted
// the superblock back through the file descriptor on Close. Those
// writes target the same fd opened O_RDONLY, so they would silently
// fail in the OS layer — a "read-only verifier" leak. This rewrite
// constructs only the fields Read + Recover need, and Close only
// closes the fd.

package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// WALStoreReader is the read-only handle returned by OpenReadOnly.
// Surface is intentionally narrow: Read(lba), Close. No mutating
// methods (Write, Sync, Recover, Trim) are exposed; callers that
// need them must use OpenWALStore + Recover.
type WALStoreReader struct {
	mu     sync.Mutex
	fd     *os.File
	sb     *superblock
	dm     *dirtyMap
	closed bool
}

// OpenReadOnly opens an existing walstore file for read-only access
// and replays the WAL into an in-memory dirty map so subsequent
// Read(lba) calls return the most-recent value (per-LBA WAL-then-
// extent merge — same semantic as the daemon's Read).
//
// No background goroutines are started. The file descriptor is
// opened O_RDONLY; on Close only the fd is released — no superblock
// or checkpoint persistence.
//
// Caller MUST ensure no writer (i.e., no daemon) is currently open
// against the same path; concurrent writes from another process
// would race the dirty-map snapshot. Typical use: stop the daemon
// (or pause writes) before invoking OpenReadOnly.
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
	dm := newDirtyMap(64)
	// Replay WAL into the dirty map. recoverWAL is a pure read pass
	// over the WAL frames — no writes back to the fd.
	if _, err := recoverWAL(f, &sb, dm); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: OpenReadOnly recover: %w", err)
	}
	return &WALStoreReader{fd: f, sb: &sb, dm: dm}, nil
}

// Read returns the current bytes at lba — same merge logic the
// daemon uses (WAL-recent over extent). No mutation; safe to call
// concurrently from multiple goroutines.
func (r *WALStoreReader) Read(lba uint32) ([]byte, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("storage: WALStoreReader.Read after Close")
	}
	r.mu.Unlock()

	maxLBA := uint32(r.sb.VolumeSize / uint64(r.sb.BlockSize))
	if lba >= maxLBA {
		return nil, fmt.Errorf("storage: LBA %d out of range", lba)
	}
	// WAL-recent first. Inline the same logic as WALStore.readFromWAL
	// + readFromExtent, but operating against the read-only fd.
	if walRelOff, _, _, ok := r.dm.get(uint64(lba)); ok {
		return r.readFromWAL(walRelOff)
	}
	return r.readFromExtent(lba)
}

func (r *WALStoreReader) readFromWAL(walRelOff uint64) ([]byte, error) {
	headerBuf := make([]byte, walEntryHeaderSize)
	absOff := int64(r.sb.WALOffset + walRelOff)
	if _, err := r.fd.ReadAt(headerBuf, absOff); err != nil {
		return nil, fmt.Errorf("storage: OpenReadOnly WAL header: %w", err)
	}
	length := parseLengthFromHeader(headerBuf)
	if length == 0 {
		return make([]byte, r.sb.BlockSize), nil
	}
	data := make([]byte, length)
	if _, err := r.fd.ReadAt(data, absOff+int64(walEntryPrefixSize)); err != nil {
		return nil, fmt.Errorf("storage: OpenReadOnly WAL data: %w", err)
	}
	return data, nil
}

func (r *WALStoreReader) readFromExtent(lba uint32) ([]byte, error) {
	bs := uint64(r.sb.BlockSize)
	off := int64(uint64(r.sb.WALOffset) + uint64(r.sb.WALSize) + uint64(lba)*bs)
	buf := make([]byte, bs)
	if _, err := r.fd.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("storage: OpenReadOnly extent read lba=%d: %w", lba, err)
	}
	return buf, nil
}

// Close releases the file descriptor. Idempotent. Does NOT persist
// superblock or any state — the verifier path is read-only.
func (r *WALStoreReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.fd != nil {
		err := r.fd.Close()
		r.fd = nil
		return err
	}
	return nil
}
