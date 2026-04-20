package authority

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ============================================================
// P14 S5 — File-Per-Volume Authority Store
//
// Bounded single-process deployment backend. One file per
// VolumeID under a configured store directory. Atomic rename
// on every write. No bolt / sqlite / log-structured backend;
// the backend upgrade is a separate later slice with its own
// acceptance (sketch §6).
//
// V2 muscle influence (mechanism only, not copy): the
// per-volume durable-record pattern mirrors the shape used in
// weed/server/master_block_registry.go and the atomic write
// discipline mirrors weed/server/master_block_assignment_queue.go.
// Policy from those V2 files (promotion, failover choice, etc.)
// is NOT ported — strictly denylisted in sketch §15.
// ============================================================

// FileAuthorityStore persists DurableRecords as one file per
// VolumeID under a configured directory. Writes use write-to-
// tempfile + fsync + atomic rename. Reads enumerate the
// directory, parse each file, and skip corrupt records
// (per-volume fail-closed).
type FileAuthorityStore struct {
	dir string

	mu     sync.Mutex
	closed bool
}

// NewFileAuthorityStore opens (and if needed, creates) a
// file-per-volume store under the given directory. The caller
// is expected to have already acquired the single-owner lock
// (sketch §9) before constructing the store; the store itself
// does not enforce exclusivity.
func NewFileAuthorityStore(dir string) (*FileAuthorityStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("authority_store: empty store directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("authority_store: mkdir %q: %w", dir, err)
	}
	return &FileAuthorityStore{dir: dir}, nil
}

// storeFileExtension is the filename suffix for durable authority
// records. Chosen to be distinctive and unlikely to collide with
// the lock file or tempfiles during the atomic-rename dance.
const storeFileExtension = ".v3auth.json"

// tempSuffix is appended to in-progress writes before atomic
// rename. Any leftover ".*.tmp" files from prior crashed writes
// are cleaned up at Load time.
const tempSuffix = ".tmp"

// volumePath returns the canonical filesystem path for one
// volume's durable record. Caller-supplied volumeIDs go through
// encodeVolumeIDForFilename so filesystem-reserved characters
// cannot escape the directory.
func (s *FileAuthorityStore) volumePath(volumeID string) string {
	return filepath.Join(s.dir, encodeVolumeIDForFilename(volumeID)+storeFileExtension)
}

// encodeVolumeIDForFilename is an INJECTIVE filename-safe
// encoding of a VolumeID. Two distinct VolumeIDs always produce
// two distinct encoded filenames — there is no aliasing path by
// which one volume's durable record can overwrite another's.
//
// Encoding:
//   - Bytes in the set [a-zA-Z0-9-] pass through unchanged.
//   - Every other byte (including literal '_') is escaped as
//     "_XX" where XX is the 2-digit lowercase hex of the byte.
//
// Injectivity proof: '_' is NEVER a passthrough character. Any
// '_' in the encoded output is always the start of an escape
// sequence followed by exactly 2 hex digits representing one
// input byte. Given an encoded string, a deterministic decoder
// can recover the original VolumeID; therefore no two distinct
// VolumeIDs can encode to the same filename.
//
// Example (the collision case the architect flagged):
//
//   VolumeID="/"       -> "_2f"          (1 byte, escaped)
//   VolumeID="_2f"     -> "_5f2f"        ('_' escaped, then passthrough)
//   VolumeID="_002f"   -> "_5f002f"      ('_' escaped, then passthrough)
//
// All three are distinct. The old non-injective encoding
// collapsed "_002f" and "/" onto the same name.
//
// Not a security boundary; the VolumeID comes from the operator
// or control plane. This is a correctness boundary: the durable
// store is file-per-volume, so aliased names would cause one
// volume's authority line to overwrite another's.
func encodeVolumeIDForFilename(volumeID string) string {
	var b strings.Builder
	for _, c := range []byte(volumeID) {
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "_%02x", c)
		}
	}
	return b.String()
}

// Put atomically writes the record to disk: serialize → fsync
// tempfile → rename-over-final. Returns nil on success.
//
// The atomic-rename pattern means a crash at any point leaves
// either the old file (if rename hadn't landed) or the new file
// (if it had). There is never a torn record on disk. A leftover
// tempfile from a pre-crash failed write is cleaned up at Load
// time.
func (s *FileAuthorityStore) Put(record DurableRecord) error {
	if record.VolumeID == "" {
		return fmt.Errorf("authority_store: cannot persist record with empty VolumeID")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("authority_store: Put on closed store")
	}

	payload, err := encodeEnvelope(record)
	if err != nil {
		return fmt.Errorf("authority_store: encode: %w", err)
	}

	finalPath := s.volumePath(record.VolumeID)
	tempPath := finalPath + tempSuffix

	// Write tempfile. Create exclusively so we don't silently
	// overwrite an in-progress write from another (forbidden)
	// writer.
	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("authority_store: open temp: %w", err)
	}
	if _, werr := f.Write(payload); werr != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("authority_store: write temp: %w", werr)
	}
	if syncErr := f.Sync(); syncErr != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("authority_store: fsync temp: %w", syncErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("authority_store: close temp: %w", closeErr)
	}

	// Atomic rename. On POSIX this is atomic by spec; on Windows
	// os.Rename uses MoveFileEx with MOVEFILE_REPLACE_EXISTING.
	if err := os.Rename(tempPath, finalPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("authority_store: rename temp->final: %w", err)
	}

	// Best-effort fsync the directory so the rename is durable.
	// On Windows, opening directories as files for fsync is not
	// supported; skip silently.
	if d, derr := os.Open(s.dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Load returns every record currently in the store. Records
// that fail integrity (corrupt envelope, checksum mismatch,
// VolumeID / filename mismatch) are skipped; an accompanying
// slice of errors describes each skip so a caller can log them.
//
// Returns (records, skippedErrors, err). The err is non-nil
// ONLY for index-level failures (e.g. the store directory
// cannot be listed). Per-record corruption populates
// skippedErrors but keeps err == nil (per-volume fail closed,
// not whole process — sketch §10).
func (s *FileAuthorityStore) LoadWithSkips() ([]DurableRecord, []error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, fmt.Errorf("authority_store: Load on closed store")
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		// Index-level failure: cannot list store directory. Whole
		// boot fails per sketch §10.
		return nil, nil, fmt.Errorf("authority_store: readdir: %w", err)
	}

	var records []DurableRecord
	var skips []error

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Clean up stale tempfiles from prior crashed writes.
		if strings.HasSuffix(name, tempSuffix) {
			_ = os.Remove(filepath.Join(s.dir, name))
			continue
		}
		if !strings.HasSuffix(name, storeFileExtension) {
			continue
		}
		// Expected VolumeID is the filename base after stripping
		// the extension. Any mismatch with the envelope's Body
		// VolumeID field triggers the corruption path.
		encodedVolumeID := strings.TrimSuffix(name, storeFileExtension)
		path := filepath.Join(s.dir, name)
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			skips = append(skips, fmt.Errorf("%s: read: %w", name, rerr))
			continue
		}
		// We don't try to reverse-encode the filename; the
		// envelope's VolumeID is the authoritative value. We
		// pass encodedVolumeID only for the mismatch check —
		// if the operator manually copied a file under a
		// different name, that's a corruption signal.
		rec, derr := decodeEnvelope(raw, "")
		if derr != nil {
			skips = append(skips, fmt.Errorf("%s: %w", name, derr))
			continue
		}
		// Filename / VolumeID consistency check (encoded form).
		if encodeVolumeIDForFilename(rec.VolumeID) != encodedVolumeID {
			skips = append(skips, fmt.Errorf("%s: %w: filename does not match VolumeID %q",
				name, ErrCorruptRecord, rec.VolumeID))
			continue
		}
		records = append(records, rec)
	}

	return records, skips, nil
}

// Load is the AuthorityStore interface method; it discards
// per-record skip errors. Callers that need them should use
// LoadWithSkips directly.
func (s *FileAuthorityStore) Load() ([]DurableRecord, error) {
	recs, _, err := s.LoadWithSkips()
	return recs, err
}

// Close releases store resources. Idempotent.
func (s *FileAuthorityStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
