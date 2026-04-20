package authority

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ============================================================
// P14 S5 — Bootstrap: Single-Owner OS File Lock (Crash-Safe)
//
// Sketch §9: the bounded single-active-master deployment is
// enforced by a cross-platform OS-level advisory lock on a
// well-known file under the store directory. Acquired once at
// startup, held by an owned file handle for process lifetime,
// released by closing that handle.
//
// Crash-safety guarantee: the OS lock is tied to the file
// handle. When a process dies for any reason (crash, kill -9,
// host reboot), the OS closes the handle and releases the lock.
// A fresh restart can acquire the lock without manual cleanup.
// This is the "restart-recoverable" property the S5 slice
// claims.
//
// Platform implementation (acquireOSLock):
//   - Linux / macOS / *BSD: syscall.Flock(fd, LOCK_EX|LOCK_NB).
//     Advisory lock, tied to the file descriptor; dropped when
//     the fd is closed (explicitly or by the OS on crash).
//   - Windows:              syscall.CreateFile opened with
//                           dwShareMode = 0 (exclusive share).
//                           A second open from another handle
//                           fails with ERROR_SHARING_VIOLATION;
//                           exclusivity is tied to the handle
//                           and dropped when it is closed.
//
// Note on Windows: syscall.LockFileEx was an earlier candidate,
// but its binding is not stable-stdlib across supported Go
// versions. Exclusive share mode on CreateFile delivers the
// same single-owner property using only stdlib.
//
// Both paths are in the Go standard library; no external
// dependency. Implementation lives in authority_bootstrap_unix.go
// and authority_bootstrap_windows.go with the appropriate build
// tags.
// ============================================================

// LockFilename is the well-known lockfile under the store
// directory.
const LockFilename = ".sparrow-authority-lock"

// StoreLock is the per-process single-owner lock on the
// authority store directory. The locked file handle is held
// for process lifetime. Release() closes it, which the OS uses
// as the signal to drop the lock.
type StoreLock struct {
	f    *os.File
	path string

	mu       sync.Mutex
	released bool
}

// ErrStoreLockHeld means another process (or another call
// within this process) is currently holding the lock.
var ErrStoreLockHeld = errors.New("authority: store lock is held")

// AcquireStoreLock tries to acquire exclusive ownership of the
// store directory via an OS-level advisory file lock. Succeeds
// only if no other live process is holding it.
//
// On success, returns a *StoreLock; caller must Release() on
// exit (typically via defer). The PID is written into the lock
// file for diagnostic purposes only — the actual exclusivity
// gate is the OS lock on the file handle, not the PID content.
//
// On a hard process crash, the OS drops the lock automatically
// when it closes our file handle; the next startup will acquire
// cleanly without manual intervention.
func AcquireStoreLock(storeDir string) (*StoreLock, error) {
	if storeDir == "" {
		return nil, fmt.Errorf("authority: empty store directory for lock")
	}
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("authority: mkdir for lock: %w", err)
	}
	path := filepath.Join(storeDir, LockFilename)

	f, err := acquireOSLock(path)
	if err != nil {
		if errors.Is(err, ErrStoreLockHeld) {
			// Add context (store path + previous holder PID if
			// readable) without overwriting the sentinel.
			pid := "unknown"
			if raw, rerr := os.ReadFile(path); rerr == nil {
				pid = strings.TrimSpace(string(raw))
				if pid == "" {
					pid = "unknown"
				}
			}
			return nil, fmt.Errorf("%w: store=%q holder-pid=%s",
				ErrStoreLockHeld, storeDir, pid)
		}
		return nil, fmt.Errorf("authority: acquire lock: %w", err)
	}

	// Write our PID for diagnosis. Truncate first so stale PIDs
	// from prior crashed owners don't bleed through. The OS lock
	// is already held by this point, so nobody else can observe
	// a partial write.
	if _, err := f.Seek(0, 0); err == nil {
		_ = f.Truncate(0)
		_, _ = f.WriteString(strconv.Itoa(os.Getpid()))
		_ = f.Sync()
	}

	return &StoreLock{f: f, path: path}, nil
}

// Release drops the OS lock by closing the underlying file
// handle. Idempotent. Safe to call from any goroutine. Callers
// typically defer this immediately after a successful acquire.
//
// The lock file itself is NOT removed — removing it would race
// with another process that has just acquired the lock, or with
// a crash-recovery restart. The file is a stable rendezvous
// point; the OS-level lock on its handle is the actual lock.
func (l *StoreLock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true
	if l.f != nil {
		if err := l.f.Close(); err != nil {
			return fmt.Errorf("authority: release lock: %w", err)
		}
	}
	return nil
}

// Path returns the lockfile path. Useful for diagnostics.
func (l *StoreLock) Path() string {
	return l.path
}
