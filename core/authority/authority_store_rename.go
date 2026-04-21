package authority

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"syscall"
	"time"
)

// renameWithRetry wraps os.Rename with a bounded retry loop that
// absorbs Windows-specific transient rename failures caused by
// antivirus real-time scanners (Defender), the Windows Search
// indexer, and similar filesystem filters briefly holding
// handles on freshly-created or freshly-closed files.
//
// We deliberately do NOT use github.com/google/renameio/v2: it
// explicitly refuses Windows atomic writes (WriteFile is
// !windows; maybe/Windows falls back to non-atomic os.WriteFile).
// Our Windows route is exactly where the atomic-rename work has
// to happen, so we roll the pattern locally.
//
// Retry budget: 20 attempts with exponential backoff starting at
// 20ms, capped at 2s per wait, total worst-case ~30s. Defender
// scan windows under load are typically 200ms–2s; the budget
// covers that with margin.
//
// Retryable errors are classified narrowly to ERROR_ACCESS_DENIED
// and ERROR_SHARING_VIOLATION — the Windows handle-retention
// class. Any other error (cross-device, ENOSPC, permission
// without race, …) surfaces on the first attempt. No silent
// absorption of real failures.
//
// On final failure the returned error includes the last os
// error plus Stat diagnostics for both paths so the caller
// knows which path was stuck.
func renameWithRetry(tempPath, finalPath string) error {
	const (
		maxAttempts = 20
		baseBackoff = 20 * time.Millisecond
		maxBackoff  = 2 * time.Second
	)

	var lastErr error
	backoff := baseBackoff
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := os.Rename(tempPath, finalPath)
		if err == nil {
			return nil
		}
		lastErr = err

		if !isRetryableRenameErr(err) {
			return err
		}

		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return fmt.Errorf(
		"rename failed after %d attempts: %w (tempStat=%s, finalStat=%s, goos=%s)",
		maxAttempts, lastErr, statString(tempPath), statString(finalPath), runtime.GOOS,
	)
}

// isRetryableRenameErr returns true when the error matches the
// Windows-specific transient class that retry can absorb:
//
//   - ERROR_ACCESS_DENIED (5): AV scanner or indexer holding
//     the source or destination.
//   - ERROR_SHARING_VIOLATION (32): another handle opened the
//     file with incompatible sharing mode.
//
// On POSIX these codes don't occur during normal rename paths;
// if a POSIX rename fails it's typically for a reason retry
// won't fix (EXDEV / ENOSPC / EACCES-no-race), so we don't
// retry there.
func isRetryableRenameErr(err error) bool {
	if err == nil {
		return false
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		return false
	}
	var errno syscall.Errno
	if !errors.As(pathErr.Err, &errno) {
		return false
	}
	// Windows numeric errnos; hard-coded so this file compiles
	// on non-Windows too — they simply never match there.
	const (
		errorAccessDenied     syscall.Errno = 5
		errorSharingViolation syscall.Errno = 32
	)
	switch errno {
	case errorAccessDenied, errorSharingViolation:
		return true
	}
	return false
}

// statString returns a compact diagnostic about a path for error
// messages: exists + size + modtime, or the stat error.
func statString(path string) string {
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "missing"
		}
		return "stat-err=" + err.Error()
	}
	return fmt.Sprintf("size=%d mtime=%s", fi.Size(), fi.ModTime().UTC().Format(time.RFC3339Nano))
}
