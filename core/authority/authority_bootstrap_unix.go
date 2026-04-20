//go:build !windows

package authority

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// acquireOSLock opens (and creates if needed) the lockfile and
// takes an exclusive, non-blocking advisory lock via flock(2).
// The lock is tied to the returned *os.File handle; closing the
// handle releases the lock, including when the OS closes it on
// our behalf after a process crash.
//
// Returns ErrStoreLockHeld (wrapped) if another owner already
// holds the lock; any other error is wrapped directly.
func acquireOSLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		// EWOULDBLOCK / EAGAIN → another process holds the lock.
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrStoreLockHeld
		}
		return nil, fmt.Errorf("flock: %w", err)
	}
	return f, nil
}
