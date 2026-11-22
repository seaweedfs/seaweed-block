//go:build windows

package hostlimit

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
)

func tryLock(f *os.File) error {
	err := windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return ErrBusy
	}
	return err
}

// lockShared waits for a shared lock on f. An exclusive holder keeps it
// only for a non-blocking probe, so the wait is short.
func lockShared(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), 0, 0, 1, 0, &windows.Overlapped{})
}
