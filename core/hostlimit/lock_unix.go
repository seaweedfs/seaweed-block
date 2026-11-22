//go:build !windows

package hostlimit

import (
	"errors"
	"os"
	"syscall"
)

func tryLock(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return ErrBusy
	}
	return err
}

// lockShared waits for a shared lock on f. An exclusive holder keeps it
// only for a non-blocking probe, so the wait is short.
func lockShared(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_SH)
}
