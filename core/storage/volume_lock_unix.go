//go:build !windows

package storage

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func lockWALStoreFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrWALStoreLocked
		}
		return fmt.Errorf("storage: flock: %w", err)
	}
	return nil
}
