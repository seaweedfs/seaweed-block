package storage

import (
	"errors"
	"os"
)

// ErrWALStoreLocked refuses a second store owner or an offline migration
// while an owner holds the same file. Closing the descriptor releases it.
var ErrWALStoreLocked = errors.New("storage: volume locked")

// LockWALStoreFile takes the same exclusive, nonblocking advisory lock for
// v3 read/write open, offline read-only open and the migration CLI. Call it
// before reading descriptors or mutating bytes; retain f until finished.
func LockWALStoreFile(f *os.File) error { return lockWALStoreFile(f) }
