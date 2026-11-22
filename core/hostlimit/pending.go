package hostlimit

import (
	"os"
	"path/filepath"
)

// HoldUrgentPending tells every limiter using dir that urgent work is
// pending on the host: until release is called, Normal admissions there
// are refused with ErrUrgentPending. Callers hold it across a stop or a
// drain, so background cuts do not take slots the stop is waiting for.
func HoldUrgentPending(dir string) (release func(), err error) {
	f, err := holdShared(filepath.Join(dir, urgentPendingFile))
	if err != nil {
		return nil, err
	}
	return func() { _ = f.Close() }, nil
}

func holdShared(path string) (*os.File, error) {
	if i, err := os.Lstat(path); err == nil && !i.Mode().IsRegular() {
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrInvalid}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = lockShared(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
