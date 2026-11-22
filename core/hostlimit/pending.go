package hostlimit

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

// urgentHoldWait bounds how long urgent work waits for its shared hold on
// urgent-pending. A waiter holds the file exclusively only from its probe
// to its slot try-lock; one that stalled there must not block urgent work.
const urgentHoldWait = 200 * time.Millisecond

// HoldUrgentPending tells every limiter using dir that urgent work is
// pending on the host: until release is called, Normal admissions there
// are refused with ErrUrgentPending. Callers hold it across a stop or a
// drain. If the hold cannot be taken within 200 ms (a waiter stalled in its
// probe), held is false and release does nothing: the caller proceeds
// without it, since the slots themselves are the exclusion.
func HoldUrgentPending(dir string) (release func(), held bool, err error) {
	f, held, err := holdSharedBounded(filepath.Join(dir, urgentPendingFile))
	if err != nil {
		return nil, false, err
	}
	if !held {
		log.Printf("hostlimit: urgent-pending in %s stayed held exclusively for %s; proceeding without the hold", dir, urgentHoldWait)
		return func() {}, false, nil
	}
	return func() { _ = f.Close() }, true, nil
}

// holdSharedBounded opens path and takes a shared lock on it, retrying for
// up to urgentHoldWait. It reports held=false (and no error) when the lock
// stayed taken exclusively for that long.
func holdSharedBounded(path string) (*os.File, bool, error) {
	if i, err := os.Lstat(path); err == nil && !i.Mode().IsRegular() {
		return nil, false, &os.PathError{Op: "open", Path: path, Err: os.ErrInvalid}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	deadline := time.Now().Add(urgentHoldWait)
	for {
		err = tryLockShared(f)
		if err == nil {
			return f, true, nil
		}
		if !errors.Is(err, ErrBusy) {
			_ = f.Close()
			return nil, false, err
		}
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, false, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}
