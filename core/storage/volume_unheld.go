package storage

import (
	"errors"
	"fmt"
	"os"
)

// ErrVolumeHeld: a live process on this host holds the volume file's lock.
var ErrVolumeHeld = errors.New("storage: volume held by a live owner on this host")

// ProbeVolumeUnheld reports whether no process on THIS host holds the v3
// volume file at path. It takes and releases the exclusive, nonblocking
// advisory lock that every v3 owner holds for as long as its store is open;
// the kernel releases that lock when the owner's process exits.
//
// nil: unheld. ErrVolumeHeld: held. Any other error: unknown (missing file,
// not a v3 volume, the lock call failed); treat it as not proven.
//
// The proof is per host and for local filesystems only: it says nothing
// about a process on another host, and nothing on a network or shared mount,
// where advisory lock semantics vary. It is not proof the owner process is
// dead; host cleanup must check that separately. The probe holds the lock
// only for an instant, but an owner opening the volume in that instant is
// refused with ErrWALStoreLocked: probe only when the owner is believed gone.
func ProbeVolumeUnheld(path string) error {
	// Only a v3 owner takes the lock; for any other file a free lock proves
	// nothing.
	if err := ProbeWALStoreV3(path); err != nil {
		return fmt.Errorf("storage: %s is not a v3 volume, unheld cannot be proven: %w", path, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() // closing releases the probe's lock
	switch err := LockWALStoreFile(f); {
	case errors.Is(err, ErrWALStoreLocked):
		return ErrVolumeHeld
	case err != nil:
		return err
	}
	return nil
}
