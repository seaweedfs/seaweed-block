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
// where advisory lock semantics vary. It covers the file at path when the
// probe opens it, not a file renamed away earlier. It is not proof the owner
// process is dead; host cleanup must check that separately. The probe holds
// the lock only for an instant and leaves no reservation behind, so it does
// not keep a new owner out; an owner opening the volume in that instant is
// refused with ErrWALStoreLocked: probe only when the owner is believed gone.
func ProbeVolumeUnheld(path string) error {
	// One descriptor for both steps, locked first: the answer is about the
	// file this descriptor opened, whatever the path names afterwards, and
	// on Windows an owner's byte lock would refuse a read made before ours.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() // closing releases the probe's lock
	if hook := testProbeUnheldAfterOpen; hook != nil {
		hook()
	}
	switch err := LockWALStoreFile(f); {
	case errors.Is(err, ErrWALStoreLocked):
		return ErrVolumeHeld // held, whatever the file is: the safe answer
	case err != nil:
		return err
	}
	// Only a v3 owner takes the lock; for any other file a free lock proves
	// nothing.
	if err := probeWALStoreV3At(f); err != nil {
		return fmt.Errorf("storage: %s is not a v3 volume, unheld cannot be proven: %w", path, err)
	}
	return nil
}

// testProbeUnheldAfterOpen, test-only, runs in ProbeVolumeUnheld after the
// open, before the lock.
var testProbeUnheldAfterOpen func()
