// Package storeident identifies the file system a durable directory lives
// on, so that two processes can tell whether their fsyncs share one journal
// (one ext4 file system = one jbd2 journal).
//
// An Identity is a raw observation fact: the machine's id and the directory's
// device number. Zero values mean unknown and never compare equal.
package storeident

// Identity is the (machine, device) pair of a directory's file system.
type Identity struct {
	MachineID string // trimmed /etc/machine-id; "" = unknown
	Dev       uint64 // st_dev of the directory; 0 = unknown
}

// Of returns the identity of dir's file system. On a platform without
// st_dev or on any error the unknown parts are left zero.
func Of(dir string) Identity {
	return Identity{MachineID: machineID(), Dev: devOf(dir)}
}

// Known reports whether both parts of the identity are known.
func (a Identity) Known() bool { return a.MachineID != "" && a.Dev != 0 }

// SameFileSystem reports whether a and b are known to name the same file
// system: same machine and same device. Unknown never matches.
func (a Identity) SameFileSystem(b Identity) bool {
	return a.Known() && b.Known() && a.MachineID == b.MachineID && a.Dev == b.Dev
}
