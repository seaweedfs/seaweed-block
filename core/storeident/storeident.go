// Package storeident identifies the file system a durable directory lives
// on, so that two processes can tell whether their fsyncs share one journal
// (one ext4 file system = one jbd2 journal).
//
// An Identity is a raw observation fact: the machine's id, the directory's
// device number and its file system id. Zero values mean unknown and never
// compare equal.
package storeident

// Identity is the (machine, device, file system id) of a directory's file
// system.
type Identity struct {
	MachineID string // trimmed /etc/machine-id; "" = unknown
	Dev       uint64 // st_dev of the directory; 0 = unknown
	// FSID is statfs f_fsid of the directory (its two 32-bit words, high
	// word first); 0 = unknown. Peers that predate it report 0.
	FSID uint64
}

// Of returns the identity of dir's file system. On a platform without
// st_dev or statfs, or on any error, the unknown parts are left zero.
func Of(dir string) Identity {
	return Identity{MachineID: machineID(), Dev: devOf(dir), FSID: fsidOf(dir)}
}

// Known reports whether the machine id and device are known (the parts
// every comparison needs; FSID is compared only when both sides know it).
func (a Identity) Known() bool { return a.MachineID != "" && a.Dev != 0 }

// SameFileSystem reports whether a and b are known to name the same file
// system: same machine and same device, and, when both sides know their file
// system id, the same file system id. One unknown FSID (a peer that predates
// it, a platform without statfs) falls back to machine id + device, as
// before FSID existed. Unknown machine id or device never matches.
func (a Identity) SameFileSystem(b Identity) bool {
	if !a.Known() || !b.Known() || a.MachineID != b.MachineID || a.Dev != b.Dev {
		return false
	}
	return a.FSID == 0 || b.FSID == 0 || a.FSID == b.FSID
}
