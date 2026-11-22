//go:build linux

package hostlimit

import "syscall"

// remoteFS names the remote file system dir is on, if it is on one. The
// host-limit directory needs local flock semantics, which every local file
// system gives (tmpfs and overlay included); a remote one (NFS, CIFS/SMB,
// FUSE such as glusterfs, 9p, CephFS) can share the directory between
// hosts, whose tickets would then compare two monotonic clocks. Unknown
// types are accepted.
func remoteFS(dir string) (string, bool) {
	var st syscall.Statfs_t
	if syscall.Statfs(dir, &st) != nil {
		return "", false
	}
	switch uint32(st.Type) {
	case 0x6969:
		return "NFS", true
	case 0xFF534D42:
		return "CIFS", true
	case 0xFE534D42:
		return "SMB2", true
	case 0x517B:
		return "SMB", true
	case 0x65735546:
		return "FUSE", true
	case 0x01021997:
		return "9p", true
	case 0x00C36400:
		return "CephFS", true
	}
	return "", false
}
