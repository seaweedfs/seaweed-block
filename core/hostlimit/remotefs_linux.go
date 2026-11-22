//go:build linux

package hostlimit

import "syscall"

// remoteFS names the network file system dir is on, if it is on one.
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
	case 0x65735546:
		return "FUSE", true
	}
	return "", false
}
