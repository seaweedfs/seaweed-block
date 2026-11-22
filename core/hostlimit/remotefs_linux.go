//go:build linux

package hostlimit

import (
	"fmt"
	"syscall"
)

// localDiskFS are the file systems a host-limit directory may be on: local
// disk file systems, whose kernel file locks and host monotonic clock
// belong to one host. A network or cluster file system (NFS, CIFS, CephFS,
// FUSE, ...) can share the directory between hosts, whose tickets would then
// compare two different monotonic clocks; tmpfs and overlay are refused too.
var localDiskFS = map[uint32]string{
	0xEF53:     "ext4",
	0x58465342: "xfs",
	0x9123683E: "btrfs",
	0x2FC12FC1: "zfs",
	0xF2F52010: "f2fs",
}

// unsupportedFS names the file system dir is on when it is not a local disk
// file system.
func unsupportedFS(dir string) (string, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return "an unreadable file system (" + err.Error() + ")", true
	}
	if _, ok := localDiskFS[uint32(st.Type)]; ok {
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
	case 0x00C36400:
		return "CephFS", true
	case 0x01021994:
		return "tmpfs", true
	case 0x794C7630:
		return "overlay", true
	}
	return fmt.Sprintf("file system type %#x", uint32(st.Type)), true
}
