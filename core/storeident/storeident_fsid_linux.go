//go:build linux

package storeident

import "syscall"

// fsidOf returns statfs f_fsid of dir as one value (high word first), or 0
// when dir cannot be read.
func fsidOf(dir string) uint64 {
	if dir == "" {
		return 0
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0
	}
	return uint64(uint32(st.Fsid.X__val[0]))<<32 | uint64(uint32(st.Fsid.X__val[1]))
}
