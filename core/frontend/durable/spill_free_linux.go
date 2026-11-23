//go:build linux

package durable

import "syscall"

// spillFree returns the free bytes available to an unprivileged writer on
// the filesystem holding dir (the COW side files).
func spillFree(dir string) func() (int64, error) {
	return func() (int64, error) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(dir, &st); err != nil {
			return 0, err
		}
		return int64(st.Bavail) * int64(st.Bsize), nil
	}
}

// allocatedBytes is the space path's file holds on disk (0 if unknown).
func allocatedBytes(path string) int64 {
	var st syscall.Stat_t
	if syscall.Stat(path, &st) != nil {
		return 0
	}
	return st.Blocks * 512
}
