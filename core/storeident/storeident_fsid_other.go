//go:build !linux

package storeident

// No statfs f_fsid here: the file system id is unknown and comparisons fall
// back to machine id + device.
func fsidOf(string) uint64 { return 0 }
