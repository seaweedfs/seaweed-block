//go:build !unix

package storeident

// No st_dev here: the identity is unknown and never matches.
func machineID() string { return "" }

func devOf(string) uint64 { return 0 }
