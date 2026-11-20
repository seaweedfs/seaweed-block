//go:build linux

package storage

func deviceDataSyncMode(root string, major, minor uint32) (string, string) {
	return "fdatasync", "unconditional Linux default"
}
