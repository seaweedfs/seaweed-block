//go:build !linux

package hostlimit

// unsupportedFS accepts every file system where it cannot tell.
func unsupportedFS(string) (string, bool) { return "", false }
