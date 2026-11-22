//go:build !linux

package hostlimit

// remoteFS reports no network file system where it cannot tell.
func remoteFS(string) (string, bool) { return "", false }
