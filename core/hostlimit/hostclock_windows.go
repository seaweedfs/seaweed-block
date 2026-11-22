//go:build windows

package hostlimit

import "golang.org/x/sys/windows"

// hostMonoNs reads the host's monotonic clock (time since boot), shared by
// every process on the host; it is never compared across hosts or reboots.
func hostMonoNs() int64 { return int64(windows.DurationSinceBoot()) }
