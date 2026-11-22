//go:build !windows

package hostlimit

import "golang.org/x/sys/unix"

// hostMonoNs reads the host's monotonic clock, shared by every process on
// the host; it is never compared across hosts or reboots.
func hostMonoNs() int64 {
	var ts unix.Timespec
	if unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts) != nil {
		return 0
	}
	return ts.Nano()
}
